// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

const (
	lifecycleNamespace = "default"
	lifecycleCluster   = "test"

	// keepFinalizer keeps a deleted Machine around with a deletion timestamp, as the Machine controller does.
	keepFinalizer = "test.cluster.x-k8s.io/keep"
)

// fakeMachineAPI answers for the Talos API of the machines from fixed state and records the calls that
// change etcd.
type fakeMachineAPI struct {
	mu sync.Mutex

	// members is the etcd member list every healthy machine reports.
	members []*machineapi.EtcdMember
	// etcdDown lists the machines whose etcd service is not running.
	etcdDown map[string]bool
	// unreachable lists the machines whose Talos API cannot be reached.
	unreachable map[string]bool
	// servicesDown lists the machines with an unhealthy control plane service.
	servicesDown map[string]bool

	removeErr error

	calls []string
}

func newFakeMachineAPI(members ...string) *fakeMachineAPI {
	return &fakeMachineAPI{
		members:      etcdMembersFor(members...),
		etcdDown:     map[string]bool{},
		unreachable:  map[string]bool{},
		servicesDown: map[string]bool{},
	}
}

func (f *fakeMachineAPI) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, call)
}

func (f *fakeMachineAPI) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.calls)
}

func (f *fakeMachineAPI) CheckServices(_ context.Context, _ *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) error {
	switch {
	case f.unreachable[machine.Name]:
		return fmt.Errorf("%s is unreachable", machine.Name)
	case f.servicesDown[machine.Name]:
		return &errServiceUnhealthy{service: "kubelet", reason: "Failed"}
	}

	return nil
}

func (f *fakeMachineAPI) ProbeEtcd(_ context.Context, _ *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) etcdMachineStatus {
	switch {
	case f.unreachable[machine.Name]:
		return etcdMachineStatus{inspectErr: fmt.Errorf("%s is unreachable", machine.Name)}
	case f.etcdDown[machine.Name]:
		return etcdMachineStatus{serviceErr: fmt.Errorf("%s: etcd is not running", machine.Name)}
	}

	return etcdMachineStatus{members: f.members}
}

func (f *fakeMachineAPI) LeaveEtcd(_ context.Context, _ *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) error {
	if f.unreachable[machine.Name] || f.etcdDown[machine.Name] {
		return fmt.Errorf("%s cannot leave etcd", machine.Name)
	}

	f.record("leave:" + machine.Name)

	return nil
}

func (f *fakeMachineAPI) RemoveEtcdMember(_ context.Context, _ *controlplanev1.TalosControlPlane, via *clusterv1.Machine, member *machineapi.EtcdMember) error {
	if f.removeErr != nil {
		return f.removeErr
	}

	f.record(fmt.Sprintf("remove:%s:via=%s", member.Hostname, via.Name))

	return nil
}

// etcdMembersFor returns one voting etcd member per host name.
func etcdMembersFor(names ...string) []*machineapi.EtcdMember {
	members := make([]*machineapi.EtcdMember, 0, len(names))

	for i, name := range names {
		members = append(members, &machineapi.EtcdMember{Id: uint64(i + 1), Hostname: name})
	}

	return members
}

type machineOption func(*clusterv1.Machine)

// flagged marks the machine as a MachineHealthCheck does when it leaves remediation to the owner.
func flagged(m *clusterv1.Machine) {
	markUnhealthy(m)
	conditions.Set(m, metav1.Condition{Type: clusterv1.ReadyCondition, Status: metav1.ConditionFalse, Reason: clusterv1.NotReadyReason})
}

func withoutNode(m *clusterv1.Machine) {
	m.Status.NodeRef = clusterv1.MachineNodeReference{}
}

func hooked(m *clusterv1.Machine) {
	withAnnotation(PreTerminateHookCleanupAnnotation, "")(m)
}

func withAnnotation(key, value string) machineOption {
	return func(m *clusterv1.Machine) {
		if m.Annotations == nil {
			m.Annotations = map[string]string{}
		}

		m.Annotations[key] = value
	}
}

// deletingAt marks the machine as deleted at the time, with the MachineDeleting condition at the reason.
func deletingAt(at time.Time, reason string) machineOption {
	return func(m *clusterv1.Machine) {
		m.DeletionTimestamp = ptr.To(metav1.NewTime(at))
		conditions.Set(m, metav1.Condition{Type: clusterv1.MachineDeletingCondition, Status: metav1.ConditionTrue, Reason: reason})
	}
}

func waitingForHook(at time.Time) machineOption {
	return deletingAt(at, clusterv1.MachineDeletingWaitingForPreTerminateHookReason)
}

func newLifecycleTCP(replicas int32) *controlplanev1.TalosControlPlane {
	return &controlplanev1.TalosControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "tcp", Namespace: lifecycleNamespace, UID: "tcp-uid"},
		Spec:       controlplanev1.TalosControlPlaneSpec{Replicas: ptr.To(replicas)},
		Status: controlplanev1.TalosControlPlaneStatus{
			Initialization: controlplanev1.TalosControlPlaneInitializationStatus{ControlPlaneInitialized: ptr.To(true)},
		},
	}
}

func newLifecycleMachine(tcp *controlplanev1.TalosControlPlane, name string, created time.Time, opts ...machineOption) *clusterv1.Machine {
	m := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         lifecycleNamespace,
			CreationTimestamp: metav1.NewTime(created),
			Finalizers:        []string{keepFinalizer},
			Labels: map[string]string{
				clusterv1.ClusterNameLabel:         lifecycleCluster,
				clusterv1.MachineControlPlaneLabel: "",
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane")),
			},
		},
		Spec: clusterv1.MachineSpec{ClusterName: lifecycleCluster},
		Status: clusterv1.MachineStatus{
			NodeRef: clusterv1.MachineNodeReference{Name: name},
			Conditions: []metav1.Condition{
				{Type: clusterv1.ReadyCondition, Status: metav1.ConditionTrue, Reason: clusterv1.ReadyReason, LastTransitionTime: metav1.NewTime(created)},
				{Type: clusterv1.MachineHealthCheckSucceededCondition, Status: metav1.ConditionTrue, Reason: clusterv1.MachineHealthCheckSucceededReason, LastTransitionTime: metav1.NewTime(created)},
			},
		},
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

type lifecycleEnv struct {
	t        *testing.T
	ctx      context.Context
	client   client.Client
	api      *fakeMachineAPI
	recorder *events.FakeRecorder
	r        *TalosControlPlaneReconciler
	tcp      *controlplanev1.TalosControlPlane
	cluster  *clusterv1.Cluster
}

func newLifecycleEnv(t *testing.T, api *fakeMachineAPI, tcp *controlplanev1.TalosControlPlane, machines ...*clusterv1.Machine) *lifecycleEnv {
	t.Helper()

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clusterv1.AddToScheme, controlplanev1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}

	objs := []client.Object{tcp}
	for _, m := range machines {
		objs = append(objs, m)
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&clusterv1.Machine{}, &controlplanev1.TalosControlPlane{}).
		Build()

	recorder := events.NewFakeRecorder(100)

	return &lifecycleEnv{
		t:        t,
		ctx:      t.Context(),
		client:   c,
		api:      api,
		recorder: recorder,
		r: &TalosControlPlaneReconciler{
			Client:     c,
			APIReader:  c,
			Log:        testr.New(t),
			Scheme:     scheme,
			Recorder:   recorder,
			machineAPI: api,
		},
		tcp:     tcp,
		cluster: &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: lifecycleCluster, Namespace: lifecycleNamespace}},
	}
}

func (e *lifecycleEnv) machineList() *clusterv1.MachineList {
	e.t.Helper()

	list := &clusterv1.MachineList{}
	if err := e.client.List(e.ctx, list, client.InNamespace(lifecycleNamespace)); err != nil {
		e.t.Fatal(err)
	}

	return list
}

func (e *lifecycleEnv) machine(name string) *clusterv1.Machine {
	e.t.Helper()

	m := &clusterv1.Machine{}
	if err := e.client.Get(e.ctx, client.ObjectKey{Namespace: lifecycleNamespace, Name: name}, m); err != nil {
		e.t.Fatal(err)
	}

	return m
}

func (e *lifecycleEnv) hasHook(name string) bool {
	_, ok := e.machine(name).Annotations[PreTerminateHookCleanupAnnotation]

	return ok
}

func (e *lifecycleEnv) hooks() (time.Duration, error) {
	result, err := e.r.reconcilePreTerminateHooks(e.ctx, e.cluster, e.tcp, e.machineList())

	return result.RequeueAfter, err
}

func (e *lifecycleEnv) events() []string {
	var events []string

	for {
		select {
		case event := <-e.recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

func expectCalls(t *testing.T, api *fakeMachineAPI, want ...string) {
	t.Helper()

	if got := api.recorded(); !slices.Equal(got, want) {
		t.Fatalf("etcd calls = %q, want %q", got, want)
	}
}

var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func TestPreTerminateHookIsAddedToControlledMachines(t *testing.T) {
	tcp := newLifecycleTCP(3)
	other := newLifecycleMachine(tcp, "other", epoch)
	other.OwnerReferences = nil

	env := newLifecycleEnv(t, newFakeMachineAPI("a", "b"), tcp,
		newLifecycleMachine(tcp, "a", epoch),
		newLifecycleMachine(tcp, "b", epoch, deletingAt(epoch, clusterv1.MachineDeletingDrainingNodeReason)),
		other,
	)

	if _, err := env.hooks(); err != nil {
		t.Fatal(err)
	}

	if !env.hasHook("a") {
		t.Fatal("the hook was not added to a controlled machine")
	}

	if env.hasHook("b") {
		t.Fatal("the hook was added to a machine that is already deleting")
	}

	if env.hasHook("other") {
		t.Fatal("the hook was added to a machine another controller owns")
	}
}

func TestPreTerminateHookWaitsForThePreTerminatePhase(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")
	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, deletingAt(epoch, clusterv1.MachineDeletingDrainingNodeReason)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	requeue, err := env.hooks()
	if err != nil {
		t.Fatal(err)
	}

	if requeue == 0 || !env.hasHook("a") {
		t.Fatal("the hook was released before the machine reached the pre-terminate phase")
	}

	expectCalls(t, api)
}

func TestPreTerminateHookWaitsForOtherHooks(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")
	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch),
			withAnnotation(clusterv1.PreTerminateDeleteHookAnnotationPrefix+"/other", "")),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	if _, err := env.hooks(); err != nil {
		t.Fatal(err)
	}

	if !env.hasHook("a") {
		t.Fatal("the hook was released while another pre-terminate hook was pending")
	}

	expectCalls(t, api)
}

func TestPreTerminateHookLeavesEtcdFromTheMachine(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")
	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	if _, err := env.hooks(); err != nil {
		t.Fatal(err)
	}

	expectCalls(t, api, "leave:a")

	if env.hasHook("a") {
		t.Fatal("the hook was not released after the member left")
	}

	if events := env.events(); len(events) != 1 || !strings.Contains(events[0], etcdMemberRemovedEvent) {
		t.Fatalf("events = %q, want one %s event", events, etcdMemberRemovedEvent)
	}
}

func TestPreTerminateHookRemovesAnUnreachableMemberThroughAPeer(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")
	api.etcdDown["a"] = true

	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	if _, err := env.hooks(); err != nil {
		t.Fatal(err)
	}

	expectCalls(t, api, "remove:a:via=b")

	if env.hasHook("a") {
		t.Fatal("the hook was not released after the member was removed")
	}
}

func TestPreTerminateHookKeepsTheHookWhenTheMemberCannotBeRemoved(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")
	api.etcdDown["a"] = true
	api.removeErr = errors.New("etcd refused")

	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	if _, err := env.hooks(); err == nil {
		t.Fatal("expected the failed removal to be reported")
	}

	if !env.hasHook("a") {
		t.Fatal("the hook was released although the member is still in etcd")
	}
}

func TestPreTerminateHookReleasesAMachineThatIsNotAMember(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("b", "c")
	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	if _, err := env.hooks(); err != nil {
		t.Fatal(err)
	}

	expectCalls(t, api)

	if env.hasHook("a") {
		t.Fatal("the hook of a machine without a member was not released")
	}
}

func TestPreTerminateHookHoldsWhenRemovalWouldLoseQuorum(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")
	api.unreachable["c"] = true

	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	requeue, err := env.hooks()
	if err != nil {
		t.Fatal(err)
	}

	if requeue == 0 || !env.hasHook("a") {
		t.Fatal("the hook was released although removing the member costs quorum")
	}

	expectCalls(t, api)

	if events := env.events(); len(events) != 1 || !strings.Contains(events[0], etcdQuorumAtRiskEvent) {
		t.Fatalf("events = %q, want one %s event", events, etcdQuorumAtRiskEvent)
	}
}

func TestPreTerminateHookHoldsWhileAMemberIsALearner(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c", "d")
	api.members[3].IsLearner = true

	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
		newLifecycleMachine(tcp, "d", epoch, hooked),
	)

	if _, err := env.hooks(); err != nil {
		t.Fatal(err)
	}

	if !env.hasHook("a") {
		t.Fatal("the hook was released while a learner waits to be promoted")
	}

	expectCalls(t, api)
}

func TestPreTerminateHookKeepsTheHookWhenEtcdCannotBeInspected(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")

	for _, name := range []string{"a", "b", "c"} {
		api.unreachable[name] = true
	}

	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	if _, err := env.hooks(); err == nil {
		t.Fatal("expected the failed inspection to be reported")
	}

	if !env.hasHook("a") {
		t.Fatal("the hook was released without knowing whether the member is gone")
	}
}

func TestPreTerminateHookServicesOnlyTheOldestDeletion(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c", "d", "e")
	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch.Add(time.Minute))),
		newLifecycleMachine(tcp, "b", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "c", epoch, hooked),
		newLifecycleMachine(tcp, "d", epoch, hooked),
		newLifecycleMachine(tcp, "e", epoch, hooked),
	)

	requeue, err := env.hooks()
	if err != nil {
		t.Fatal(err)
	}

	expectCalls(t, api, "leave:b")

	if requeue == 0 || !env.hasHook("a") || env.hasHook("b") {
		t.Fatal("expected only the oldest deletion to be serviced, and a requeue for the other")
	}
}

func TestPreTerminateHookWaitsForADeletionWithoutTheHook(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")
	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch.Add(time.Minute))),
		newLifecycleMachine(tcp, "b", epoch, deletingAt(epoch, clusterv1.MachineDeletingDrainingNodeReason)),
		newLifecycleMachine(tcp, "c", epoch, hooked),
	)

	requeue, err := env.hooks()
	if err != nil {
		t.Fatal(err)
	}

	if requeue == 0 || !env.hasHook("a") {
		t.Fatal("a member was removed while another deletion's member is unaccounted for")
	}

	expectCalls(t, api)
}

func TestPreTerminateHookReleasesWithoutEtcdWhenEverythingGoes(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*lifecycleEnv)
	}{
		{name: "cluster deleting", mutate: func(e *lifecycleEnv) { e.cluster.DeletionTimestamp = ptr.To(metav1.NewTime(epoch)) }},
		{name: "control plane deleting", mutate: func(e *lifecycleEnv) { e.tcp.DeletionTimestamp = ptr.To(metav1.NewTime(epoch)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tcp := newLifecycleTCP(3)
			api := newFakeMachineAPI("a", "b", "c")
			env := newLifecycleEnv(t, api, tcp,
				newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
				newLifecycleMachine(tcp, "b", epoch, hooked),
				newLifecycleMachine(tcp, "c", epoch, hooked),
			)

			tt.mutate(env)

			if _, err := env.hooks(); err != nil {
				t.Fatal(err)
			}

			expectCalls(t, api)

			if env.hasHook("a") {
				t.Fatal("the hook was not released")
			}
		})
	}
}

func TestPreTerminateHookReleasesTheLastMachine(t *testing.T) {
	tcp := newLifecycleTCP(1)
	api := newFakeMachineAPI("a")
	env := newLifecycleEnv(t, api, tcp, newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)))

	if _, err := env.hooks(); err != nil {
		t.Fatal(err)
	}

	expectCalls(t, api)

	if env.hasHook("a") {
		t.Fatal("the hook of the last machine was not released")
	}
}

func TestReconcileDeleteReleasesTheHooks(t *testing.T) {
	tcp := newLifecycleTCP(3)
	tcp.Finalizers = []string{controlplanev1.TalosControlPlaneFinalizer}
	tcp.DeletionTimestamp = ptr.To(metav1.NewTime(epoch))

	env := newLifecycleEnv(t, newFakeMachineAPI("a", "b"), tcp,
		newLifecycleMachine(tcp, "a", epoch, hooked, waitingForHook(epoch)),
		newLifecycleMachine(tcp, "b", epoch, hooked),
	)

	if _, err := env.r.reconcileDelete(env.ctx, env.cluster, env.tcp); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"a", "b"} {
		m := env.machine(name)

		if _, ok := m.Annotations[PreTerminateHookCleanupAnnotation]; ok {
			t.Fatalf("the hook of machine %s was not released", name)
		}

		if m.DeletionTimestamp.IsZero() {
			t.Fatalf("machine %s was not deleted", name)
		}
	}
}

func TestDesiredMachineAnnotations(t *testing.T) {
	tcp := newLifecycleTCP(3)
	tcp.Spec.MachineTemplate.ObjectMeta.Annotations = map[string]string{"example.com/note": "kept"}

	got := desiredMachineAnnotations(tcp)
	if _, ok := got[PreTerminateHookCleanupAnnotation]; !ok || got["example.com/note"] != "kept" {
		t.Fatalf("annotations = %v, want the template's and the hook", got)
	}

	if _, ok := got[remediationForAnnotation]; ok {
		t.Fatal("a machine created outside a remediation carries the remediation record")
	}

	tcp.Annotations = map[string]string{remediationInProgressAnnotation: `{"machine":"a"}`}

	if got := desiredMachineAnnotations(tcp); got[remediationForAnnotation] != `{"machine":"a"}` {
		t.Fatalf("annotations = %v, want the remediation record on the replacement", got)
	}
}

func TestCanSafelyRemoveEtcdMember(t *testing.T) {
	tcp := newLifecycleTCP(3)
	machinesNamed := func(names ...string) []*clusterv1.Machine {
		out := make([]*clusterv1.Machine, 0, len(names))
		for _, name := range names {
			out = append(out, newLifecycleMachine(tcp, name, epoch))
		}

		return out
	}
	healthyOnly := func(names ...string) map[string]bool {
		out := map[string]bool{}
		for _, name := range names {
			out[name] = true
		}

		return out
	}

	for _, tt := range []struct {
		name     string
		members  []*machineapi.EtcdMember
		machines []*clusterv1.Machine
		healthy  map[string]bool
		want     bool
	}{
		{
			name:     "three healthy members",
			members:  etcdMembersFor("a", "b", "c"),
			machines: machinesNamed("a", "b", "c"),
			healthy:  healthyOnly("a", "b", "c"),
			want:     true,
		},
		{
			name:     "the target is the dead member",
			members:  etcdMembersFor("a", "b", "c"),
			machines: machinesNamed("a", "b", "c"),
			healthy:  healthyOnly("b", "c"),
			want:     true,
		},
		{
			name:     "another member is dead",
			members:  etcdMembersFor("a", "b", "c"),
			machines: machinesNamed("a", "b", "c"),
			healthy:  healthyOnly("a", "b"),
			want:     false,
		},
		{
			name:     "five members, one other dead",
			members:  etcdMembersFor("a", "b", "c", "d", "e"),
			machines: machinesNamed("a", "b", "c", "d", "e"),
			healthy:  healthyOnly("a", "b", "c", "d"),
			want:     true,
		},
		{
			name:     "a member matching no machine counts as unhealthy",
			members:  etcdMembersFor("a", "b", "x"),
			machines: machinesNamed("a", "b"),
			healthy:  healthyOnly("a", "b"),
			want:     false,
		},
		{
			name:     "the target is not a member",
			members:  etcdMembersFor("b", "c"),
			machines: machinesNamed("a", "b", "c"),
			healthy:  healthyOnly("b", "c"),
			want:     true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, details := canSafelyRemoveEtcdMember(tt.members, tt.machines[0], tt.machines, tt.healthy)
			if got != tt.want {
				t.Fatalf("canSafelyRemoveEtcdMember() = %t (%s), want %t", got, details, tt.want)
			}
		})
	}
}

func (e *lifecycleEnv) remediate() bool {
	e.t.Helper()

	_, remediated, err := e.r.reconcileUnhealthyMachines(e.ctx, e.tcp, e.cluster, e.machineList().Items)
	if err != nil {
		e.t.Fatal(err)
	}

	return remediated
}

func (e *lifecycleEnv) expectNotRemediated(name, reason, message string) {
	e.t.Helper()

	m := e.machine(name)
	if !m.DeletionTimestamp.IsZero() {
		e.t.Fatalf("machine %s was deleted", name)
	}

	c := conditions.Get(m, clusterv1.MachineOwnerRemediatedCondition)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != reason || !strings.Contains(c.Message, message) {
		e.t.Fatalf("OwnerRemediated = %+v, want reason %s with %q", c, reason, message)
	}

	if remediationInProgress(e.tcp) {
		e.t.Fatal("a remediation was recorded")
	}
}

func (e *lifecycleEnv) expectRemediated(name string) {
	e.t.Helper()

	if m := e.machine(name); m.DeletionTimestamp.IsZero() {
		e.t.Fatalf("machine %s was not deleted", name)
	} else if c := conditions.Get(m, clusterv1.MachineOwnerRemediatedCondition); c == nil || c.Reason != clusterv1.MachineSetMachineRemediationMachineDeletingReason {
		e.t.Fatalf("OwnerRemediated = %+v, want the machine deleting", c)
	}

	stored := &controlplanev1.TalosControlPlane{}
	if err := e.client.Get(e.ctx, client.ObjectKeyFromObject(e.tcp), stored); err != nil {
		e.t.Fatal(err)
	}

	data, err := remediationDataFromAnnotation(stored.Annotations[remediationInProgressAnnotation])
	if err != nil || data.Machine != name {
		e.t.Fatalf("stored remediation record = %v (%v), want one for %s", data, err, name)
	}
}

func TestRemediationDeletesTheFlaggedMachine(t *testing.T) {
	tcp := newLifecycleTCP(3)
	env := newLifecycleEnv(t, newFakeMachineAPI("a", "b", "c"), tcp,
		newLifecycleMachine(tcp, "a", epoch),
		newLifecycleMachine(tcp, "b", epoch, flagged),
		newLifecycleMachine(tcp, "c", epoch),
	)

	if !env.remediate() {
		t.Fatal("the flagged machine was not remediated")
	}

	env.expectRemediated("b")
	expectCalls(t, env.api)
}

func TestRemediationRemovesOnlyOneMachineAtATime(t *testing.T) {
	tcp := newLifecycleTCP(5)
	env := newLifecycleEnv(t, newFakeMachineAPI("a", "b", "c", "d", "e"), tcp,
		newLifecycleMachine(tcp, "a", epoch, flagged),
		newLifecycleMachine(tcp, "b", epoch.Add(time.Minute), flagged),
		newLifecycleMachine(tcp, "c", epoch),
		newLifecycleMachine(tcp, "d", epoch),
		newLifecycleMachine(tcp, "e", epoch),
	)

	if !env.remediate() {
		t.Fatal("the first flagged machine was not remediated")
	}

	env.expectRemediated("a")

	if env.remediate() {
		t.Fatal("a second machine was remediated while the first one is deleting")
	}

	if !env.machine("b").DeletionTimestamp.IsZero() {
		t.Fatal("the second flagged machine was deleted")
	}
}

func TestRemediationRefusesToLoseQuorum(t *testing.T) {
	tcp := newLifecycleTCP(3)
	api := newFakeMachineAPI("a", "b", "c")
	api.unreachable["a"] = true
	api.unreachable["c"] = true

	env := newLifecycleEnv(t, api, tcp,
		newLifecycleMachine(tcp, "a", epoch, flagged),
		newLifecycleMachine(tcp, "b", epoch),
		newLifecycleMachine(tcp, "c", epoch, flagged),
	)

	if env.remediate() {
		t.Fatal("remediation went ahead with etcd short of quorum")
	}

	env.expectNotRemediated("a", clusterv1.MachineSetMachineCannotBeRemediatedReason, "quorum")
}

func TestRemediationPreconditions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		tcp      func(*controlplanev1.TalosControlPlane)
		machines func(*controlplanev1.TalosControlPlane) []*clusterv1.Machine
		reason   string
		message  string
	}{
		{
			name: "single machine",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch, flagged)}
			},
			reason:  clusterv1.MachineSetMachineCannotBeRemediatedReason,
			message: "less or equal to 1",
		},
		{
			name: "not initialised",
			tcp: func(tcp *controlplanev1.TalosControlPlane) {
				tcp.Status.Initialization.ControlPlaneInitialized = nil
			},
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch, flagged), newLifecycleMachine(tcp, "b", epoch)}
			},
			reason:  clusterv1.MachineSetMachineRemediationDeferredReason,
			message: "initialized",
		},
		{
			name: "another machine deleting",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{
					newLifecycleMachine(tcp, "a", epoch, flagged),
					newLifecycleMachine(tcp, "b", epoch, deletingAt(epoch, clusterv1.MachineDeletingDrainingNodeReason)),
					newLifecycleMachine(tcp, "c", epoch),
				}
			},
			reason:  clusterv1.MachineSetMachineRemediationDeferredReason,
			message: "deletion of control plane machine b",
		},
		{
			name: "another machine provisioning",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{
					newLifecycleMachine(tcp, "a", epoch, flagged),
					newLifecycleMachine(tcp, "b", epoch, withoutNode),
					newLifecycleMachine(tcp, "c", epoch),
				}
			},
			reason:  clusterv1.MachineSetMachineRemediationDeferredReason,
			message: "provisioning of control plane machine b",
		},
		{
			name: "remediation in progress",
			tcp: func(tcp *controlplanev1.TalosControlPlane) {
				tcp.Annotations = map[string]string{remediationInProgressAnnotation: fmt.Sprintf(`{"machine":"gone","timestamp":%q}`, time.Now().UTC().Format(time.RFC3339))}
			},
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch, flagged), newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "c", epoch)}
			},
			reason:  clusterv1.MachineSetMachineRemediationDeferredReason,
			message: "previous remediation",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tcp := newLifecycleTCP(3)
			if tt.tcp != nil {
				tt.tcp(tcp)
			}

			env := newLifecycleEnv(t, newFakeMachineAPI("a", "b", "c"), tcp, tt.machines(tcp)...)

			if env.remediate() {
				t.Fatal("remediation went ahead")
			}

			c := conditions.Get(env.machine("a"), clusterv1.MachineOwnerRemediatedCondition)
			if c == nil || c.Reason != tt.reason || !strings.Contains(c.Message, tt.message) {
				t.Fatalf("OwnerRemediated = %+v, want reason %s with %q", c, tt.reason, tt.message)
			}

			if !env.machine("a").DeletionTimestamp.IsZero() {
				t.Fatal("the flagged machine was deleted")
			}
		})
	}
}

func TestRemediationRetryLimits(t *testing.T) {
	now := time.Now().UTC()
	record := func(at time.Time, retries int) string {
		value, err := (&remediationData{Machine: "old", Timestamp: metav1.NewTime(at), RetryCount: retries}).marshal()
		if err != nil {
			t.Fatal(err)
		}

		return value
	}

	for _, tt := range []struct {
		name       string
		annotation string
		reason     string
		retries    int
	}{
		{name: "first remediation"},
		{name: "replacement healthy long enough", annotation: record(now.Add(-2*time.Hour), 3)},
		{name: "too soon after the previous one", annotation: record(now.Add(-time.Minute), 0), reason: clusterv1.MachineSetMachineRemediationDeferredReason},
		{name: "a retry", annotation: record(now.Add(-10*time.Minute), 1), retries: 2},
		{name: "out of retries", annotation: record(now.Add(-10*time.Minute), remediationMaxRetry), reason: clusterv1.MachineSetMachineCannotBeRemediatedReason},
	} {
		t.Run(tt.name, func(t *testing.T) {
			target := newLifecycleMachine(newLifecycleTCP(3), "a", epoch)
			if tt.annotation != "" {
				withAnnotation(remediationForAnnotation, tt.annotation)(target)
			}

			data, reason, _ := checkRemediationRetryLimits(target, now, testr.New(t))
			if reason != tt.reason {
				t.Fatalf("reason = %q, want %q", reason, tt.reason)
			}

			if reason == "" && data.RetryCount != tt.retries {
				t.Fatalf("retries = %d, want %d", data.RetryCount, tt.retries)
			}
		})
	}
}

func TestRemediationInProgressCompletes(t *testing.T) {
	now := time.Now().UTC()
	recorded := now.Add(-10 * time.Minute)
	value := fmt.Sprintf(`{"machine":"old","timestamp":%q,"retryCount":0}`, recorded.Format(time.RFC3339))

	for _, tt := range []struct {
		name     string
		machines func(*controlplanev1.TalosControlPlane) []*clusterv1.Machine
		cleared  bool
	}{
		{
			name: "replacement ready",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "new", now)}
			},
			cleared: true,
		},
		{
			name: "replacement not ready yet",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{
					newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch),
					newLifecycleMachine(tcp, "new", now, func(m *clusterv1.Machine) {
						conditions.Set(m, metav1.Condition{Type: clusterv1.ReadyCondition, Status: metav1.ConditionFalse, Reason: clusterv1.NotReadyReason})
					}),
				}
			},
		},
		{
			name: "remediated machine never deleted",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "old", epoch), newLifecycleMachine(tcp, "b", epoch)}
			},
			cleared: true,
		},
		{
			name: "remediated machine still deleting",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "old", epoch, waitingForHook(recorded)), newLifecycleMachine(tcp, "b", epoch)}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tcp := newLifecycleTCP(3)
			tcp.Annotations = map[string]string{remediationInProgressAnnotation: value}

			env := newLifecycleEnv(t, newFakeMachineAPI("a", "b", "new"), tcp, tt.machines(tcp)...)

			var machines []*clusterv1.Machine
			for _, m := range env.machineList().Items {
				machines = append(machines, &m)
			}

			if err := env.r.reconcileRemediationInProgress(env.ctx, tcp, machines, now, testr.New(t)); err != nil {
				t.Fatal(err)
			}

			if cleared := !remediationInProgress(tcp); cleared != tt.cleared {
				t.Fatalf("cleared = %t, want %t", cleared, tt.cleared)
			}

			if tt.cleared && tt.name == "replacement ready" && env.machine("new").Annotations[remediationForAnnotation] != value {
				t.Fatal("the remediation record was not moved to the replacement")
			}
		})
	}
}

func TestRemediationClearsStaleOwnerRemediated(t *testing.T) {
	tcp := newLifecycleTCP(3)
	recovered := newLifecycleMachine(tcp, "a", epoch, func(m *clusterv1.Machine) {
		conditions.Set(m, metav1.Condition{Type: clusterv1.MachineOwnerRemediatedCondition, Status: metav1.ConditionFalse, Reason: clusterv1.MachineOwnerRemediatedWaitingForRemediationReason})
	})

	env := newLifecycleEnv(t, newFakeMachineAPI("a", "b", "c"), tcp, recovered, newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "c", epoch))

	if env.remediate() {
		t.Fatal("a recovered machine was remediated")
	}

	if conditions.Has(env.machine("a"), clusterv1.MachineOwnerRemediatedCondition) {
		t.Fatal("the stale OwnerRemediated condition was kept")
	}
}

func TestMachineToBeRemediated(t *testing.T) {
	tcp := newLifecycleTCP(3)
	older := newLifecycleMachine(tcp, "older", epoch, flagged)
	newer := newLifecycleMachine(tcp, "newer", epoch.Add(time.Hour), flagged)
	noNode := newLifecycleMachine(tcp, "no-node", epoch.Add(2*time.Hour), flagged, withoutNode)
	annotated := newLifecycleMachine(tcp, "annotated", epoch.Add(3*time.Hour), flagged, withAnnotation(clusterv1.RemediateMachineAnnotation, ""))

	for _, tt := range []struct {
		machines []*clusterv1.Machine
		want     string
	}{
		{machines: []*clusterv1.Machine{newer, older}, want: "older"},
		{machines: []*clusterv1.Machine{older, noNode}, want: "no-node"},
		{machines: []*clusterv1.Machine{older, noNode, annotated}, want: "annotated"},
		{machines: []*clusterv1.Machine{newLifecycleMachine(tcp, "healthy", epoch)}},
	} {
		got := machineToBeRemediated(tt.machines)

		switch {
		case tt.want == "" && got != nil:
			t.Fatalf("machineToBeRemediated() = %s, want none", got.Name)
		case tt.want != "" && (got == nil || got.Name != tt.want):
			t.Fatalf("machineToBeRemediated() = %v, want %s", got, tt.want)
		}
	}
}

func TestPreflightChecks(t *testing.T) {
	inProgress := fmt.Sprintf(`{"machine":"gone","timestamp":%q}`, time.Now().UTC().Format(time.RFC3339))

	for _, tt := range []struct {
		name        string
		remediating bool
		scaleUp     bool
		machines    func(*controlplanev1.TalosControlPlane) []*clusterv1.Machine
		api         func(*fakeMachineAPI)
		exclude     string
		want        string
	}{
		{
			name: "healthy",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "c", epoch)}
			},
		},
		{
			name: "a machine is deleting",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch, waitingForHook(epoch))}
			},
			want: "waiting for machines to be deleted: b",
		},
		{
			name: "a machine has no node",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch, withoutNode)}
			},
			want: `machine "b" does not have a corresponding Node yet`,
		},
		{
			name: "a control plane service is unhealthy",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch)}
			},
			api:  func(api *fakeMachineAPI) { api.servicesDown["b"] = true },
			want: "control plane components are not healthy",
		},
		{
			name: "etcd is down on a machine",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "c", epoch)}
			},
			api:  func(api *fakeMachineAPI) { api.etcdDown["c"] = true },
			want: `etcd is not healthy on machine "c"`,
		},
		{
			name: "the excluded victim is not checked",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "c", epoch)}
			},
			api:     func(api *fakeMachineAPI) { api.etcdDown["c"] = true; api.servicesDown["c"] = true },
			exclude: "c",
		},
		{
			name: "a learner",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch)}
			},
			api:  func(api *fakeMachineAPI) { api.members[1].IsLearner = true },
			want: `etcd member "b" is a learner`,
		},
		{
			name:        "scale-up while remediating ignores the flagged machines",
			remediating: true,
			scaleUp:     true,
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{
					newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "c", epoch),
					newLifecycleMachine(tcp, "d", epoch, flagged),
				}
			},
			api: func(api *fakeMachineAPI) {
				api.members = etcdMembersFor("a", "b", "c", "d")
				api.unreachable["d"] = true
			},
		},
		{
			name:        "scale-up while remediating still needs quorum",
			remediating: true,
			scaleUp:     true,
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{
					newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch, flagged), newLifecycleMachine(tcp, "c", epoch, flagged),
				}
			},
			api: func(api *fakeMachineAPI) {
				api.unreachable["b"] = true
				api.unreachable["c"] = true
			},
			want: "1 of 3 etcd members are healthy, short of quorum",
		},
		{
			name: "a scale-down does not ignore flagged machines",
			machines: func(tcp *controlplanev1.TalosControlPlane) []*clusterv1.Machine {
				return []*clusterv1.Machine{newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "c", epoch, flagged)}
			},
			api:  func(api *fakeMachineAPI) { api.unreachable["c"] = true },
			want: `etcd is not healthy on machine "c"`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tcp := newLifecycleTCP(3)
			if tt.remediating {
				tcp.Annotations = map[string]string{remediationInProgressAnnotation: inProgress}
			}

			api := newFakeMachineAPI("a", "b", "c")
			if tt.api != nil {
				tt.api(api)
			}

			machines := tt.machines(tcp)
			env := newLifecycleEnv(t, api, tcp, machines...)

			var exclude []*clusterv1.Machine

			for _, m := range machines {
				if m.Name == tt.exclude {
					exclude = append(exclude, m)
				}
			}

			got := env.r.preflightChecks(env.ctx, tcp, collections.FromMachines(machines...), tt.scaleUp, exclude...)

			switch {
			case tt.want == "" && got != "":
				t.Fatalf("preflightChecks() = %q, want none", got)
			case tt.want != "" && !strings.Contains(got, tt.want):
				t.Fatalf("preflightChecks() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEtcdHealthcheckReportsQuorum(t *testing.T) {
	tcp := newLifecycleTCP(3)

	for _, tt := range []struct {
		name   string
		api    func(*fakeMachineAPI)
		reason string
	}{
		{name: "healthy"},
		{name: "one member down", api: func(api *fakeMachineAPI) { api.etcdDown["c"] = true }, reason: controlplanev1.EtcdClusterUnhealthyReason},
		{name: "two members down", api: func(api *fakeMachineAPI) { api.etcdDown["b"] = true; api.etcdDown["c"] = true }, reason: controlplanev1.EtcdClusterQuorumLostReason},
		{name: "two members unreachable", api: func(api *fakeMachineAPI) { api.unreachable["b"] = true; api.unreachable["c"] = true }, reason: controlplanev1.EtcdClusterInspectionFailedReason},
	} {
		t.Run(tt.name, func(t *testing.T) {
			api := newFakeMachineAPI("a", "b", "c")
			if tt.api != nil {
				tt.api(api)
			}

			env := newLifecycleEnv(t, api, tcp, newLifecycleMachine(tcp, "a", epoch), newLifecycleMachine(tcp, "b", epoch), newLifecycleMachine(tcp, "c", epoch))

			health, err := env.r.etcdHealthcheck(env.ctx, tcp, env.machineList().Items)

			switch {
			case tt.reason == "" && err != nil:
				t.Fatalf("etcdHealthcheck() = %v, want healthy", err)
			case tt.reason != "" && err == nil:
				t.Fatal("etcdHealthcheck() reported healthy")
			case tt.reason != "" && health.conditionReason() != tt.reason:
				t.Fatalf("reason = %s, want %s", health.conditionReason(), tt.reason)
			}
		})
	}
}
