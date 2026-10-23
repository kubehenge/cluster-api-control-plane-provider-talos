// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controlplanev1 "github.com/kubehenge/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func TestSetAvailableCondition(t *testing.T) {
	unreachable := errors.New("connection refused")

	etcd := func(reason string) func(*controlplanev1.TalosControlPlane) {
		return func(tcp *controlplanev1.TalosControlPlane) {
			status := metav1.ConditionFalse
			if reason == controlplanev1.EtcdClusterHealthyReason {
				status = metav1.ConditionTrue
			}

			conditions.Set(tcp, metav1.Condition{Type: controlplanev1.EtcdClusterHealthyCondition, Status: status, Reason: reason, Message: "detail"})
		}
	}

	for _, tt := range []struct {
		name             string
		mutate           func(*controlplanev1.TalosControlPlane)
		machinesWithNode int32
		workloadErr      error
		keep             bool
		status           metav1.ConditionStatus
		reason           string
	}{
		{name: "available", machinesWithNode: 1, status: metav1.ConditionTrue, reason: controlplanev1.AvailableReason},
		{
			name:             "deleting",
			mutate:           func(tcp *controlplanev1.TalosControlPlane) { tcp.DeletionTimestamp = ptr.To(metav1.Now()) },
			machinesWithNode: 1,
			status:           metav1.ConditionFalse,
			reason:           controlplanev1.NotAvailableReason,
		},
		{
			name:             "not initialised",
			mutate:           func(tcp *controlplanev1.TalosControlPlane) { tcp.Status.Initialization.ControlPlaneInitialized = nil },
			machinesWithNode: 1,
			status:           metav1.ConditionFalse,
			reason:           controlplanev1.NotAvailableReason,
		},
		{name: "unreachable", machinesWithNode: 1, workloadErr: unreachable, status: metav1.ConditionUnknown, reason: controlplanev1.RemoteConnectionProbeFailedReason},
		{
			name: "unreachable within the grace period keeps the last state",
			mutate: func(tcp *controlplanev1.TalosControlPlane) {
				conditions.Set(tcp, metav1.Condition{Type: controlplanev1.AvailableCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.AvailableReason})
			},
			machinesWithNode: 1,
			workloadErr:      unreachable,
			keep:             true,
			status:           metav1.ConditionTrue,
			reason:           controlplanev1.AvailableReason,
		},
		{name: "unreachable within the grace period without a last state", machinesWithNode: 1, workloadErr: unreachable, keep: true, status: metav1.ConditionUnknown, reason: controlplanev1.RemoteConnectionProbeFailedReason},
		{name: "etcd lost quorum", mutate: etcd(controlplanev1.EtcdClusterQuorumLostReason), machinesWithNode: 3, status: metav1.ConditionFalse, reason: controlplanev1.NotAvailableReason},
		{name: "etcd cannot be inspected", mutate: etcd(controlplanev1.EtcdClusterInspectionFailedReason), machinesWithNode: 3, status: metav1.ConditionUnknown, reason: controlplanev1.AvailableInspectionFailedReason},
		{name: "etcd degraded", mutate: etcd(controlplanev1.EtcdClusterUnhealthyReason), machinesWithNode: 3, status: metav1.ConditionTrue, reason: controlplanev1.AvailableReason},
		{name: "no machine with a node", status: metav1.ConditionFalse, reason: controlplanev1.NotAvailableReason},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tcp := newLifecycleTCP(3)
			if tt.mutate != nil {
				tt.mutate(tcp)
			}

			setAvailableCondition(tcp, tt.machinesWithNode, tt.workloadErr, tt.keep)

			c := conditions.Get(tcp, controlplanev1.AvailableCondition)
			if c == nil || c.Status != tt.status || c.Reason != tt.reason {
				t.Fatalf("Available = %+v, want %s with reason %s", c, tt.status, tt.reason)
			}
		})
	}
}

func TestMachinesBootstrappedFollowsStatus(t *testing.T) {
	tcp := newLifecycleTCP(3)
	r := &TalosControlPlaneReconciler{}

	if _, err := r.reconcileConditions(t.Context(), nil, tcp, nil); err != nil {
		t.Fatal(err)
	}

	if !conditions.IsFalse(tcp, controlplanev1.MachinesBootstrapped) {
		t.Fatal("MachinesBootstrapped is not False before the bootstrap")
	}

	tcp.Status.Bootstrapped = true

	if _, err := r.reconcileConditions(t.Context(), nil, tcp, nil); err != nil {
		t.Fatal(err)
	}

	if !conditions.IsTrue(tcp, controlplanev1.MachinesBootstrapped) {
		t.Fatal("MachinesBootstrapped did not follow status.bootstrapped")
	}
}

func TestClusterToTalosControlPlane(t *testing.T) {
	r := &TalosControlPlaneReconciler{Log: newLifecycleEnv(t, newFakeMachineAPI(), newLifecycleTCP(1)).r.Log}

	for _, tt := range []struct {
		name string
		ref  clusterv1.ContractVersionedObjectReference
		want bool
	}{
		{name: "ours", ref: clusterv1.ContractVersionedObjectReference{APIGroup: controlplanev1.GroupVersion.Group, Kind: "TalosControlPlane", Name: "tcp"}, want: true},
		{name: "another group", ref: clusterv1.ContractVersionedObjectReference{APIGroup: "controlplane.example.com", Kind: "TalosControlPlane", Name: "tcp"}},
		{name: "another kind", ref: clusterv1.ContractVersionedObjectReference{APIGroup: controlplanev1.GroupVersion.Group, Kind: "KubeadmControlPlane", Name: "tcp"}},
		{name: "none"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &clusterv1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: lifecycleCluster, Namespace: lifecycleNamespace},
				Spec:       clusterv1.ClusterSpec{ControlPlaneRef: tt.ref},
			}

			requests := r.ClusterToTalosControlPlane(t.Context(), cluster)
			if got := len(requests) == 1 && requests[0].NamespacedName == (client.ObjectKey{Namespace: lifecycleNamespace, Name: "tcp"}); got != tt.want {
				t.Fatalf("requests = %v, want a request: %t", requests, tt.want)
			}
		})
	}
}

func TestReconcileSetsPausedCondition(t *testing.T) {
	tcp := newLifecycleTCP(1)
	tcp.OwnerReferences = []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: lifecycleCluster}}

	env := newLifecycleEnv(t, newFakeMachineAPI(), tcp)

	cluster := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: lifecycleCluster, Namespace: lifecycleNamespace},
		Spec:       clusterv1.ClusterSpec{Paused: ptr.To(true)},
	}
	if err := env.client.Create(env.ctx, cluster); err != nil {
		t.Fatal(err)
	}

	result, err := env.r.Reconcile(env.ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tcp)})
	if err != nil || !result.IsZero() {
		t.Fatalf("Reconcile() = %+v, %v, want an empty result", result, err)
	}

	stored := &controlplanev1.TalosControlPlane{}
	if err := env.client.Get(env.ctx, client.ObjectKeyFromObject(tcp), stored); err != nil {
		t.Fatal(err)
	}

	if !conditions.IsTrue(stored, clusterv1.PausedCondition) {
		t.Fatalf("Paused = %+v, want True", conditions.Get(stored, clusterv1.PausedCondition))
	}

	if len(stored.Finalizers) != 0 {
		t.Fatal("a paused TalosControlPlane was reconciled")
	}
}
