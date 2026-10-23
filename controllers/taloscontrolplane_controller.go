// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1alpha3"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/certs"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	"sigs.k8s.io/cluster-api/util/labels/format"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/paused"
	"sigs.k8s.io/cluster-api/util/secret"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

const (
	requeueDuration = 30 * time.Second

	// machineCreatedRequeueAfter is how soon the control plane is looked at again after a Machine was created.
	machineCreatedRequeueAfter = 5 * time.Second

	// remoteConnectionGracePeriod is how long the Available condition keeps its last known state while the
	// workload cluster cannot be reached, as after a controller restart, before it turns Unknown. It is
	// KubeadmControlPlane's default.
	remoteConnectionGracePeriod = 5 * time.Minute
)

// TalosControlPlaneReconciler reconciles a TalosControlPlane object
type TalosControlPlaneReconciler struct {
	client.Client
	APIReader    client.Reader
	Log          logr.Logger
	Scheme       *runtime.Scheme
	ClusterCache clustercache.ClusterCache
	Recorder     events.EventRecorder

	// machineAPI replaces the calls to the Talos API of the machines in tests.
	machineAPI machineAPI
}

func (r *TalosControlPlaneReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&controlplanev1.TalosControlPlane{}).
		Owns(&clusterv1.Machine{}).
		Watches(
			&clusterv1.Cluster{},
			handler.EnqueueRequestsFromMapFunc(r.ClusterToTalosControlPlane),
		).
		// The connection to the workload cluster going up or down, or its health probe failing for
		// longer than the grace period, enqueues the object, so Available follows it.
		WatchesRawSource(r.ClusterCache.GetClusterSource("taloscontrolplane", r.ClusterToTalosControlPlane,
			clustercache.WatchForProbeFailure(remoteConnectionGracePeriod))).
		WithOptions(options).
		Complete(r)
}

// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;patch;update
// +kubebuilder:rbac:groups=core,resources=configmaps,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac,resources=roles,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac,resources=rolebindings,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io;bootstrap.cluster.x-k8s.io;controlplane.cluster.x-k8s.io,resources=*,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters;clusters/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines;machines/status,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

func (r *TalosControlPlaneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, reterr error) {
	logger := r.Log.WithValues("namespace", req.Namespace, "talosControlPlane", req.Name)

	// Fetch the TalosControlPlane instance.
	tcp := &controlplanev1.TalosControlPlane{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, tcp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// Fetch the Cluster.
	cluster, err := util.GetOwnerCluster(ctx, r.Client, tcp.ObjectMeta)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to retrieve owner Cluster from the API Server")

			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	// The owner reference is set by the Cluster controller, and setting it enqueues the object again.
	if cluster == nil {
		logger.Info("cluster Controller has not yet set OwnerRef")
		return ctrl.Result{}, nil
	}
	logger = logger.WithValues("cluster", cluster.Name)

	// The helper is created before the Paused condition is set: when only the condition's observed
	// generation changes, EnsurePausedCondition leaves the update to the final patch, which sends what
	// changed since the helper was created.
	patchHelper, err := patch.NewHelper(tcp, r.Client)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to configure the patch helper")
	}

	// The Cluster and TalosControlPlane watches enqueue the object again once it is unpaused.
	isPaused, _, err := paused.EnsurePausedCondition(ctx, r.Client, cluster, tcp)
	if err != nil || isPaused {
		if isPaused {
			logger.Info("reconciliation is paused for this object")
		}

		return ctrl.Result{}, err
	}

	// Add finalizer first if not exist to avoid the race condition between init and delete
	if ensureTalosControlPlaneFinalizers(tcp) {

		// patch and return right away instead of reusing the main defer,
		// because the main defer may take too much time to get cluster status

		if err := patchTalosControlPlane(ctx, patchHelper, tcp, patch.WithStatusObservedGeneration{}); err != nil {
			logger.Error(err, "failed to add finalizer to TalosControlPlane")
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	defer func() {
		r.Log.Info("attempting to set control plane status")

		// Always attempt to update status.
		if err := r.updateStatus(ctx, tcp, cluster); err != nil {
			logger.Error(err, "failed to update TalosControlPlane Status")

			reterr = kerrors.NewAggregate([]error{reterr, err})
		}

		// Always attempt to Patch the TalosControlPlane object and status after each reconciliation.
		if err := patchTalosControlPlane(ctx, patchHelper, tcp, patch.WithStatusObservedGeneration{}); err != nil {
			logger.Error(err, "failed to patch TalosControlPlane")
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}

		// TODO: remove this as soon as we have a proper remote cluster cache in place.
		// Make TCP to requeue in case status is not ready, so we can check for node status without waiting for a full resync (by default 10 minutes).
		// Only requeue if we are not going in exponential backoff due to error, or if we are not already re-queueing, or if the object has a deletion timestamp.
		if reterr == nil && res.RequeueAfter <= 0 && tcp.DeletionTimestamp.IsZero() {
			deprecated := tcp.V1Beta1DeprecatedStatus()
			if !deprecated.Ready || deprecated.UnavailableReplicas > 0 {
				res = ctrl.Result{RequeueAfter: 20 * time.Second}
			}
		}

		logger.Info("successfully updated control plane status")
	}()

	if !tcp.ObjectMeta.DeletionTimestamp.IsZero() {
		// Handle deletion reconciliation loop.
		return r.reconcileDelete(ctx, cluster, tcp)
	}

	return r.reconcile(ctx, cluster, tcp)
}

func (r *TalosControlPlaneReconciler) reconcile(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) (res ctrl.Result, err error) {
	logger := ctrl.LoggerFrom(ctx, "cluster", cluster.Name)
	logger.Info("reconcile TalosControlPlane")

	ownedMachines, err := r.getControlPlaneMachinesForCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		logger.Error(err, "failed to retrieve control plane machines for cluster")

		return ctrl.Result{}, err
	}

	// A deleting machine waits at its pre-terminate hook whatever else is wrong, so the hooks come first.
	hookResult, hookErr := r.reconcilePreTerminateHooks(ctx, cluster, tcp, &ownedMachines)
	if hookErr != nil {
		logger.Info("failed to reconcile the pre-terminate hooks of the control plane machines", "error", hookErr)
	}

	res, err = r.reconcileControlPlane(ctx, cluster, tcp, &ownedMachines)

	return util.LowestNonZeroResult(res, hookResult), kerrors.NewAggregate([]error{err, hookErr})
}

func (r *TalosControlPlaneReconciler) reconcileControlPlane(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (res ctrl.Result, err error) {
	logger := ctrl.LoggerFrom(ctx, "cluster", cluster.Name)

	// Update ownerrefs on infra templates
	if err := r.reconcileExternalReference(ctx, tcp.Spec.MachineTemplate.Spec.InfrastructureRef, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// Machines are created only once the cluster infrastructure is provisioned. The set-once
	// initialization field is used instead of the InfrastructureReady condition, which can go back to
	// false during the cluster lifecycle and would deadlock the bootstrap. The Cluster watch enqueues
	// the object again once it is set.
	if !ptr.Deref(cluster.Status.Initialization.InfrastructureProvisioned, false) {
		logger.Info("cluster infra not provisioned")

		return ctrl.Result{}, nil
	}

	// If ControlPlaneEndpoint is not set, return early
	if !cluster.Spec.ControlPlaneEndpoint.IsValid() {
		logger.Info("cluster does not yet have a ControlPlaneEndpoint defined")

		return ctrl.Result{}, nil
	}

	ownedMachines := *machines

	if len(ownedMachines.Items) > 0 {
		machinesForConditions := make([]*clusterv1.Machine, 0, len(ownedMachines.Items))
		for i := range ownedMachines.Items {
			machinesForConditions = append(machinesForConditions, &ownedMachines.Items[i])
		}

		if err := conditions.SetAggregateCondition(
			machinesForConditions,
			tcp,
			string(clusterv1.MachineReadyCondition),
			conditions.TargetConditionType(controlplanev1.MachinesAllReadyCondition),
		); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to aggregate machine readiness conditions")
		}
	}

	var (
		errs        error
		result      ctrl.Result
		phaseResult ctrl.Result
	)

	// run all similar reconcile steps in the loop and pick the lowest RetryAfter, aggregate errors and check the requeue flags.
	for _, phase := range []func(context.Context, *clusterv1.Cluster, *controlplanev1.TalosControlPlane, *clusterv1.MachineList) (ctrl.Result, error){
		r.reconcileEtcdMembers,
		r.reconcileNodeHealth,
		r.reconcileConditions,
		r.reconcileKubeconfig,
		r.reconcileRemediationAndMachines,
	} {
		phaseResult, err = phase(ctx, cluster, tcp, &ownedMachines)
		if err != nil {
			errs = kerrors.NewAggregate([]error{errs, err})
		}

		result = util.LowestNonZeroResult(result, phaseResult)
	}

	if result.RequeueAfter != 0 {
		if err != nil {
			r.Log.Error(err, "reconcile failed", "requeue after", result.RequeueAfter.String(), "error", err.Error())
		}

		return result, nil
	}

	return result, errs
}

// ClusterToTalosControlPlane is a handler.ToRequestsFunc to be used to enqueue requests for reconciliation
// for TalosControlPlane based on updates to a Cluster.
func (r *TalosControlPlaneReconciler) ClusterToTalosControlPlane(_ context.Context, o client.Object) []ctrl.Request {
	c, ok := o.(*clusterv1.Cluster)
	if !ok {
		r.Log.Error(nil, fmt.Sprintf("expected a Cluster but got a %T", o))
		return nil
	}

	controlPlaneRef := c.Spec.ControlPlaneRef
	if controlPlaneRef.IsDefined() && controlPlaneRef.APIGroup == controlplanev1.GroupVersion.Group && controlPlaneRef.Kind == "TalosControlPlane" {
		return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: c.Namespace, Name: controlPlaneRef.Name}}}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) reconcileDelete(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) (ctrl.Result, error) {
	// Get list of all control plane machines
	ownedMachines, err := r.getControlPlaneMachinesForCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		r.Log.Error(err, "failed to retrieve control plane machines for cluster")

		return ctrl.Result{}, err
	}

	// If no control plane machines remain, remove the finalizer
	if len(ownedMachines.Items) == 0 {
		removeTalosControlPlaneFinalizers(tcp)
		return ctrl.Result{}, r.Client.Update(ctx, tcp)
	}

	for i := range ownedMachines.Items {
		ownedMachine := &ownedMachines.Items[i]

		// The whole control plane goes, so no etcd member needs to leave first: KCP releases every
		// hook on deletion too.
		if err := r.releasePreTerminateHook(ctx, ownedMachine); err != nil {
			return ctrl.Result{}, err
		}

		if !ownedMachine.DeletionTimestamp.IsZero() {
			continue
		}

		if err := r.Client.Delete(ctx, ownedMachine); err != nil && !apierrors.IsNotFound(err) {
			r.Log.Error(err, "failed to cleanup owned machine")
			return ctrl.Result{}, err
		}
	}

	conditions.Set(tcp, metav1.Condition{
		Type:   string(controlplanev1.ResizedCondition),
		Status: metav1.ConditionFalse,
		Reason: clusterv1.DeletingReason,
	})
	// Requeue the deletion so we can check to make sure machines got cleaned up
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

func (r *TalosControlPlaneReconciler) getControlPlaneMachinesForCluster(ctx context.Context, cluster client.ObjectKey) (clusterv1.MachineList, error) {
	selector := map[string]string{
		clusterv1.ClusterNameLabel:         cluster.Name,
		clusterv1.MachineControlPlaneLabel: "",
	}

	machineList := clusterv1.MachineList{}
	if err := r.Client.List(
		ctx,
		&machineList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return machineList, err
	}

	return machineList, nil
}

func ensureTalosControlPlaneFinalizers(tcp *controlplanev1.TalosControlPlane) bool {
	// The API server rejects any finalizer added to an object that is being deleted, so a
	// TalosControlPlane deleted with only the legacy finalizer keeps it until reconcileDelete
	// removes both.
	if !tcp.ObjectMeta.DeletionTimestamp.IsZero() {
		return false
	}

	changed := false

	if !controllerutil.ContainsFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer) {
		controllerutil.AddFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer)
		changed = true
	}

	if controllerutil.ContainsFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy) {
		controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy)
		changed = true
	}

	return changed
}

func removeTalosControlPlaneFinalizers(tcp *controlplanev1.TalosControlPlane) {
	controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer)
	controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy)
}

// getFailureDomain will return a slice of failure domains from the cluster status.
//
// Only returns domains where ControlPlane is true, and falls back to all domains if none are explicitly marked for control plane.
func (r *TalosControlPlaneReconciler) getFailureDomain(_ context.Context, cluster *clusterv1.Cluster) []string {
	if cluster.Status.FailureDomains == nil {
		return nil
	}

	var retList []string

	for _, fd := range cluster.Status.FailureDomains {
		if fd.ControlPlane != nil && *fd.ControlPlane {
			retList = append(retList, fd.Name)
		}
	}

	if len(retList) == 0 {
		// pick all failure domains if none are explicitly marked for control plane
		for _, fd := range cluster.Status.FailureDomains {
			retList = append(retList, fd.Name)
		}
	}

	slices.Sort(retList)

	return retList
}

// pickFailureDomain selects the failure domain with the fewest existing machines.
// This ensures control plane nodes are spread evenly across availability zones.
func pickFailureDomain(failureDomains []string, existingMachines []clusterv1.Machine) string {
	if len(failureDomains) == 0 {
		return ""
	}

	// Count existing machines per failure domain.
	counts := make(map[string]int, len(failureDomains))
	for _, fd := range failureDomains {
		counts[fd] = 0
	}

	for i := range existingMachines {
		if existingMachines[i].Spec.FailureDomain != "" && existingMachines[i].DeletionTimestamp.IsZero() {
			counts[existingMachines[i].Spec.FailureDomain]++
		}
	}

	// Pick the domain with the fewest machines (first in sorted order for determinism).
	best := failureDomains[0]

	for _, fd := range failureDomains[1:] {
		if counts[fd] < counts[best] {
			best = fd
		}
	}

	return best
}

func (r *TalosControlPlaneReconciler) bootControlPlane(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, first bool) (ctrl.Result, error) {
	// Since the cloned resource should eventually have a controller ref for the Machine, we create an
	// OwnerReference here without the Controller field set
	infraCloneOwner := &metav1.OwnerReference{
		APIVersion: controlplanev1.GroupVersion.String(),
		Kind:       "TalosControlPlane",
		Name:       tcp.Name,
		UID:        tcp.UID,
	}

	machineName, err := tcp.Spec.GenerateMachineName(cluster.Name, tcp.Name)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.MachineGenerationFailedReason,
			Message: fmt.Sprintf("Failed to generate machine name: %v", err),
		})

		return ctrl.Result{}, err
	}

	// Clone the infrastructure template. The infrastructureRef is a CAPI v1beta2
	// ContractVersionedObjectReference, so we resolve the concrete APIVersion via
	// contract labels before delegating to external.CreateFromTemplate.
	templateObj, err := external.GetObjectFromContractVersionedRef(ctx, r.Client, tcp.Spec.MachineTemplate.Spec.InfrastructureRef, tcp.Namespace)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.InfrastructureTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to retrieve infrastructure template: %v", err),
		})

		// A missing template is a user-recoverable misconfiguration; back off instead of
		// returning the error so the controller does not hot-loop at MaxConcurrentReconciles.
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		return ctrl.Result{}, err
	}

	templateRef := corev1.ObjectReference{
		APIVersion: templateObj.GetAPIVersion(),
		Kind:       templateObj.GetKind(),
		Name:       templateObj.GetName(),
		Namespace:  templateObj.GetNamespace(),
	}
	_, infraRef, err := external.CreateFromTemplate(ctx, &external.CreateFromTemplateInput{
		Client:      r.Client,
		TemplateRef: &templateRef,
		Namespace:   tcp.Namespace,
		Name:        machineName,
		OwnerRef:    infraCloneOwner,
		ClusterName: cluster.Name,
		Labels: map[string]string{
			clusterv1.MachineControlPlaneLabel: "",
		},
	})
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.InfrastructureTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to clone infrastructure template: %v", err),
		})

		return ctrl.Result{}, err
	}

	bootstrapConfig := &tcp.Spec.ControlPlaneConfig.ControlPlaneConfig
	if !reflect.ValueOf(tcp.Spec.ControlPlaneConfig.InitConfig).IsZero() && first {
		bootstrapConfig = &tcp.Spec.ControlPlaneConfig.InitConfig
	}

	// Clone the bootstrap configuration
	bootstrapRef, err := r.generateTalosConfig(ctx, tcp, machineName, bootstrapConfig)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.BootstrapTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to create bootstrap configuration: %v", err),
		})

		return ctrl.Result{}, err
	}

	machine := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:        machineName,
			Namespace:   tcp.Namespace,
			Labels:      controlPlaneMachineLabelsForCluster(tcp, cluster.Name),
			Annotations: desiredMachineAnnotations(tcp),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane")),
			},
		},
		Spec: clusterv1.MachineSpec{
			ClusterName:       cluster.Name,
			Version:           tcp.Spec.Version,
			InfrastructureRef: infraRef,
			ReadinessGates:    copyMachineReadinessGates(tcp.Spec.MachineTemplate.Spec.ReadinessGates),
			Deletion:          buildMachineDeletionSpec(tcp.Spec.MachineTemplate.Spec.Deletion),
			Bootstrap: clusterv1.Bootstrap{
				ConfigRef: *bootstrapRef,
			},
		},
	}

	failureDomains := r.getFailureDomain(ctx, cluster)
	if len(failureDomains) > 0 {
		// Fetch existing CP machines to determine least-used failure domain.
		existingMachines, err := r.getControlPlaneMachinesForCluster(ctx, util.ObjectKey(cluster))
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to get existing machines for failure domain selection")
		}
		machine.Spec.FailureDomain = pickFailureDomain(failureDomains, existingMachines.Items)
	}

	if err := r.Client.Create(ctx, machine); err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.MachineGenerationFailedReason,
			Message: fmt.Sprintf("Failed to create machine: %v", err),
		})

		return ctrl.Result{}, errors.Wrap(err, "Failed to create machine")
	}

	return ctrl.Result{RequeueAfter: machineCreatedRequeueAfter}, nil
}

func (r *TalosControlPlaneReconciler) bootstrapCluster(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	defer cancel()

	if len(machines) == 0 {
		return fmt.Errorf("no machines to bootstrap")
	}

	addresses := make(map[string]string, len(machines))

	for _, machine := range machines {
		address, ok := bootstrapAddress(machine)
		if !ok {
			return fmt.Errorf("machine %q doesn't have an any InternalIP or ExternalIP address yet", machine.Name)
		}

		addresses[machine.Name] = address
	}

	// A machine whose etcd data directory holds anything already runs or ran an etcd cluster, which a
	// bootstrap of another machine would split; Talos refuses to bootstrap a machine for the same reason.
	// Any machine that cannot be inspected means retrying later, never bootstrapping.
	for i := range machines {
		bootstrapped, err := r.etcdDataPresent(ctx, tcp, machines[i])
		if err != nil {
			return fmt.Errorf("failed to inspect the etcd data of machine %q: %w", machines[i].Name, err)
		}

		if bootstrapped {
			return nil
		}
	}

	first := slices.MinFunc(machines, func(a, b clusterv1.Machine) int {
		return strings.Compare(addresses[a.Name], addresses[b.Name])
	})

	c, err := r.talosconfigForMachines(ctx, tcp, first)
	if err != nil {
		return err
	}

	defer c.Close() //nolint:errcheck

	if err := c.Bootstrap(ctx, &machineapi.BootstrapRequest{}); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return err
		}
	}

	return nil
}

// bootstrapAddress returns the address the machine is ordered by for the bootstrap: its first
// InternalIP, or its first ExternalIP when it has none.
func bootstrapAddress(machine clusterv1.Machine) (string, bool) {
	for _, addressType := range []clusterv1.MachineAddressType{clusterv1.MachineInternalIP, clusterv1.MachineExternalIP} {
		for _, addr := range machine.Status.Addresses {
			if addr.Type == addressType {
				return addr.Address, true
			}
		}
	}

	return "", false
}

// etcdDataPresent reports whether the etcd data directory of the machine holds anything. The client
// points at that machine alone, so the answer is its own.
func (r *TalosControlPlaneReconciler) etcdDataPresent(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine clusterv1.Machine) (bool, error) {
	c, err := r.talosconfigForMachines(ctx, tcp, machine)
	if err != nil {
		return false, err
	}

	defer c.Close() //nolint:errcheck

	list, err := c.LS(ctx, &machineapi.ListRequest{Root: constants.EtcdDataPath, Recurse: true, RecursionDepth: 1})
	if err != nil {
		return false, err
	}

	listed := false

	for {
		info, err := list.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return false, err
		}

		if info.GetError() != "" {
			return false, fmt.Errorf("%s: %s", info.GetName(), info.GetError())
		}

		if info.GetRelativeName() == "." {
			listed = true

			continue
		}

		return true, nil
	}

	// Talos lists the directory itself first; a listing without it says nothing about the data.
	if !listed {
		return false, fmt.Errorf("the listing of %s is empty", constants.EtcdDataPath)
	}

	return false, nil
}

func (r *TalosControlPlaneReconciler) generateTalosConfig(ctx context.Context, tcp *controlplanev1.TalosControlPlane, name string, spec *cabptv1.TalosConfigSpec) (*clusterv1.ContractVersionedObjectReference, error) {
	owner := metav1.OwnerReference{
		APIVersion:         controlplanev1.GroupVersion.String(),
		Kind:               "TalosControlPlane",
		Name:               tcp.Name,
		UID:                tcp.UID,
		BlockOwnerDeletion: ptr.To(true),
	}

	bootstrapConfig := &cabptv1.TalosConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       tcp.Namespace,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: *spec,
	}

	if err := r.Client.Create(ctx, bootstrapConfig); err != nil {
		return nil, errors.Wrap(err, "Failed to create bootstrap configuration")
	}

	bootstrapRef := &clusterv1.ContractVersionedObjectReference{
		APIGroup: cabptv1.GroupVersion.Group,
		Kind:     "TalosConfig",
		Name:     bootstrapConfig.GetName(),
	}

	return bootstrapRef, nil
}

func (r *TalosControlPlaneReconciler) updateStatus(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) error {
	clusterSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			clusterv1.ClusterNameLabel:         cluster.Name,
			clusterv1.MachineControlPlaneLabel: "",
		},
	}

	selector, err := metav1.LabelSelectorAsSelector(clusterSelector)
	if err != nil {
		// Since we are building up the LabelSelector above, this should not fail
		return errors.Wrap(err, "failed to parse label selector")
	}
	// Copy label selector to its status counterpart in string format.
	// This is necessary for CRDs including scale subresources.
	tcp.Status.Selector = selector.String()

	ownedMachines, err := r.getControlPlaneMachinesForCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		return err
	}

	nonDeletingMachines := collections.FromMachineList(&ownedMachines).Filter(collections.Not(collections.HasDeletionTimestamp))
	replicas := int32(nonDeletingMachines.Len())

	// Everything up to the workload cluster probe reads only the Machines, so it stays accurate while
	// the workload cluster cannot be reached or the TalosControlPlane is being deleted.
	lowestVersion := nonDeletingMachines.LowestVersion()
	if lowestVersion != "" {
		tcp.Status.Version = &lowestVersion
	}

	deprecated := tcp.V1Beta1DeprecatedStatus()
	deprecated.UpdatedReplicas = 0

	// A control plane being deleted no longer rolls its Machines out, so their UpToDate conditions
	// stay as they are.
	if tcp.DeletionTimestamp.IsZero() {
		controlPlane, err := newControlPlane(ctx, r.Client, cluster, tcp, nonDeletingMachines)
		if err != nil {
			r.Log.Info("failed to compute updated replica status", "error", err)
		} else {
			deprecated.UpdatedReplicas = int32(len(controlPlane.Machines) - len(controlPlane.MachinesWithOutdatedRolloutSpec()))

			// The Machine controller leaves UpToDate unset on Machines a control plane owns, and
			// MachineDeployment rollouts and the Cluster status wait for it.
			if err := controlPlane.reconcileMachineUpToDateConditions(ctx, r.Client); err != nil {
				r.Log.Info("failed to reconcile machine UpToDate conditions", "error", err)
			}
		}
	}

	var availableReplicas, upToDateReplicas, readyReplicas, machinesWithNode int32

	for _, machine := range nonDeletingMachines {
		if machine.Status.NodeRef.IsDefined() {
			machinesWithNode++
		}

		if conditions.IsTrue(machine, clusterv1.MachineAvailableCondition) {
			availableReplicas++
		}

		if conditions.IsTrue(machine, clusterv1.MachineUpToDateCondition) {
			upToDateReplicas++
		}

		if conditions.IsTrue(machine, clusterv1.MachineReadyCondition) {
			readyReplicas++
		}
	}

	tcp.Status.Replicas = ptr.To(replicas)
	tcp.Status.ReadyReplicas = ptr.To(readyReplicas)
	tcp.Status.AvailableReplicas = ptr.To(availableReplicas)
	tcp.Status.UpToDateReplicas = ptr.To(upToDateReplicas)

	deprecated.UnavailableReplicas = replicas - readyReplicas
	deprecated.Ready = readyReplicas > 0

	var workloadErr error

	if tcp.DeletionTimestamp.IsZero() {
		workloadErr = r.probeWorkloadCluster(ctx, tcp, cluster)
		if workloadErr != nil {
			r.Log.Info("failed to reach the workload cluster", "error", workloadErr)
		}
	}

	setAvailableCondition(tcp, machinesWithNode, workloadErr, workloadErr != nil && r.withinRemoteConnectionGracePeriod(ctx, cluster))

	r.Log.Info("ready replicas", "count", readyReplicas)

	return nil
}

// probeWorkloadCluster lists the control plane nodes of the workload cluster and marks the control
// plane initialised once that works. It returns why the workload cluster cannot be reached.
func (r *TalosControlPlaneReconciler) probeWorkloadCluster(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) error {
	c, err := r.ClusterCache.GetClient(ctx, util.ObjectKey(cluster))
	if err != nil {
		return errors.Wrap(err, "failed to get a client for the workload cluster")
	}

	req, err := labels.NewRequirement(constants.LabelNodeRoleControlPlane, selection.Exists, []string{})
	if err != nil {
		return err
	}

	if err := c.List(ctx, &corev1.NodeList{}, &client.ListOptions{LabelSelector: labels.NewSelector().Add(*req)}); err != nil {
		return errors.Wrap(err, "failed to list control plane nodes")
	}

	tcp.V1Beta1DeprecatedStatus().Initialized = true
	tcp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)

	return nil
}

// withinRemoteConnectionGracePeriod reports whether a failure to reach the workload cluster is still
// within the grace period: the cluster cache has not probed often enough yet, as right after a
// controller restart, or its last successful probe is recent. KubeadmControlPlane keeps its remote
// conditions the same way.
func (r *TalosControlPlaneReconciler) withinRemoteConnectionGracePeriod(ctx context.Context, cluster *clusterv1.Cluster) bool {
	state := r.ClusterCache.GetHealthCheckingState(ctx, util.ObjectKey(cluster))

	if state.LastProbeSuccessTime.IsZero() {
		return state.ConsecutiveFailures < 5
	}

	return time.Since(state.LastProbeSuccessTime) <= remoteConnectionGracePeriod
}

// setAvailableCondition is the only place that sets the Available condition.
//
// The control plane is available when it is initialised, the workload cluster can be reached, etcd
// has quorum and at least one control plane machine has a node. Like KubeadmControlPlane's, it does
// not depend on the nodes being Ready: nodes stay NotReady until a CNI is installed while the control
// plane already serves. Fewer ready machines than desired, as during a rollout, show on the
// MachinesReady condition instead.
//
// While the workload cluster cannot be reached and keepOnWorkloadErr is set, the last known state is
// kept, so a controller restart or a short connection loss does not flip the condition.
func setAvailableCondition(tcp *controlplanev1.TalosControlPlane, machinesWithNode int32, workloadErr error, keepOnWorkloadErr bool) {
	condition := metav1.Condition{
		Type:   string(controlplanev1.AvailableCondition),
		Status: metav1.ConditionTrue,
		Reason: controlplanev1.AvailableReason,
	}

	etcdHealthy := conditions.Get(tcp, string(controlplanev1.EtcdClusterHealthyCondition))
	etcdUnhealthy := etcdHealthy != nil && etcdHealthy.Status == metav1.ConditionFalse

	switch {
	case !tcp.DeletionTimestamp.IsZero():
		condition.Status = metav1.ConditionFalse
		condition.Reason = controlplanev1.NotAvailableReason
		condition.Message = "TalosControlPlane is being deleted"
	case !ptr.Deref(tcp.Status.Initialization.ControlPlaneInitialized, false):
		condition.Status = metav1.ConditionFalse
		condition.Reason = controlplanev1.NotAvailableReason
		condition.Message = "Control plane is not yet initialized"
	case workloadErr != nil && keepOnWorkloadErr && conditions.Has(tcp, string(controlplanev1.AvailableCondition)):
		return
	case workloadErr != nil:
		condition.Status = metav1.ConditionUnknown
		condition.Reason = controlplanev1.RemoteConnectionProbeFailedReason
		condition.Message = fmt.Sprintf("Workload cluster can't be reached: %v", workloadErr)
	case etcdUnhealthy && etcdHealthy.Reason == controlplanev1.EtcdClusterQuorumLostReason:
		condition.Status = metav1.ConditionFalse
		condition.Reason = controlplanev1.NotAvailableReason
		condition.Message = "etcd does not have quorum: " + etcdHealthy.Message
	case etcdUnhealthy && etcdHealthy.Reason == controlplanev1.EtcdClusterInspectionFailedReason:
		condition.Status = metav1.ConditionUnknown
		condition.Reason = controlplanev1.AvailableInspectionFailedReason
		condition.Message = "etcd can't be inspected: " + etcdHealthy.Message
	case machinesWithNode < 1:
		condition.Status = metav1.ConditionFalse
		condition.Reason = controlplanev1.NotAvailableReason
		condition.Message = "There are no control plane machines with a node, at least 1 required"
	case etcdUnhealthy:
		condition.Message = "etcd is degraded but has quorum: " + etcdHealthy.Message
	}

	conditions.Set(tcp, condition)
}

func (r *TalosControlPlaneReconciler) reconcileExternalReference(ctx context.Context, ref clusterv1.ContractVersionedObjectReference, cluster *clusterv1.Cluster) error {
	if !strings.HasSuffix(ref.Kind, clusterv1.TemplateSuffix) {
		return nil
	}

	obj, err := external.GetObjectFromContractVersionedRef(ctx, r.Client, ref, cluster.Namespace)
	if err != nil {
		return err
	}

	objPatchHelper, err := patch.NewHelper(obj, r.Client)
	if err != nil {
		return err
	}

	obj.SetOwnerReferences(util.EnsureOwnerRef(obj.GetOwnerReferences(), metav1.OwnerReference{
		APIVersion: clusterv1.GroupVersion.String(),
		Kind:       "Cluster",
		Name:       cluster.Name,
		UID:        cluster.UID,
	}))

	return objPatchHelper.Patch(ctx, obj)
}

func (r *TalosControlPlaneReconciler) reconcileKubeconfig(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (ctrl.Result, error) {
	endpoint := cluster.Spec.ControlPlaneEndpoint
	if endpoint.IsZero() {
		return ctrl.Result{}, nil
	}

	clusterName := util.ObjectKey(cluster)
	existingKubeconfig, err := secret.GetFromNamespacedName(ctx, r.Client, clusterName, secret.Kubeconfig)

	switch {
	case apierrors.IsNotFound(err):
		createErr := kubeconfig.CreateSecretWithOwner(
			ctx,
			r.Client,
			clusterName,
			endpoint.String(),
			*metav1.NewControllerRef(tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane")),
		)
		if createErr != nil {
			if errors.Is(createErr, kubeconfig.ErrDependentCertificateNotFound) {
				r.Log.Info("could not find secret", "secret", secret.ClusterCA, "cluster", clusterName.Name, "namespace", clusterName.Namespace)

				return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
			}

			return ctrl.Result{}, createErr
		}
	case err != nil:
		return ctrl.Result{RequeueAfter: 20 * time.Second}, fmt.Errorf("failed to retrieve kubeconfig Secret for Cluster %q in namespace %q: %w", clusterName.Name, clusterName.Namespace, err)
	default:
		// kubeconfig is already generated
		needsRotation, err := kubeconfig.NeedsClientCertRotation(existingKubeconfig, certs.ClientCertificateRenewalDuration)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to figure out if we need to regenerate cluster client cert: %w", err)
		}

		if !needsRotation {
			return ctrl.Result{}, nil
		}

		r.Log.Info("kubeconfig certificate rotation", "secret", secret.Kubeconfig, "cluster", clusterName.Name, "namespace", clusterName.Namespace)

		err = kubeconfig.RegenerateSecret(ctx, r.Client, existingKubeconfig)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to regenerate kubeconfig: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileEtcdMembers(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (result ctrl.Result, err error) {
	var errs error
	// Audit the etcd member list to remove any nodes that no longer exist
	if err := r.auditEtcd(ctx, tcp, util.ObjectKey(cluster)); err != nil {
		errs = kerrors.NewAggregate([]error{errs, err})
	}

	if health, err := r.etcdHealthcheck(ctx, tcp, machines.Items); err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.EtcdClusterHealthyCondition),
			Status:  metav1.ConditionFalse,
			Reason:  health.conditionReason(),
			Message: fmt.Sprintf("Failed to perform etcd healthcheck: %v", err),
		})
		errs = kerrors.NewAggregate([]error{errs, err})
	} else {
		conditions.Set(tcp, metav1.Condition{
			Type:   string(controlplanev1.EtcdClusterHealthyCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.EtcdClusterHealthyReason,
		})
	}

	if errs != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, errs
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileNodeHealth(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (result ctrl.Result, err error) {
	// a deleting machine is drained and its services stop on purpose, which says nothing of the health
	// of the control plane that stays
	if err := r.nodesHealthcheck(ctx, tcp, remainingMachines(machines.Items)); err != nil {
		reason := controlplanev1.ControlPlaneComponentsInspectionFailedReason

		var unhealthy *errServiceUnhealthy
		if errors.As(err, &unhealthy) {
			reason = controlplanev1.ControlPlaneComponentsUnhealthyReason
		}

		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.ControlPlaneComponentsHealthyCondition),
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: fmt.Sprintf("Failed to perform control plane healthcheck: %v", err),
		})

		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	} else {
		conditions.Set(tcp, metav1.Condition{
			Type:   string(controlplanev1.ControlPlaneComponentsHealthyCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.ControlPlaneComponentsHealthyReason,
		})
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileConditions(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (result ctrl.Result, err error) {
	// status.bootstrapped never goes back to false, so the condition follows it whatever path the
	// reconcile takes, also when the condition was lost, as on a round trip through v1alpha3.
	if tcp.Status.Bootstrapped {
		conditions.Set(tcp, metav1.Condition{
			Type:   string(controlplanev1.MachinesBootstrapped),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.MachinesBootstrappedReason,
		})
	} else if !conditions.Has(tcp, string(controlplanev1.MachinesBootstrapped)) {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesBootstrapped),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.WaitingForMachinesReason,
			Message: "Waiting for machines to bootstrap",
		})
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileMachineTemplateState(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) error {
	desiredLabels := controlPlaneMachineLabelsForCluster(tcp, cluster.Name)
	desiredAnnotations := copyStringMap(tcp.Spec.MachineTemplate.ObjectMeta.Annotations)
	desiredReadinessGates := copyMachineReadinessGates(tcp.Spec.MachineTemplate.Spec.ReadinessGates)
	desiredDeletion := tcp.Spec.MachineTemplate.Spec.Deletion

	for i := range machines.Items {
		machine := &machines.Items[i]
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}

		patchHelper, err := patch.NewHelper(machine, r.Client)
		if err != nil {
			return err
		}

		changed := false

		if machine.Labels == nil {
			machine.Labels = map[string]string{}
		}

		for key, value := range desiredLabels {
			if machine.Labels[key] != value {
				machine.Labels[key] = value
				changed = true
			}
		}

		if len(desiredAnnotations) > 0 {
			if machine.Annotations == nil {
				machine.Annotations = map[string]string{}
			}

			for key, value := range desiredAnnotations {
				if machine.Annotations[key] != value {
					machine.Annotations[key] = value
					changed = true
				}
			}
		}

		if !reflect.DeepEqual(machine.Spec.ReadinessGates, desiredReadinessGates) {
			machine.Spec.ReadinessGates = copyMachineReadinessGates(desiredReadinessGates)
			changed = true
		}

		// Only the timeouts set in the template are enforced: the Machine defaulting webhook sets
		// nodeDeletionTimeoutSeconds when it is empty, so clearing it would patch every Machine on
		// every reconcile.
		for _, timeout := range []struct {
			machine  **int32
			template *int32
		}{
			{&machine.Spec.Deletion.NodeDrainTimeoutSeconds, desiredDeletion.NodeDrainTimeoutSeconds},
			{&machine.Spec.Deletion.NodeVolumeDetachTimeoutSeconds, desiredDeletion.NodeVolumeDetachTimeoutSeconds},
			{&machine.Spec.Deletion.NodeDeletionTimeoutSeconds, desiredDeletion.NodeDeletionTimeoutSeconds},
		} {
			if timeout.template != nil && !ptr.Equal(*timeout.machine, timeout.template) {
				*timeout.machine = copyInt32Pointer(timeout.template)
				changed = true
			}
		}

		if !changed {
			continue
		}

		if err := patchHelper.Patch(ctx, machine); err != nil {
			return err
		}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) reconcileMachines(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (res ctrl.Result, err error) {
	logger := r.Log.WithValues("namespace", tcp.Namespace, "talosControlPlane", tcp.Name)

	// If we've made it this far, we can assume that all ownedMachines are up to date
	numMachines := len(machines.Items)
	desiredReplicas := int(tcp.Spec.GetReplicas())

	controlPlane, err := newControlPlane(ctx, r.Client, cluster, tcp, collections.FromMachineList(machines))
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileMachineTemplateState(ctx, cluster, tcp, machines); err != nil {
		return ctrl.Result{}, err
	}

	needRollout := controlPlane.MachinesNeedingRollout()
	if len(needRollout) > 0 {
		logger.Info("rolling out control plane machines", "needRollout", needRollout.Names())
		conditions.Set(controlPlane.TCP, metav1.Condition{
			Type:    string(controlplanev1.MachinesSpecUpToDateCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.RollingUpdateInProgressReason,
			Message: fmt.Sprintf("Rolling %d replicas with outdated spec (%d replicas up to date)", len(needRollout), len(controlPlane.Machines)-len(needRollout)),
		})

		return r.upgradeControlPlane(ctx, cluster, tcp, controlPlane, needRollout)
	} else {
		if conditions.Has(controlPlane.TCP, string(controlplanev1.MachinesSpecUpToDateCondition)) {
			conditions.Set(controlPlane.TCP, metav1.Condition{
				Type:   string(controlplanev1.MachinesSpecUpToDateCondition),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.MachinesSpecUpToDateReason,
			})
		}
	}

	switch {
	// We are creating the first replica
	case numMachines < desiredReplicas && numMachines == 0:
		// Create new Machine w/ init
		logger.Info("initializing control plane", "Desired", desiredReplicas, "Existing", numMachines)

		return r.bootControlPlane(ctx, cluster, tcp, true)
	// We are scaling up
	case numMachines < desiredReplicas && numMachines > 0:
		return r.scaleUpControlPlane(ctx, cluster, tcp, controlPlane)
	// We are scaling down
	case numMachines > desiredReplicas:
		return r.scaleDownControlPlane(ctx, tcp, controlPlane, collections.Machines{})
	default:
		if !reflect.ValueOf(tcp.Spec.ControlPlaneConfig.InitConfig).IsZero() {
			tcp.Status.Bootstrapped = true
			conditions.Set(tcp, metav1.Condition{
				Type:   string(controlplanev1.MachinesBootstrapped),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.MachinesBootstrappedReason,
			})
		}

		if !tcp.Status.Bootstrapped {
			if err := r.bootstrapCluster(ctx, tcp, machines.Items); err != nil {
				conditions.Set(tcp, metav1.Condition{
					Type:    string(controlplanev1.MachinesBootstrapped),
					Status:  metav1.ConditionFalse,
					Reason:  controlplanev1.WaitingForTalosBootReason,
					Message: fmt.Sprintf("Failed to bootstrap cluster: %v", err),
				})

				logger.Info("bootstrap failed, retrying in 20 seconds", "error", err)

				return ctrl.Result{RequeueAfter: time.Second * 20}, nil
			}

			conditions.Set(tcp, metav1.Condition{
				Type:   string(controlplanev1.MachinesBootstrapped),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.MachinesBootstrappedReason,
			})

			tcp.Status.Bootstrapped = true
		}

		if conditions.Has(tcp, string(controlplanev1.MachinesAllReadyCondition)) {
			conditions.Set(tcp, metav1.Condition{
				Type:   string(controlplanev1.ResizedCondition),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.ResizedReason,
			})
		}

		conditions.Set(tcp, metav1.Condition{
			Type:   string(controlplanev1.MachinesCreatedCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.MachinesCreatedReason,
		})
	}

	return ctrl.Result{}, nil
}

func controlPlaneMachineLabelsForCluster(tcp *controlplanev1.TalosControlPlane, clusterName string) map[string]string {
	labels := copyStringMap(tcp.Spec.MachineTemplate.ObjectMeta.Labels)
	if labels == nil {
		labels = map[string]string{}
	}

	labels[clusterv1.ClusterNameLabel] = clusterName
	labels[clusterv1.MachineControlPlaneLabel] = ""
	labels[clusterv1.MachineControlPlaneNameLabel] = format.MustFormatValue(tcp.Name)

	return labels
}

// desiredMachineAnnotations returns the annotations a new control plane Machine is created with: the
// template's, the pre-terminate hook, and, while a remediation waits for its replacement, the record of
// that remediation, so that the new Machine's own remediation counts as a retry.
func desiredMachineAnnotations(tcp *controlplanev1.TalosControlPlane) map[string]string {
	annotations := copyStringMap(tcp.Spec.MachineTemplate.ObjectMeta.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}

	annotations[PreTerminateHookCleanupAnnotation] = ""

	if value, ok := tcp.Annotations[remediationInProgressAnnotation]; ok {
		annotations[remediationForAnnotation] = value
	}

	return annotations
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}

func copyMachineReadinessGates(in []clusterv1.MachineReadinessGate) []clusterv1.MachineReadinessGate {
	if len(in) == 0 {
		return nil
	}

	out := make([]clusterv1.MachineReadinessGate, len(in))
	copy(out, in)

	return out
}

func buildMachineDeletionSpec(deletion controlplanev1.TalosControlPlaneMachineTemplateDeletionSpec) clusterv1.MachineDeletionSpec {
	return clusterv1.MachineDeletionSpec{
		NodeDrainTimeoutSeconds:        copyInt32Pointer(deletion.NodeDrainTimeoutSeconds),
		NodeVolumeDetachTimeoutSeconds: copyInt32Pointer(deletion.NodeVolumeDetachTimeoutSeconds),
		NodeDeletionTimeoutSeconds:     copyInt32Pointer(deletion.NodeDeletionTimeoutSeconds),
	}
}

func copyInt32Pointer(in *int32) *int32 {
	if in == nil {
		return nil
	}

	out := *in
	return &out
}

func patchTalosControlPlane(ctx context.Context, patchHelper *patch.Helper, tcp *controlplanev1.TalosControlPlane, opts ...patch.Option) error {
	// Always update the readyCondition by summarizing the state of other conditions.
	if err := conditions.SetSummaryCondition(
		tcp,
		tcp,
		clusterv1.ReadyCondition,
		conditions.ForConditionTypes{
			string(controlplanev1.MachinesCreatedCondition),
			string(controlplanev1.ResizedCondition),
			string(controlplanev1.MachinesAllReadyCondition),
			string(controlplanev1.AvailableCondition),
			string(controlplanev1.MachinesBootstrapped),
		},
	); err != nil {
		return errors.Wrap(err, "failed to set summary Ready condition")
	}

	opts = append(opts,
		patch.WithOwnedConditions{Conditions: []string{
			string(controlplanev1.MachinesCreatedCondition),
			string(clusterv1.ReadyCondition),
			string(controlplanev1.ResizedCondition),
			string(controlplanev1.MachinesAllReadyCondition),
			string(controlplanev1.AvailableCondition),
			string(controlplanev1.MachinesBootstrapped),
		}},
	)

	// Patch the object, ignoring conflicts on the conditions owned by this controller.
	return patchHelper.Patch(
		ctx,
		tcp,
		opts...,
	)
}
