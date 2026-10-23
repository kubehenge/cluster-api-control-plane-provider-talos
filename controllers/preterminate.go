// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "github.com/kubehenge/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// PreTerminateHookCleanupAnnotation is the Cluster API pre-terminate hook every control plane Machine
// carries. The Machine controller holds a deleting Machine at it after the node is drained and its
// volumes are detached, and before the infrastructure machine is deleted: the one point where the
// node's etcd member can still leave cleanly. The name follows KubeadmControlPlane's kcp-cleanup hook.
const PreTerminateHookCleanupAnnotation = clusterv1.PreTerminateDeleteHookAnnotationPrefix + "/tcp-cleanup"

// deleteRequeueAfter is how often a deletion that waits for something outside this controller (an
// earlier deletion phase, another hook, etcd quorum) is looked at again.
const deleteRequeueAfter = 15 * time.Second

// Event reasons recorded on control plane Machines at their pre-terminate hook.
const (
	etcdMemberRemovedEvent  = "EtcdMemberRemoved"
	etcdCleanupSkippedEvent = "EtcdCleanupSkipped"
	etcdQuorumAtRiskEvent   = "EtcdQuorumAtRisk"
)

// reconcilePreTerminateHooks puts the pre-terminate hook on the control plane Machines that lack it and
// services it on the Machine being deleted. It runs before every other step of the reconcile: a Machine
// held at the hook keeps its infrastructure, and the gates further down (control plane endpoint, etcd
// and component health) may well be failing exactly when a control plane Machine is on its way out.
func (r *TalosControlPlaneReconciler) reconcilePreTerminateHooks(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (ctrl.Result, error) {
	owned := controlledMachines(tcp, machines)

	// A hook that cannot be added must not hold up a deletion that is already in progress.
	stampErr := r.addPreTerminateHooks(ctx, owned)

	result, err := r.servicePreTerminateHook(ctx, cluster, tcp, owned)

	return result, kerrors.NewAggregate([]error{stampErr, err})
}

// servicePreTerminateHook removes the etcd member of the deleting Machine with the oldest deletion
// timestamp and then releases its hook. etcd takes one membership change at a time, so the other
// deleting Machines wait for a later reconcile.
func (r *TalosControlPlaneReconciler) servicePreTerminateHook(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, owned []*clusterv1.Machine) (ctrl.Result, error) {
	var deleting []*clusterv1.Machine

	for _, machine := range owned {
		if machine.DeletionTimestamp.IsZero() {
			continue
		}

		// A deleting Machine without the hook may already be past the point where its member could
		// leave, so whether that member is still in etcd is unknown. Removing another member before it
		// is gone could leave a three member cluster with one live member of two, as KCP knows.
		if _, hooked := machine.Annotations[PreTerminateHookCleanupAnnotation]; !hooked {
			r.Log.Info("waiting for a deleting control plane machine without the pre-terminate hook to go away", "machine", machine.Name)

			return ctrl.Result{RequeueAfter: deleteRequeueAfter}, nil
		}

		deleting = append(deleting, machine)
	}

	if len(deleting) == 0 {
		return ctrl.Result{}, nil
	}

	slices.SortStableFunc(deleting, func(a, b *clusterv1.Machine) int {
		if !a.DeletionTimestamp.Equal(b.DeletionTimestamp) {
			if a.DeletionTimestamp.Before(b.DeletionTimestamp) {
				return -1
			}

			return 1
		}

		return strings.Compare(a.Name, b.Name)
	})

	victim := deleting[0]

	// The member leaves last, as with KCP: other pre-terminate hooks run while the node's kubelet and
	// API server still work.
	if machineHasOtherPreTerminateHooks(victim) {
		r.Log.Info("waiting for the other pre-terminate hooks of the machine", "machine", victim.Name)

		return ctrl.Result{RequeueAfter: deleteRequeueAfter}, nil
	}

	// Only once the Machine controller waits for the hooks are the node drained and its volumes
	// detached; the member leaving any earlier takes the node's API server down mid-drain.
	if c := conditions.Get(victim, clusterv1.MachineDeletingCondition); c == nil ||
		c.Status != metav1.ConditionTrue ||
		c.Reason != clusterv1.MachineDeletingWaitingForPreTerminateHookReason {
		return ctrl.Result{RequeueAfter: deleteRequeueAfter}, nil
	}

	switch {
	case !cluster.DeletionTimestamp.IsZero():
		return r.releasePreTerminateHookWithEvent(ctx, victim, corev1.EventTypeNormal, etcdCleanupSkippedEvent,
			"The cluster is being deleted, the etcd member is not removed")
	case !tcp.DeletionTimestamp.IsZero():
		return r.releasePreTerminateHookWithEvent(ctx, victim, corev1.EventTypeNormal, etcdCleanupSkippedEvent,
			"The control plane is being deleted, the etcd member is not removed")
	case len(owned) <= 1:
		return r.releasePreTerminateHookWithEvent(ctx, victim, corev1.EventTypeNormal, etcdCleanupSkippedEvent,
			"Last control plane machine, the etcd member is not removed")
	case len(nodeNamesForMachine(*victim)) == 0:
		// A machine that never reported a host name cannot be matched to a member; if it joined etcd
		// after all, auditEtcd removes the member as an orphan once the Machine is gone.
		return r.releasePreTerminateHookWithEvent(ctx, victim, corev1.EventTypeNormal, etcdCleanupSkippedEvent,
			"The machine has no host name to match an etcd member, any member it has is removed as an orphan")
	}

	// Unlike KCP's, this never gives up and releases on its own: a Machine held at the hook is visible
	// and costs nothing, a member left behind or a removal that costs quorum can cost the cluster.
	check, err := r.checkEtcdMemberRemoval(ctx, tcp, victim, owned)
	if err != nil {
		return ctrl.Result{RequeueAfter: deleteRequeueAfter}, fmt.Errorf("failed to inspect etcd before removing the member of machine %q: %w", victim.Name, err)
	}

	if check.member == nil {
		return r.releasePreTerminateHookWithEvent(ctx, victim, corev1.EventTypeNormal, etcdCleanupSkippedEvent,
			"The machine is not an etcd member")
	}

	if !check.safe {
		message := fmt.Sprintf("Removing etcd member %q could cost etcd its quorum, the deletion waits: %s", check.member.Hostname, check.details)

		r.Log.Info(message, "machine", victim.Name)
		r.recordEvent(victim, corev1.EventTypeWarning, etcdQuorumAtRiskEvent, "RemoveEtcdMember", message)

		return ctrl.Result{RequeueAfter: deleteRequeueAfter}, nil
	}

	if err := r.removeEtcdMember(ctx, tcp, victim, check); err != nil {
		return ctrl.Result{RequeueAfter: deleteRequeueAfter}, err
	}

	result, err := r.releasePreTerminateHookWithEvent(ctx, victim, corev1.EventTypeNormal, etcdMemberRemovedEvent,
		fmt.Sprintf("etcd member %q left the cluster", check.member.Hostname))
	if err != nil {
		return result, err
	}

	if len(deleting) > 1 {
		result.RequeueAfter = deleteRequeueAfter
	}

	return result, nil
}

// releasePreTerminateHookWithEvent releases the hook and records why on the Machine.
func (r *TalosControlPlaneReconciler) releasePreTerminateHookWithEvent(ctx context.Context, machine *clusterv1.Machine, eventType, reason, message string) (ctrl.Result, error) {
	if err := r.releasePreTerminateHook(ctx, machine); err != nil {
		return ctrl.Result{}, err
	}

	r.Log.Info(message, "machine", machine.Name)
	r.recordEvent(machine, eventType, reason, "ReleasePreTerminateHook", message)

	return ctrl.Result{}, nil
}

// releasePreTerminateHook removes the hook, letting the Machine controller delete the infrastructure.
func (r *TalosControlPlaneReconciler) releasePreTerminateHook(ctx context.Context, machine *clusterv1.Machine) error {
	if _, hooked := machine.Annotations[PreTerminateHookCleanupAnnotation]; !hooked {
		return nil
	}

	patchHelper, err := patch.NewHelper(machine, r.Client)
	if err != nil {
		return err
	}

	delete(machine.Annotations, PreTerminateHookCleanupAnnotation)

	if err := patchHelper.Patch(ctx, machine); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to release the pre-terminate hook of machine %q: %w", machine.Name, err)
	}

	return nil
}

// addPreTerminateHooks puts the hook on the owned Machines that lack it. Deleting Machines are left
// alone: one may already be past its pre-terminate phase, and servicePreTerminateHook waits for it.
func (r *TalosControlPlaneReconciler) addPreTerminateHooks(ctx context.Context, owned []*clusterv1.Machine) error {
	var errs []error

	for _, machine := range owned {
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}

		if _, hooked := machine.Annotations[PreTerminateHookCleanupAnnotation]; hooked {
			continue
		}

		patchHelper, err := patch.NewHelper(machine, r.Client)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		if machine.Annotations == nil {
			machine.Annotations = map[string]string{}
		}

		machine.Annotations[PreTerminateHookCleanupAnnotation] = ""

		if err := patchHelper.Patch(ctx, machine); err != nil {
			errs = append(errs, fmt.Errorf("failed to add the pre-terminate hook to machine %q: %w", machine.Name, err))
		}
	}

	return kerrors.NewAggregate(errs)
}

// machineHasOtherPreTerminateHooks reports whether the Machine carries a pre-terminate hook other than
// this controller's.
func machineHasOtherPreTerminateHooks(machine *clusterv1.Machine) bool {
	for key := range machine.Annotations {
		if strings.HasPrefix(key, clusterv1.PreTerminateDeleteHookAnnotationPrefix) && key != PreTerminateHookCleanupAnnotation {
			return true
		}
	}

	return false
}

// controlledMachines returns the machines the TalosControlPlane is the controller of, by name.
func controlledMachines(tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) []*clusterv1.Machine {
	owned := make([]*clusterv1.Machine, 0, len(machines.Items))

	for i := range machines.Items {
		if metav1.IsControlledBy(&machines.Items[i], tcp) {
			owned = append(owned, &machines.Items[i])
		}
	}

	slices.SortFunc(owned, func(a, b *clusterv1.Machine) int { return strings.Compare(a.Name, b.Name) })

	return owned
}

// recordEvent records an event about the object when the reconciler has a recorder.
func (r *TalosControlPlaneReconciler) recordEvent(object runtime.Object, eventType, reason, action, message string) {
	if r.Recorder == nil {
		return
	}

	r.Recorder.Eventf(object, nil, eventType, reason, action, "%s", message)
}
