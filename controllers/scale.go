// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func (r *TalosControlPlaneReconciler) scaleUpControlPlane(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, controlPlane *ControlPlane) (ctrl.Result, error) {
	numMachines := len(controlPlane.Machines)
	desiredReplicas := tcp.Spec.GetReplicas()
	summary := fmt.Sprintf("Scaling up control plane to %d replicas (actual %d)", desiredReplicas, numMachines)

	// A new machine joins an etcd cluster that has to be able to take it, so machines are added one at a
	// time, each once the previous one has a node and the control plane is healthy.
	if failure := r.preflightChecks(ctx, tcp, controlPlane.Machines, true); failure != "" {
		return r.holdScaleOperation(tcp, controlplanev1.ScalingUpReason, summary, failure), nil
	}

	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.ResizedCondition),
		Status:  metav1.ConditionFalse,
		Reason:  controlplanev1.ScalingUpReason,
		Message: summary,
	})

	r.Log.Info("scaling up control plane", "Desired", desiredReplicas, "Existing", numMachines)

	return r.bootControlPlane(ctx, cluster, tcp, false)
}

// scaleDownControlPlane deletes one control plane machine, and nothing else: as on `kubectl delete
// machine`, the Machine controller drains the node, the pre-terminate hook removes the etcd member
// (reconcilePreTerminateHooks), and the Machine controller deletes the node once the infrastructure is
// gone.
func (r *TalosControlPlaneReconciler) scaleDownControlPlane(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	controlPlane *ControlPlane,
	machinesRequireUpgrade collections.Machines,
) (ctrl.Result, error) {
	numMachines := len(controlPlane.Machines)
	desiredReplicas := tcp.Spec.GetReplicas()
	summary := fmt.Sprintf("Scaling down control plane to %d replicas (actual %d)", desiredReplicas, numMachines)

	setResized := func(message string) {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.ResizedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.ScalingDownReason,
			Message: message,
		})
	}

	setResized(summary)

	switch numMachines {
	case 0:
		return ctrl.Result{}, fmt.Errorf("no machines found")
	case 1:
		setResized("Cannot scale down control plane nodes to 0")

		return ctrl.Result{}, nil
	}

	// Remediation takes out one machine at a time: no other is deleted before its replacement is up.
	if remediationInProgress(tcp) {
		setResized("Waiting for the remediation of an unhealthy machine to complete before scaling down")

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	victim := selectMachineForScaleDown(controlPlane, machinesRequireUpgrade)

	// The victim is not checked: it may be exactly the broken machine a rollout replaces. Everything
	// else has to be healthy before one more member is taken away.
	if failure := r.preflightChecks(ctx, tcp, controlPlane.Machines, false, victim); failure != "" {
		return r.holdScaleOperation(tcp, controlplanev1.ScalingDownReason, summary, failure), nil
	}

	remaining := make([]clusterv1.Machine, 0, numMachines-1)

	for _, machine := range controlPlane.Machines {
		if machine.Name != victim.Name {
			remaining = append(remaining, *machine)
		}
	}

	if err := r.ensureNodesBooted(ctx, controlPlane.TCP, remaining); err != nil {
		r.Log.Info("waiting for all nodes to finish boot sequence", "error", err)

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	r.Log.Info("scaling down control plane", "Desired", desiredReplicas, "Existing", numMachines, "machine", victim.Name)

	if err := r.Client.Delete(ctx, victim); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("failed to delete control plane machine %q: %w", victim.Name, err)
	}

	return ctrl.Result{RequeueAfter: deleteRequeueAfter}, nil
}

// selectMachineForScaleDown picks the machine to remove: one the user annotated for deletion first,
// then one a MachineHealthCheck flagged for remediation (an outdated one first), then an outdated one,
// and the oldest machine otherwise.
func selectMachineForScaleDown(controlPlane *ControlPlane, outdatedMachines collections.Machines) *clusterv1.Machine {
	machines := controlPlane.Machines
	switch {
	case controlPlane.MachineWithDeleteAnnotation(outdatedMachines).Len() > 0:
		machines = controlPlane.MachineWithDeleteAnnotation(outdatedMachines)
	case controlPlane.MachineWithDeleteAnnotation(machines).Len() > 0:
		machines = controlPlane.MachineWithDeleteAnnotation(machines)
	case outdatedMachines.Filter(needsRemediation).Len() > 0:
		machines = outdatedMachines.Filter(needsRemediation)
	case controlPlane.UnhealthyMachines().Len() > 0:
		machines = controlPlane.UnhealthyMachines()
	case outdatedMachines.Len() > 0:
		machines = outdatedMachines
	}

	return machines.Oldest()
}
