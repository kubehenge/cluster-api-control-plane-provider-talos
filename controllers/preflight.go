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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "github.com/kubehenge/cluster-api-control-plane-provider-talos/api/v1beta1"
)

const (
	// preflightFailedRequeueAfter is how long a scale operation waits before the checks run again.
	preflightFailedRequeueAfter = 15 * time.Second

	// controlPlaneUnhealthyEvent is recorded on the TalosControlPlane while a scale operation waits
	// for the preflight checks, named as KubeadmControlPlane names it.
	controlPlaneUnhealthyEvent = "ControlPlaneUnhealthy"
)

// preflightChecks reports why the control plane is not ready for a scale operation, or "" when it is,
// as KubeadmControlPlane's preflight checks do: no machine is being deleted, and every machine has a
// node, healthy control plane services and a healthy etcd member that is not a learner. The machines
// in excludeFor (the one a scale-down is about to delete) are not checked.
//
// While a remediation waits for its replacement, a scale-up only needs the machines that are not
// flagged for remediation to be healthy and enough of them for etcd quorum: the replacement is what
// restores the redundancy the flagged machines cost.
func (r *TalosControlPlaneReconciler) preflightChecks(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines collections.Machines, isScaleUp bool, excludeFor ...*clusterv1.Machine) string {
	if machines.Len() == 0 {
		return ""
	}

	if deleting := machines.Filter(collections.HasDeletionTimestamp); deleting.Len() > 0 {
		names := deleting.Names()
		slices.Sort(names)

		return "waiting for machines to be deleted: " + strings.Join(names, ", ")
	}

	whileRemediating := isScaleUp && remediationInProgress(tcp)

	var (
		failures  []string
		checkable []*clusterv1.Machine
	)

	for _, machine := range machines.SortedByCreationTimestamp() {
		if slices.ContainsFunc(excludeFor, func(m *clusterv1.Machine) bool { return m.Name == machine.Name }) {
			continue
		}

		if whileRemediating && needsRemediation(machine) {
			continue
		}

		if !machine.Status.NodeRef.IsDefined() {
			failures = append(failures, fmt.Sprintf("machine %q does not have a corresponding Node yet", machine.Name))

			continue
		}

		checkable = append(checkable, machine)
	}

	checked := make([]clusterv1.Machine, 0, len(checkable))
	for _, machine := range checkable {
		checked = append(checked, *machine)
	}

	if err := r.nodesHealthcheck(ctx, tcp, checked); err != nil {
		failures = append(failures, fmt.Sprintf("control plane components are not healthy: %v", err))
	}

	statuses := r.probeEtcdMachines(ctx, tcp, checkable)

	var (
		healthy int
		voting  int
	)

	for _, machine := range checkable {
		status := statuses[machine.Name]
		if !status.healthy() {
			failures = append(failures, fmt.Sprintf("etcd is not healthy on machine %q: %v", machine.Name, status.err()))

			continue
		}

		healthy++

		members := 0

		for _, member := range status.members {
			if member.IsLearner {
				failures = append(failures, fmt.Sprintf("etcd member %q is a learner that has not been promoted yet", member.Hostname))

				continue
			}

			members++
		}

		voting = max(voting, members)
	}

	if whileRemediating && healthy < voting/2+1 {
		failures = append(failures, fmt.Sprintf("%d of %d etcd members are healthy, short of quorum", healthy, voting))
	}

	return strings.Join(slices.Compact(failures), "; ")
}

// holdScaleOperation records why a scale operation waits, on the Resized condition, in the log and as an
// event on the TalosControlPlane, and returns the result that retries it.
func (r *TalosControlPlaneReconciler) holdScaleOperation(tcp *controlplanev1.TalosControlPlane, reason, summary, failure string) ctrl.Result {
	conditions.Set(tcp, metav1.Condition{
		Type:    string(controlplanev1.ResizedCondition),
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: summary + ": " + failure,
	})

	r.Log.Info("waiting for the control plane to pass the preflight checks", "operation", reason, "failures", failure)
	r.recordEvent(tcp, corev1.EventTypeWarning, controlPlaneUnhealthyEvent, reason,
		"Waiting for the control plane to pass the preflight checks to continue reconciliation: "+failure)

	return ctrl.Result{RequeueAfter: preflightFailedRequeueAfter}
}
