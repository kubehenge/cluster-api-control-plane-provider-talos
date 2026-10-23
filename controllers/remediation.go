// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

const (
	// remediationInProgressAnnotation is set on the TalosControlPlane before an unhealthy Machine is
	// deleted, until a replacement Machine is Ready or remediationInProgressTimeout expired. Its value is
	// a JSON encoded remediationData. While it is set, no other Machine is remediated and the control
	// plane is not scaled down.
	remediationInProgressAnnotation = "controlplane.cluster.x-k8s.io/remediation-in-progress"

	// remediationForAnnotation is set on the Machine that replaced a remediated Machine. Its value is the
	// remediationData of that remediation, which counts the retries if the replacement is unhealthy too.
	remediationForAnnotation = "controlplane.cluster.x-k8s.io/remediation-for"
)

// Remediation limits, KubeadmControlPlane's defaults.
const (
	// remediationInProgressTimeout is how long a remediation blocks the next one when no replacement
	// becomes Ready.
	remediationInProgressTimeout = 30 * time.Minute

	// remediationStartTimeout is how long after a remediation was recorded the remediated Machine may
	// still be seen not deleting before the remediation is taken as never started: the controller
	// stopped between recording it and deleting the Machine, or the cached Machines lag behind.
	remediationStartTimeout = time.Minute

	// remediationMinHealthyPeriod is how long a replacement must stay healthy for its own remediation
	// to start a new retry sequence instead of counting as a retry of the previous one.
	remediationMinHealthyPeriod = time.Hour

	// remediationRetryPeriod is the minimum time between two remediations of the same retry sequence.
	remediationRetryPeriod = 5 * time.Minute

	// remediationMaxRetry is the maximum number of retries of a remediation sequence.
	remediationMaxRetry = 3
)

// remediationData is stored in remediationInProgressAnnotation and remediationForAnnotation.
type remediationData struct {
	// Machine is the name of the remediated Machine.
	Machine string `json:"machine"`

	// Timestamp is when the remediation happened.
	Timestamp metav1.Time `json:"timestamp"`

	// RetryCount is the number of retries of the remediation sequence: a retry happens when a Machine
	// that replaced a remediated Machine is remediated too, within remediationMinHealthyPeriod.
	RetryCount int `json:"retryCount"`
}

func remediationDataFromAnnotation(value string) (*remediationData, error) {
	data := &remediationData{}
	if err := json.Unmarshal([]byte(value), data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal remediation data %q: %w", value, err)
	}

	return data, nil
}

func (d *remediationData) marshal() (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", fmt.Errorf("failed to marshal remediation data: %w", err)
	}

	return string(b), nil
}

// remediationInProgress reports whether a remediation is waiting for its replacement Machine.
func remediationInProgress(tcp *controlplanev1.TalosControlPlane) bool {
	_, ok := tcp.Annotations[remediationInProgressAnnotation]

	return ok
}

// reconcileRemediationAndMachines runs the two steps that delete control plane Machines: remediation,
// then scaling and rollout. When remediation deleted a Machine, or tried to, scaling and rollout wait
// for the next reconcile, so that one reconcile never deletes two Machines.
func (r *TalosControlPlaneReconciler) reconcileRemediationAndMachines(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (ctrl.Result, error) {
	result, remediating, err := r.reconcileUnhealthyMachines(ctx, tcp, cluster, machines.Items)
	if remediating {
		return result, err
	}

	machinesResult, machinesErr := r.reconcileMachines(ctx, cluster, tcp, machines)

	return util.LowestNonZeroResult(result, machinesResult), kerrors.NewAggregate([]error{err, machinesErr})
}

// reconcileUnhealthyMachines remediates the control plane Machines that a MachineHealthCheck marked as
// unhealthy (HealthCheckSucceeded=False) and left to their owner (OwnerRemediated=False) by deleting
// them; the pre-terminate hook removes the etcd member, and the regular scale-up creates the
// replacement. It is modelled on KubeadmControlPlane's remediation.
//
// A Machine is only remediated when:
//   - the control plane is initialised and has more than one Machine,
//   - no other remediation is in progress and the retry limits allow it,
//   - no other control plane Machine is being deleted or still provisioning,
//   - removing its etcd member keeps etcd quorum, given the current members and their health.
//
// Otherwise the reason is surfaced in the OwnerRemediated condition of the Machine. remediating reports
// whether remediation deleted a Machine in this reconcile, or tried to.
func (r *TalosControlPlaneReconciler) reconcileUnhealthyMachines(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	cluster *clusterv1.Cluster,
	machineList []clusterv1.Machine,
) (_ ctrl.Result, remediating bool, retErr error) {
	log := r.Log.WithValues("TalosControlPlane", client.ObjectKeyFromObject(tcp).String(), "Cluster", cluster.Name)
	now := time.Now().UTC()

	machines := make([]*clusterv1.Machine, 0, len(machineList))
	for i := range machineList {
		machines = append(machines, &machineList[i])
	}

	if err := r.reconcileRemediationInProgress(ctx, tcp, machines, now, log); err != nil {
		return ctrl.Result{}, false, err
	}

	// The MachineHealthCheck leaves OwnerRemediated to the owner once set: clear it on Machines that
	// are healthy again, so that a Machine that recovered while its remediation waited is not deleted.
	if err := r.cleanupOwnerRemediated(ctx, machines); err != nil {
		return ctrl.Result{}, false, err
	}

	target := machineToBeRemediated(machines)
	if target == nil || !target.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, false, nil
	}

	log = log.WithValues("machine", target.Name)

	patchHelper, err := patch.NewHelper(target, r.Client)
	if err != nil {
		return ctrl.Result{}, false, err
	}

	defer func() {
		if err := patchHelper.Patch(ctx, target, patch.WithOwnedConditions{Conditions: []string{
			clusterv1.MachineOwnerRemediatedCondition,
		}}); err != nil && !apierrors.IsNotFound(err) {
			retErr = kerrors.NewAggregate([]error{retErr, fmt.Errorf("failed to patch control plane machine %s: %w", target.Name, err)})
		}
	}()

	setOwnerRemediated := func(reason, message string) {
		log.Info("a control plane machine needs remediation: "+message, "reason", reason)

		conditions.Set(target, metav1.Condition{
			Type:    clusterv1.MachineOwnerRemediatedCondition,
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: message,
		})
	}

	if remediationInProgress(tcp) {
		setOwnerRemediated(clusterv1.MachineSetMachineRemediationDeferredReason,
			"TalosControlPlane waiting for the previous remediation to complete before triggering remediation")

		return ctrl.Result{}, false, nil
	}

	data, reason, message := checkRemediationRetryLimits(target, now, log)
	if reason != "" {
		setOwnerRemediated(reason, message)

		return ctrl.Result{}, false, nil
	}

	if !ptr.Deref(tcp.Status.Initialization.ControlPlaneInitialized, false) {
		setOwnerRemediated(clusterv1.MachineSetMachineRemediationDeferredReason,
			"TalosControlPlane can't remediate before the control plane is initialized")

		return ctrl.Result{}, false, nil
	}

	if len(machines) <= 1 {
		setOwnerRemediated(clusterv1.MachineSetMachineCannotBeRemediatedReason,
			"TalosControlPlane can't remediate if current replicas are less or equal to 1")

		return ctrl.Result{}, false, nil
	}

	for _, m := range machines {
		if m.Name == target.Name {
			continue
		}

		if !m.DeletionTimestamp.IsZero() {
			setOwnerRemediated(clusterv1.MachineSetMachineRemediationDeferredReason,
				fmt.Sprintf("TalosControlPlane waiting for the deletion of control plane machine %s to complete before triggering remediation", m.Name))

			return ctrl.Result{}, false, nil
		}

		if !collections.IsUnhealthy(m) && !m.Status.NodeRef.IsDefined() {
			setOwnerRemediated(clusterv1.MachineSetMachineRemediationDeferredReason,
				fmt.Sprintf("TalosControlPlane waiting for the provisioning of control plane machine %s to complete before triggering remediation", m.Name))

			return ctrl.Result{}, false, nil
		}
	}

	check, err := r.checkEtcdMemberRemoval(ctx, tcp, target, machines)
	if err != nil {
		log.Info("failed to check etcd cluster health before remediation", "error", err)
		setOwnerRemediated(clusterv1.MachineSetMachineRemediationDeferredReason, "Failed to check etcd cluster health before remediation")

		return ctrl.Result{}, false, nil
	}

	if !check.safe {
		setOwnerRemediated(clusterv1.MachineSetMachineCannotBeRemediatedReason,
			"TalosControlPlane can't remediate this machine because this could result in etcd quorum loss: "+check.details)

		return ctrl.Result{}, false, nil
	}

	value, err := data.marshal()
	if err != nil {
		return ctrl.Result{}, false, err
	}

	// Recorded before the Machine is deleted rather than in the deferred patch of the TalosControlPlane:
	// if the controller stopped, or that patch failed, after the deletion, the remediation would be lost,
	// and with it the retry count and what keeps other Machines from being deleted before the
	// replacement is up.
	if err := r.patchRemediationInProgress(ctx, tcp, value); err != nil {
		setOwnerRemediated(clusterv1.InternalErrorReason, "Failed to record the remediation, please check controller logs for errors")

		return ctrl.Result{}, true, err
	}

	if err := r.Client.Delete(ctx, target); err != nil && !apierrors.IsNotFound(err) {
		setOwnerRemediated(clusterv1.InternalErrorReason, "Failed to delete the machine, please check controller logs for errors")

		// The Machine stays, so the remediation did not happen.
		if clearErr := r.patchRemediationInProgress(ctx, tcp, ""); clearErr != nil {
			err = kerrors.NewAggregate([]error{err, clearErr})
		}

		return ctrl.Result{}, true, fmt.Errorf("failed to delete unhealthy machine %s: %w", target.Name, err)
	}

	log.Info("deleted unhealthy control plane machine, a replacement will be created", "retryCount", data.RetryCount)

	conditions.Set(target, metav1.Condition{
		Type:    clusterv1.MachineOwnerRemediatedCondition,
		Status:  metav1.ConditionFalse,
		Reason:  clusterv1.MachineSetMachineRemediationMachineDeletingReason,
		Message: "Machine is deleting",
	})

	return ctrl.Result{RequeueAfter: 10 * time.Second}, true, nil
}

// patchRemediationInProgress sets the remediation-in-progress annotation of the TalosControlPlane to
// value, or removes it if value is empty, and patches only that right away; every other change made in
// this reconcile is left to the deferred patch.
func (r *TalosControlPlaneReconciler) patchRemediationInProgress(ctx context.Context, tcp *controlplanev1.TalosControlPlane, value string) error {
	before := tcp.DeepCopy()
	after := tcp.DeepCopy()

	if value != "" {
		annotations.AddAnnotations(after, map[string]string{remediationInProgressAnnotation: value})
	} else {
		delete(after.Annotations, remediationInProgressAnnotation)
	}

	if err := r.Client.Patch(ctx, after, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("failed to patch the remediation-in-progress annotation: %w", err)
	}

	tcp.Annotations = after.Annotations

	return nil
}

// cleanupOwnerRemediated removes the OwnerRemediated condition from Machines whose MachineHealthCheck
// reports them as healthy again.
func (r *TalosControlPlaneReconciler) cleanupOwnerRemediated(ctx context.Context, machines []*clusterv1.Machine) error {
	var errs []error

	for _, m := range machines {
		if !m.DeletionTimestamp.IsZero() ||
			!conditions.IsTrue(m, clusterv1.MachineHealthCheckSucceededCondition) ||
			!conditions.IsFalse(m, clusterv1.MachineOwnerRemediatedCondition) {
			continue
		}

		patchHelper, err := patch.NewHelper(m, r.Client)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		conditions.Delete(m, clusterv1.MachineOwnerRemediatedCondition)

		if err := patchHelper.Patch(ctx, m, patch.WithOwnedConditions{Conditions: []string{
			clusterv1.MachineOwnerRemediatedCondition,
		}}); err != nil {
			errs = append(errs, err)
		}
	}

	return kerrors.NewAggregate(errs)
}

// reconcileRemediationInProgress removes the remediation-in-progress annotation once the remediation
// completed: a Machine created after it is Ready, or the remediated Machine is gone and enough Machines
// are Ready, or remediationInProgressTimeout expired after the remediated Machine is gone or replaced,
// so that a replacement that never becomes Ready cannot block remediation forever. The remediation data
// moves to the replacement Machine, where it counts the retries; Machines created while the remediation
// is in progress already got it at creation.
//
// The annotation is also removed when the remediation never started: the remediated Machine is still
// not deleting remediationStartTimeout after the remediation was recorded.
func (r *TalosControlPlaneReconciler) reconcileRemediationInProgress(
	ctx context.Context,
	tcp *controlplanev1.TalosControlPlane,
	machines []*clusterv1.Machine,
	now time.Time,
	log logr.Logger,
) error {
	value, ok := tcp.Annotations[remediationInProgressAnnotation]
	if !ok {
		return nil
	}

	data, err := remediationDataFromAnnotation(value)
	if err != nil {
		log.Info("removing invalid remediation-in-progress annotation", "error", err)
		delete(tcp.Annotations, remediationInProgressAnnotation)

		return nil
	}

	var (
		replacement   *clusterv1.Machine
		remediated    *clusterv1.Machine
		readyMachines int32
	)

	for _, m := range machines {
		if m.Name == data.Machine {
			remediated = m

			continue
		}

		if !m.DeletionTimestamp.IsZero() {
			continue
		}

		if conditions.IsTrue(m, clusterv1.ReadyCondition) {
			readyMachines++
		}

		// The oldest Machine created after the remediation is its replacement.
		if m.CreationTimestamp.After(data.Timestamp.Time) &&
			(replacement == nil || m.CreationTimestamp.Before(&replacement.CreationTimestamp)) {
			replacement = m
		}
	}

	if remediated != nil && remediated.DeletionTimestamp.IsZero() && now.After(data.Timestamp.Add(remediationStartTimeout)) {
		log.Info("removing the remediation-in-progress annotation, the remediated machine was never deleted", "remediatedMachine", data.Machine)
		delete(tcp.Annotations, remediationInProgressAnnotation)

		return nil
	}

	switch {
	case replacement != nil && conditions.IsTrue(replacement, clusterv1.ReadyCondition):
		log.Info("remediation completed, the replacement machine is ready", "remediatedMachine", data.Machine, "replacement", replacement.Name)
	case remediated == nil && readyMachines >= tcp.Spec.GetReplicas():
		log.Info("remediation completed, the control plane has enough ready machines", "remediatedMachine", data.Machine, "readyMachines", readyMachines)
	// Without a replacement the remediation data would have nowhere to go, so the annotation stays while
	// the remediated Machine exists; other remediations wait for its deletion anyway.
	case now.After(data.Timestamp.Add(remediationInProgressTimeout)) && (replacement != nil || remediated == nil):
		log.Info("remediation timed out waiting for a ready replacement machine", "remediatedMachine", data.Machine, "timeout", remediationInProgressTimeout)
	default:
		return nil
	}

	if replacement != nil && replacement.Annotations[remediationForAnnotation] == "" {
		patchHelper, err := patch.NewHelper(replacement, r.Client)
		if err != nil {
			return err
		}

		annotations.AddAnnotations(replacement, map[string]string{remediationForAnnotation: value})

		if err := patchHelper.Patch(ctx, replacement); err != nil {
			return fmt.Errorf("failed to record the remediation on replacement machine %s: %w", replacement.Name, err)
		}
	}

	delete(tcp.Annotations, remediationInProgressAnnotation)

	return nil
}

// machineToBeRemediated returns the Machine to remediate first among the Machines a MachineHealthCheck
// marked for remediation by their owner: one with the remediate-machine annotation, then one that
// failed to provision (no noderef), then the oldest.
func machineToBeRemediated(machines []*clusterv1.Machine) *clusterv1.Machine {
	var candidates []*clusterv1.Machine

	for _, m := range machines {
		if needsRemediation(m) {
			candidates = append(candidates, m)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	slices.SortStableFunc(candidates, func(a, b *clusterv1.Machine) int {
		if annotations.HasRemediateMachine(a) != annotations.HasRemediateMachine(b) {
			if annotations.HasRemediateMachine(a) {
				return -1
			}

			return 1
		}

		if a.Status.NodeRef.IsDefined() != b.Status.NodeRef.IsDefined() {
			if !a.Status.NodeRef.IsDefined() {
				return -1
			}

			return 1
		}

		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			if a.CreationTimestamp.Before(&b.CreationTimestamp) {
				return -1
			}

			return 1
		}

		return strings.Compare(a.Name, b.Name)
	})

	return candidates[0]
}

// checkRemediationRetryLimits returns the remediation data to record for the target, or the reason and
// message why the retry limits do not allow remediating it now.
//
// Remediating a Machine that replaced a Machine remediated less than remediationMinHealthyPeriod ago is a
// retry of that remediation: it waits for remediationRetryPeriod after the previous one, and stops after
// remediationMaxRetry retries, so that a Machine that always fails (a quota, a broken template) is not
// replaced forever.
func checkRemediationRetryLimits(target *clusterv1.Machine, now time.Time, log logr.Logger) (data *remediationData, reason, message string) {
	data = &remediationData{
		Machine:   target.Name,
		Timestamp: metav1.Time{Time: now},
	}

	value, ok := target.Annotations[remediationForAnnotation]
	if !ok {
		return data, "", ""
	}

	last, err := remediationDataFromAnnotation(value)
	if err != nil {
		log.Info("ignoring invalid remediation-for annotation", "error", err)

		return data, "", ""
	}

	if !last.Timestamp.Add(remediationMinHealthyPeriod).After(now) {
		return data, "", ""
	}

	data.RetryCount = last.RetryCount

	if last.Timestamp.Add(remediationRetryPeriod).After(now) {
		return data, clusterv1.MachineSetMachineRemediationDeferredReason,
			fmt.Sprintf("TalosControlPlane can't remediate this machine because the operation already failed in the latest %s", remediationRetryPeriod)
	}

	if data.RetryCount >= remediationMaxRetry {
		return data, clusterv1.MachineSetMachineCannotBeRemediatedReason,
			fmt.Sprintf("TalosControlPlane can't remediate this machine because the operation already failed %d times", remediationMaxRetry)
	}

	data.RetryCount++

	return data, "", ""
}
