// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"testing"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func upgrade(t *testing.T, strategy *controlplanev1.RolloutStrategy, replicas int32, names ...string) (*lifecycleEnv, ctrl.Result, error) {
	t.Helper()

	tcp := newLifecycleTCP(replicas)
	tcp.Spec.RolloutStrategy = strategy

	var machines []*clusterv1.Machine

	for _, name := range names {
		var opts []machineOption
		if name == "deleted" {
			opts = append(opts, waitingForHook(epoch))
		}

		machines = append(machines, newLifecycleMachine(tcp, name, epoch, opts...))
	}

	env := newLifecycleEnv(t, newFakeMachineAPI(names...), tcp, machines...)
	all := collections.FromMachines(machines...)
	controlPlane := &ControlPlane{TCP: tcp, Cluster: env.cluster, Machines: all}

	result, err := env.r.upgradeControlPlane(env.ctx, env.cluster, tcp, controlPlane, all)

	return env, result, err
}

func TestOnDeleteReplacesADeletedMachine(t *testing.T) {
	env, result, err := upgrade(t, &controlplanev1.RolloutStrategy{Type: controlplanev1.OnDeleteStrategyType}, 3, "a", "deleted")
	if err != nil {
		t.Fatal(err)
	}

	// The scale-up waits for the deleted machine to go before it adds the replacement.
	resized := conditions.Get(env.tcp, string(controlplanev1.ResizedCondition))
	if resized == nil || resized.Reason != controlplanev1.ScalingUpReason {
		t.Fatalf("Resized = %+v, want reason %s", resized, controlplanev1.ScalingUpReason)
	}

	if result.RequeueAfter != preflightFailedRequeueAfter {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, preflightFailedRequeueAfter)
	}
}

func TestOnDeleteLeavesOutdatedMachinesAlone(t *testing.T) {
	env, result, err := upgrade(t, &controlplanev1.RolloutStrategy{Type: controlplanev1.OnDeleteStrategyType}, 3, "a", "b", "c")
	if err != nil {
		t.Fatal(err)
	}

	if !result.IsZero() {
		t.Fatalf("result = %+v, want none", result)
	}

	if resized := conditions.Get(env.tcp, string(controlplanev1.ResizedCondition)); resized != nil {
		t.Fatalf("Resized = %+v, want none", resized)
	}

	if got := len(env.machineList().Items); got != 3 {
		t.Fatalf("%d machines, want 3", got)
	}
}

func TestRollingUpdateNeedsItsParameters(t *testing.T) {
	_, _, err := upgrade(t, &controlplanev1.RolloutStrategy{Type: controlplanev1.RollingUpdateStrategyType}, 3, "a", "b", "c")
	if err == nil {
		t.Fatal("upgradeControlPlane() succeeded without rollingUpdate")
	}
}
