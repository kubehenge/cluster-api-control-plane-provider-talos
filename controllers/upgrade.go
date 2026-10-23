// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"

	"github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"

	controlplanev1 "github.com/kubehenge/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func (r *TalosControlPlaneReconciler) upgradeControlPlane(
	ctx context.Context,
	cluster *clusterv1.Cluster,
	tcp *controlplanev1.TalosControlPlane,
	controlPlane *ControlPlane,
	machinesRequireUpgrade collections.Machines,
) (ctrl.Result, error) {
	logger := controlPlane.Logger()

	if tcp.Spec.RolloutStrategy == nil {
		return ctrl.Result{}, errors.New("rolloutStrategy is not set")
	}

	switch tcp.Spec.RolloutStrategy.Type {
	case controlplanev1.RollingUpdateStrategyType:
		if tcp.Spec.RolloutStrategy.RollingUpdate == nil {
			return ctrl.Result{}, errors.New("rolloutStrategy.rollingUpdate is not set")
		}

		maxNodes := tcp.Spec.GetReplicas() + int32(tcp.Spec.RolloutStrategy.RollingUpdate.MaxSurge.IntValue())
		if int32(controlPlane.Machines.Len()) < maxNodes {
			// the preflight checks of the scale-up hold it until every machine has a node and is healthy
			return r.scaleUpControlPlane(ctx, cluster, tcp, controlPlane)
		}

		return r.scaleDownControlPlane(ctx, tcp, controlPlane, machinesRequireUpgrade)
	case controlplanev1.OnDeleteStrategyType:
		// Outdated machines go only when someone deletes them; each one deleted is replaced by a machine
		// with the new spec, while the others are still outdated.
		if int32(controlPlane.Machines.Len()) < tcp.Spec.GetReplicas() {
			return r.scaleUpControlPlane(ctx, cluster, tcp, controlPlane)
		}

		return ctrl.Result{}, nil
	default:
		logger.Info("RolloutStrategy type is not set to RollingUpdateStrategyType, unable to determine the strategy for rolling out machines")
		return ctrl.Result{}, nil
	}
}
