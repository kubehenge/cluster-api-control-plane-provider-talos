// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"reflect"

	cabptv1 "github.com/kubehenge/cluster-api-bootstrap-provider-talos/api/v1alpha3"
	"github.com/pkg/errors"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	talosconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	controlplanev1 "github.com/kubehenge/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// talosconfigForMachine will generate a talosconfig that uses *all* found addresses as the endpoints.
//
// NOTE: There is no client.WithNodes(...) here, so no multiplexing is done. The request will hit any
// of the controlplane nodes in machines list.
func (r *TalosControlPlaneReconciler) talosconfigForMachines(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines ...clusterv1.Machine) (*talosclient.Client, error) {
	if len(machines) == 0 {
		return nil, fmt.Errorf("at least one machine should be provided")
	}

	clusterName := tcp.GetLabels()[clusterv1.ClusterNameLabel]

	for _, ref := range tcp.GetOwnerReferences() {
		if ref.Kind != "Cluster" {
			continue
		}

		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if gv.Group == clusterv1.GroupVersion.Group {
			clusterName = ref.Name

			break
		}
	}

	if clusterName == "" {
		return nil, fmt.Errorf("failed to determine the cluster name of the control plane")
	}

	if !reflect.ValueOf(tcp.Spec.ControlPlaneConfig.InitConfig).IsZero() {
		return r.talosconfigFromWorkloadCluster(ctx, client.ObjectKey{Namespace: tcp.GetNamespace(), Name: clusterName}, machines...)
	}

	addrList := []string{}

	var (
		talosconfigSecret corev1.Secret
	)

	if err := r.Client.Get(ctx,
		types.NamespacedName{
			Namespace: tcp.GetNamespace(),
			Name:      clusterName + "-talosconfig",
		},
		&talosconfigSecret,
	); err != nil {
		return nil, err
	}

	t, err := talosconfig.FromBytes(talosconfigSecret.Data["talosconfig"])
	if err != nil {
		return nil, err
	}

	for _, machine := range machines {
		found := false

		for _, addr := range machine.Status.Addresses {
			if addr.Type == clusterv1.MachineExternalIP || addr.Type == clusterv1.MachineInternalIP {
				addrList = append(addrList, addr.Address)
				found = true
			}
		}

		if !found {
			return nil, fmt.Errorf("no addresses were found for node %q", machine.Name)
		}
	}

	// The bootstrap provider already fills the talosconfig's endpoints; they are replaced here so that `machines`
	// limits which machines answer, as talosconfigFromWorkloadCluster sets them from the workload cluster's nodes.
	return talosclient.New(ctx,
		talosclient.WithDefaultGRPCDialOptions(),
		talosclient.WithEndpoints(addrList...),
		talosclient.WithConfig(t),
	)
}

// talosconfigFromWorkloadCluster gets talosconfig and populates endoints using workload cluster nodes.
func (r *TalosControlPlaneReconciler) talosconfigFromWorkloadCluster(ctx context.Context, cluster client.ObjectKey, machines ...clusterv1.Machine) (*talosclient.Client, error) {
	if len(machines) == 0 {
		return nil, fmt.Errorf("at least one machine should be provided")
	}

	c, err := r.ClusterCache.GetClient(ctx, cluster)
	if err != nil {
		return nil, err
	}

	addrList := []string{}

	var t *talosconfig.Config

	for _, machine := range machines {
		if !machine.Status.NodeRef.IsDefined() {
			return nil, fmt.Errorf("%q machine does not have a nodeRef", machine.Name)
		}

		var node corev1.Node

		// grab all addresses as endpoints
		err := c.Get(ctx, types.NamespacedName{Name: machine.Status.NodeRef.Name}, &node)
		if err != nil {
			return nil, err
		}

		found := false

		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeExternalIP || addr.Type == corev1.NodeInternalIP {
				addrList = append(addrList, addr.Address)
				found = true
			}
		}

		if !found {
			return nil, fmt.Errorf("no addresses were found for node %q", node.Name)
		}

		if t == nil {
			var (
				cfgs  cabptv1.TalosConfigList
				found *cabptv1.TalosConfig
			)

			// find talosconfig in the machine's namespace
			err = r.Client.List(ctx, &cfgs, client.InNamespace(machine.Namespace))
			if err != nil {
				return nil, err
			}

			for _, cfg := range cfgs.Items {
				for _, ref := range cfg.OwnerReferences {
					if ref.Kind == "Machine" && ref.Name == machine.Name {
						found = &cfg
						break
					}
				}
			}

			if found == nil {
				return nil, fmt.Errorf("failed to find TalosConfig for %q", machine.Name)
			}

			t, err = talosconfig.FromString(found.Status.TalosConfig)
			if err != nil {
				return nil, err
			}
		}
	}

	return talosclient.New(ctx,
		talosclient.WithDefaultGRPCDialOptions(),
		talosclient.WithEndpoints(addrList...),
		talosclient.WithConfig(t),
	)
}
