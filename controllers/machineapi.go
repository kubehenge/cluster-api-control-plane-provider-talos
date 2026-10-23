// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"slices"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "github.com/kubehenge/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// controlPlaneServices are the Talos services the control plane health checks look at. System
// extension services are left out, as KubeadmControlPlane looks only at the control plane components
// and etcd: an extension that reports unhealthy, such as a guest agent, must not block a replacement.
var controlPlaneServices = []string{"apid", "cri", "etcd", "kubelet", "trustd"}

// errServiceUnhealthy reports a control plane service that is not healthy.
type errServiceUnhealthy struct {
	service string
	reason  string
}

func (e *errServiceUnhealthy) Error() string {
	return fmt.Sprintf("Service %s is unhealthy: %s", e.service, e.reason)
}

// machineAPI runs the operations on the Talos API of individual control plane machines that health
// checks, scaling and the pre-terminate hook depend on. The default implementation talks to each
// machine; tests replace it.
type machineAPI interface {
	// CheckServices returns an errServiceUnhealthy for a control plane service that reports itself
	// unhealthy, or the error that kept the services from being listed.
	CheckServices(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) error
	// ProbeEtcd inspects the etcd service of the machine and, when it is healthy, lists the etcd
	// members as the machine sees them.
	ProbeEtcd(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) etcdMachineStatus
	// LeaveEtcd forfeits etcd leadership and leaves the etcd cluster from the machine itself. It fails
	// unless the etcd service is running on the machine.
	LeaveEtcd(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) error
	// RemoveEtcdMember removes the etcd member through another machine.
	RemoveEtcdMember(ctx context.Context, tcp *controlplanev1.TalosControlPlane, via *clusterv1.Machine, member *machineapi.EtcdMember) error
}

// talosAPI returns the machineAPI of the reconciler.
func (r *TalosControlPlaneReconciler) talosAPI() machineAPI {
	if r.machineAPI != nil {
		return r.machineAPI
	}

	return talosMachineAPI{r: r}
}

// talosMachineAPI implements machineAPI through the Talos API. Each client it opens points at one
// machine only, so every response comes from that machine, and each call has its own timeout, so an
// unreachable machine costs at most etcdCallTimeout.
type talosMachineAPI struct {
	r *TalosControlPlaneReconciler
}

func (m talosMachineAPI) CheckServices(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
	defer cancel()

	c, err := m.r.talosconfigForMachines(ctx, tcp, *machine)
	if err != nil {
		return err
	}

	defer c.Close() //nolint:errcheck

	serviceList, err := c.ServiceList(ctx)
	if err != nil {
		return err
	}

	for _, message := range serviceList.Messages {
		for _, svc := range message.Services {
			if !slices.Contains(controlPlaneServices, svc.GetId()) {
				continue
			}

			if !svc.GetHealth().GetUnknown() && !svc.GetHealth().GetHealthy() {
				return &errServiceUnhealthy{
					service: svc.GetId(),
					reason:  svc.GetState(),
				}
			}
		}
	}

	return nil
}

func (m talosMachineAPI) ProbeEtcd(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) etcdMachineStatus {
	ctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
	defer cancel()

	c, err := m.r.talosconfigForMachines(ctx, tcp, *machine)
	if err != nil {
		return etcdMachineStatus{inspectErr: err}
	}

	defer c.Close() //nolint:errcheck

	svcs, err := c.ServiceInfo(ctx, etcdService)
	if err != nil {
		return etcdMachineStatus{inspectErr: err}
	}

	if err := etcdServiceError(machine.Name, svcs); err != nil {
		return etcdMachineStatus{serviceErr: err}
	}

	resp, err := c.EtcdMemberList(ctx, &machineapi.EtcdMemberListRequest{})
	if err != nil {
		return etcdMachineStatus{listErr: err}
	}

	if len(resp.GetMessages()) == 0 {
		return etcdMachineStatus{listErr: fmt.Errorf("%s: empty etcd member list response", machine.Name)}
	}

	return etcdMachineStatus{members: resp.Messages[0].Members}
}

func (m talosMachineAPI) LeaveEtcd(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
	defer cancel()

	c, err := m.r.talosconfigForMachines(ctx, tcp, *machine)
	if err != nil {
		return err
	}

	defer c.Close() //nolint:errcheck

	return m.r.gracefulEtcdLeave(ctx, c, *machine)
}

func (m talosMachineAPI) RemoveEtcdMember(ctx context.Context, tcp *controlplanev1.TalosControlPlane, via *clusterv1.Machine, member *machineapi.EtcdMember) error {
	ctx, cancel := context.WithTimeout(ctx, etcdCallTimeout)
	defer cancel()

	c, err := m.r.talosconfigForMachines(ctx, tcp, *via)
	if err != nil {
		return err
	}

	defer c.Close() //nolint:errcheck

	return m.r.forceEtcdLeave(ctx, c, member)
}
