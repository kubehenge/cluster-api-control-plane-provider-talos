// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"

	controlplanev1 "github.com/kubehenge/cluster-api-control-plane-provider-talos/api/v1beta1"
)

const (
	// etcdService is the Talos service running the etcd member of a control plane machine.
	etcdService = "etcd"

	// etcdCallTimeout bounds the Talos API calls made to one machine for one etcd operation, so that
	// an unreachable machine costs at most this long.
	etcdCallTimeout = 5 * time.Second
)

// needsRemediation reports whether a MachineHealthCheck flagged the machine as unhealthy and left its
// remediation to the control plane (MachineHealthCheckSucceeded=False and MachineOwnerRemediated=False).
// Both conditions are required: when the machine recovers on its own the MachineHealthCheck controller
// sets MachineHealthCheckSucceeded back to True but leaves MachineOwnerRemediated to the owner, so a
// check on MachineOwnerRemediated alone would treat the machine as unhealthy forever.
func needsRemediation(machine *clusterv1.Machine) bool {
	return collections.IsUnhealthyAndOwnerRemediated(machine)
}

// remainingMachines returns the owned machines that are staying in the control plane, those not being
// deleted. A deleting machine's member is removed at its pre-terminate hook, so its etcd may already be
// stopped or gone; counting it would keep EtcdClusterHealthyCondition false while it leaves. A machine
// flagged for remediation still counts: while its member is a voting member, a dead member there is a
// real loss of redundancy that must hold back scale-down and rollout.
func remainingMachines(ownedMachines []clusterv1.Machine) []clusterv1.Machine {
	machines := make([]clusterv1.Machine, 0, len(ownedMachines))

	for _, machine := range ownedMachines {
		if machine.DeletionTimestamp.IsZero() {
			machines = append(machines, machine)
		}
	}

	return machines
}

// nodeNamesForMachine returns the host names that can identify a machine's etcd member: the noderef
// name and every MachineHostName address (some infrastructure providers report the infrastructure
// machine name there, which may not match the node hostname). Each name is lowercased and has any
// domain suffix trimmed (noderef names can be FQDNs, e.g. on AWS).
func nodeNamesForMachine(machine clusterv1.Machine) []string {
	var candidates []string

	if machine.Status.NodeRef.IsDefined() {
		candidates = append(candidates, machine.Status.NodeRef.Name)
	}

	for _, address := range machine.Status.Addresses {
		if address.Type == clusterv1.MachineHostName {
			candidates = append(candidates, address.Address)
		}
	}

	names := make([]string, 0, len(candidates))

	for _, hostname := range candidates {
		hostname, _, _ = strings.Cut(hostname, ".")
		if hostname != "" {
			names = append(names, strings.ToLower(hostname))
		}
	}

	return names
}

// etcdMemberNames returns the set of host names (see nodeNamesForMachine) an etcd member may carry to
// belong to one of the machines. Machines without a noderef contribute nothing: until the kubelet has
// registered, a machine's member is accounted for by count (see etcdMembership.verify).
func etcdMemberNames(machines []clusterv1.Machine) map[string]struct{} {
	names := map[string]struct{}{}

	for _, machine := range machines {
		if !machine.Status.NodeRef.IsDefined() {
			continue
		}

		for _, name := range nodeNamesForMachine(machine) {
			names[name] = struct{}{}
		}
	}

	return names
}

// etcdMemberMatchesMachine reports whether the etcd member runs on the machine, matched by any of its
// host names.
func etcdMemberMatchesMachine(member *machineapi.EtcdMember, machine *clusterv1.Machine) bool {
	return slices.Contains(nodeNamesForMachine(*machine), strings.ToLower(member.Hostname))
}

// findEtcdMember returns the etcd member running on the machine, or nil if there is none.
func findEtcdMember(members []*machineapi.EtcdMember, machine *clusterv1.Machine) *machineapi.EtcdMember {
	for _, member := range members {
		if etcdMemberMatchesMachine(member, machine) {
			return member
		}
	}

	return nil
}

// etcdMembership holds what an etcd member list is verified against: the host names of the machines
// that must be etcd members (expected, keyed by machine name), the machines that must be members but
// whose names are not known yet because they have no noderef (pending), and the host names of every
// owned machine (owned), which decide whether a member is an orphan.
type etcdMembership struct {
	expected map[string][]string
	pending  []string
	owned    map[string]struct{}
}

// newEtcdMembership builds the membership for the owned machines, of which machines is the subset
// that takes part in the etcd health check (see remainingMachines).
func newEtcdMembership(ownedMachines, machines []clusterv1.Machine) etcdMembership {
	membership := etcdMembership{
		expected: make(map[string][]string, len(machines)),
		owned:    etcdMemberNames(ownedMachines),
	}

	for _, machine := range machines {
		if !machine.Status.NodeRef.IsDefined() {
			membership.pending = append(membership.pending, machine.Name)

			continue
		}

		membership.expected[machine.Name] = nodeNamesForMachine(machine)
	}

	return membership
}

// verify checks the etcd member list reported by node and returns the set of member host names,
// lowercased, for comparison across nodes.
//
// Every machine that must be a member has to have one, matched by any of its host names. A machine
// that must be a member but has no noderef yet (it is new and the kubelet has not registered) cannot
// be matched by name, so it is accounted for by count: each such machine claims one member matching
// no owned machine. Any further unmatched member is an orphan (auditEtcd force-removes these), and
// fewer unmatched members than pending machines means a new machine has not joined etcd yet. A member
// of a deleting machine still matches an owned machine, so it is tolerated; that machine's member
// may also already be gone, which is expected, so it is not required.
func (m etcdMembership) verify(node string, members []*machineapi.EtcdMember) (map[string]struct{}, error) {
	present := make(map[string]struct{}, len(members))

	var unmatched []string

	for _, member := range members {
		name := strings.ToLower(member.Hostname)
		present[name] = struct{}{}

		if _, ok := m.owned[name]; !ok {
			unmatched = append(unmatched, etcdMemberName(member))
		}
	}

	for machineName, names := range m.expected {
		if !slices.ContainsFunc(names, func(name string) bool {
			_, ok := present[name]

			return ok
		}) {
			return nil, fmt.Errorf("%s: etcd is missing a member for control plane machine %q", node, machineName)
		}
	}

	switch {
	case len(unmatched) > len(m.pending):
		return nil, fmt.Errorf("%s: etcd member %q does not match any control plane machine", node, unmatched[len(m.pending)])
	case len(unmatched) < len(m.pending):
		return nil, fmt.Errorf("%s: etcd is missing a member for %d of the control plane machines without a noderef yet %q",
			node, len(m.pending)-len(unmatched), m.pending)
	}

	return present, nil
}

// compareEtcdMembers reports a member list that differs from the one another node reported: each etcd
// member must see the same cluster, so a member known to only one of the two nodes is an error.
func compareEtcdMembers(node string, members map[string]struct{}, previousNode string, previous map[string]struct{}) error {
	for name := range members {
		if _, ok := previous[name]; !ok {
			return fmt.Errorf("%s: etcd member %q is not known to %s", node, name, previousNode)
		}
	}

	for name := range previous {
		if _, ok := members[name]; !ok {
			return fmt.Errorf("%s: etcd member %q known to %s is missing", node, name, previousNode)
		}
	}

	return nil
}

// etcdMachineStatus is what the Talos API of one control plane machine reported about etcd.
type etcdMachineStatus struct {
	// inspectErr is set when the etcd service of the machine could not be inspected at all.
	inspectErr error
	// serviceErr is set when the etcd service is not running, or not healthy.
	serviceErr error
	// members is the member list as the machine sees it; it is only listed from a healthy service.
	members []*machineapi.EtcdMember
	// listErr is set when a healthy service could not list its members.
	listErr error
}

// healthy reports whether the machine runs a healthy etcd service that could list its members.
func (s etcdMachineStatus) healthy() bool {
	return s.inspectErr == nil && s.serviceErr == nil && s.listErr == nil
}

// err returns the first problem the status records, or nil for a healthy machine.
func (s etcdMachineStatus) err() error {
	switch {
	case s.inspectErr != nil:
		return s.inspectErr
	case s.serviceErr != nil:
		return s.serviceErr
	default:
		return s.listErr
	}
}

// etcdServiceError returns an error unless the etcd service is running and healthy in every response.
func etcdServiceError(machine string, svcs []talosclient.ServiceInfo) error {
	if len(svcs) == 0 {
		return fmt.Errorf("%s: service %q not found", machine, etcdService)
	}

	for _, svc := range svcs {
		events := svc.Service.GetEvents().GetEvents()
		if len(events) == 0 {
			return fmt.Errorf("%s: no events recorded yet for service %q", machine, etcdService)
		}

		lastEvent := events[len(events)-1]
		if lastEvent.State != "Running" {
			return fmt.Errorf("%s: service %q not in expected state %q: current state [%s] %s", machine, etcdService, "Running", lastEvent.State, lastEvent.Msg)
		}

		if !svc.Service.GetHealth().GetHealthy() {
			return fmt.Errorf("%s: service is not healthy: %s", machine, etcdService)
		}
	}

	return nil
}

// probeEtcdMachines probes every machine concurrently, each within its own timeout, so that unreachable
// machines neither hold up the others nor add up their timeouts. The result is keyed by machine name.
func (r *TalosControlPlaneReconciler) probeEtcdMachines(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []*clusterv1.Machine) map[string]etcdMachineStatus {
	api := r.talosAPI()
	statuses := make([]etcdMachineStatus, len(machines))

	var wg sync.WaitGroup

	for i, machine := range machines {
		wg.Go(func() {
			statuses[i] = api.ProbeEtcd(ctx, tcp, machine)
		})
	}

	wg.Wait()

	result := make(map[string]etcdMachineStatus, len(machines))
	for i, machine := range machines {
		result[machine.Name] = statuses[i]
	}

	return result
}

// etcdHealth summarises what etcdHealthcheck observed, so that callers can tell a degraded etcd
// cluster that keeps quorum from one that lost it.
type etcdHealth struct {
	// machines is the number of machines expected to run an etcd member.
	machines int
	// inspected is the number of machines whose etcd service could be inspected.
	inspected int
	// healthy is the number of machines running a healthy etcd service that is a voting member.
	healthy int
	// members is the largest number of voting members any machine listed, 0 if none could list them.
	members int
}

// quorum returns the number of healthy voting members etcd needs to keep quorum.
func (h etcdHealth) quorum() int {
	total := h.members
	if total == 0 {
		total = h.machines
	}

	return total/2 + 1
}

// hasQuorum reports whether enough etcd members are healthy for etcd to keep quorum.
func (h etcdHealth) hasQuorum() bool {
	return h.healthy >= h.quorum()
}

// mayHaveQuorum reports whether etcd may still have quorum, assuming that the machines that could not
// be inspected run a healthy member. When it is false, etcd lost quorum whatever their state.
func (h etcdHealth) mayHaveQuorum() bool {
	return h.healthy+(h.machines-h.inspected) >= h.quorum()
}

// conditionReason returns the reason of the EtcdClusterHealthy condition when the health check failed.
func (h etcdHealth) conditionReason() string {
	switch {
	case h.inspected == 0:
		return controlplanev1.EtcdClusterInspectionFailedReason
	case h.hasQuorum():
		return controlplanev1.EtcdClusterUnhealthyReason
	case h.mayHaveQuorum():
		return controlplanev1.EtcdClusterInspectionFailedReason
	default:
		return controlplanev1.EtcdClusterQuorumLostReason
	}
}

// etcdHealthcheck checks etcd on every control plane machine that is not being deleted: the service
// runs and is healthy, every member list matches the machines (see etcdMembership.verify), and every
// machine sees the same members. It returns the first problem found, but always inspects every machine
// so that the returned etcdHealth describes the whole cluster.
func (r *TalosControlPlaneReconciler) etcdHealthcheck(ctx context.Context, tcp *controlplanev1.TalosControlPlane, ownedMachines []clusterv1.Machine) (etcdHealth, error) {
	machines := remainingMachines(ownedMachines)

	// If every owned machine is on its way out there is nothing left to verify. Reporting etcd healthy
	// here would open the scale-down and rollout gate without a single member checked.
	if len(ownedMachines) > 0 && len(machines) == 0 {
		return etcdHealth{}, fmt.Errorf("all %d owned control plane machines are being deleted", len(ownedMachines))
	}

	membership := newEtcdMembership(ownedMachines, machines)

	targets := make([]*clusterv1.Machine, 0, len(machines))
	params := make([]any, 0, len(machines)*2)

	for i := range machines {
		targets = append(targets, &machines[i])
		params = append(params, "node", machines[i].Name)
	}

	r.Log.Info("verifying etcd health on all nodes", params...)

	statuses := r.probeEtcdMachines(ctx, tcp, targets)

	health := etcdHealth{machines: len(machines)}

	var (
		firstErr        error
		previousNode    string
		previousMembers map[string]struct{}
	)

	// Evaluated in machine order, so that the reported problem does not depend on probe timing.
	for _, machine := range targets {
		if err := func() error {
			status := statuses[machine.Name]
			if status.inspectErr != nil {
				return status.inspectErr
			}

			health.inspected++

			if !status.healthy() {
				return status.err()
			}

			voting := 0

			for _, member := range status.members {
				if !member.IsLearner {
					voting++
				}
			}

			health.members = max(health.members, voting)

			// a learner does not vote, so its health does not help etcd keep quorum
			if member := findEtcdMember(status.members, machine); member == nil || !member.IsLearner {
				health.healthy++
			}

			members, err := membership.verify(machine.Name, status.members)
			if err != nil {
				return err
			}

			if previousMembers != nil {
				if err := compareEtcdMembers(machine.Name, members, previousNode, previousMembers); err != nil {
					return err
				}
			}

			previousNode, previousMembers = machine.Name, members

			return nil
		}(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("error checking etcd health on machine %q: %w", machine.Name, err)
		}
	}

	return health, firstErr
}

// canSafelyRemoveEtcdMember reports whether the target etcd member can be removed without losing
// quorum, as KubeadmControlPlane's targetEtcdClusterHealthy does. target is nil when the machine to
// remove has no member. healthy tells which machines run a healthy etcd service.
//
// Removing a member is itself a change that a quorum of the current voting members must commit, and
// the remaining voting members then need a quorum of their own: in a three member cluster both other
// members must be healthy, in a five member cluster three of the four others. Learners other than
// the target block the removal until they are promoted. A target that is itself a learner has no
// vote, so removing it leaves the voting members as they are. A member matching no machine counts as
// unhealthy, so anything unknown makes the check stricter.
func canSafelyRemoveEtcdMember(members []*machineapi.EtcdMember, target *machineapi.EtcdMember, machines []*clusterv1.Machine, healthy map[string]bool) (bool, string) {
	var learners, remainingVoting, remainingHealthy int

	for _, member := range members {
		// The target is skipped first: its own member being a learner must not block its removal.
		if target != nil && member.Id == target.Id {
			continue
		}

		if member.IsLearner {
			learners++

			continue
		}

		remainingVoting++

		if etcdMemberHealthy(member, machines, healthy) {
			remainingHealthy++
		}
	}

	if learners > 0 {
		return false, fmt.Sprintf("%d etcd members are learners that have not been promoted yet", learners)
	}

	currentVoting, currentHealthy := remainingVoting, remainingHealthy
	if target != nil && !target.IsLearner {
		currentVoting++

		if etcdMemberHealthy(target, machines, healthy) {
			currentHealthy++
		}
	}

	if quorum := currentVoting/2 + 1; currentHealthy < quorum {
		return false, fmt.Sprintf("%d of %d etcd members are healthy, removing a member needs %d", currentHealthy, currentVoting, quorum)
	}

	if quorum := remainingVoting/2 + 1; remainingHealthy < quorum {
		return false, fmt.Sprintf("%d of the %d remaining etcd members would be healthy, quorum needs %d", remainingHealthy, remainingVoting, quorum)
	}

	return true, fmt.Sprintf("%d of the %d remaining etcd members are healthy", remainingHealthy, remainingVoting)
}

// etcdMemberHealthy reports whether the member runs on one of the machines whose etcd is healthy.
func etcdMemberHealthy(member *machineapi.EtcdMember, machines []*clusterv1.Machine, healthy map[string]bool) bool {
	return slices.ContainsFunc(machines, func(m *clusterv1.Machine) bool {
		return healthy[m.Name] && etcdMemberMatchesMachine(member, m)
	})
}

// etcdQuorumCheck is what the probes of the control plane machines tell about removing the target's
// etcd member.
type etcdQuorumCheck struct {
	// member is the target's etcd member, nil if the target is not (or no longer) a member.
	member *machineapi.EtcdMember
	// peer is a healthy machine other than the target that the member can be removed through.
	peer *clusterv1.Machine
	// safe reports whether removing the member keeps quorum; details says why or why not.
	safe    bool
	details string
}

// checkEtcdMemberRemoval probes etcd on every machine and checks whether the target's member can be
// removed without losing quorum. The member list is taken from a healthy machine other than the
// target, or from the target itself when it is the only healthy member left. It returns an error when
// no healthy machine could list the members.
func (r *TalosControlPlaneReconciler) checkEtcdMemberRemoval(ctx context.Context, tcp *controlplanev1.TalosControlPlane, target *clusterv1.Machine, machines []*clusterv1.Machine) (etcdQuorumCheck, error) {
	statuses := r.probeEtcdMachines(ctx, tcp, machines)

	healthy := make(map[string]bool, len(machines))
	for name, status := range statuses {
		healthy[name] = status.healthy()
	}

	var (
		check   etcdQuorumCheck
		members []*machineapi.EtcdMember
		listed  bool
	)

	for _, m := range machines {
		if m.Name == target.Name || !healthy[m.Name] {
			continue
		}

		if check.peer == nil {
			check.peer = m
			members, listed = statuses[m.Name].members, true
		}
	}

	if !listed && healthy[target.Name] {
		members, listed = statuses[target.Name].members, true
	}

	if !listed {
		errs := make([]error, 0, len(machines))
		for _, m := range machines {
			errs = append(errs, statuses[m.Name].err())
		}

		return etcdQuorumCheck{}, fmt.Errorf("no healthy etcd member to list the etcd members from: %w", errors.Join(errs...))
	}

	check.member = findEtcdMember(members, target)
	if check.member == nil {
		check.member = findUnstartedEtcdMember(members, target, machines)
	}

	check.safe, check.details = canSafelyRemoveEtcdMember(members, check.member, machines, healthy)

	return check, nil
}

// findUnstartedEtcdMember returns the member without a name that the machine added, matched by its
// peer URL, or nil if there is none. A member gets its name only once its etcd first starts, so the
// member of a machine that joined etcd and failed before then can only be told by the address it was
// added with. The machine keeps its addresses until its infrastructure is deleted, after the
// pre-terminate hook; an address that another machine reports as well is not used.
func findUnstartedEtcdMember(members []*machineapi.EtcdMember, machine *clusterv1.Machine, machines []*clusterv1.Machine) *machineapi.EtcdMember {
	addresses := map[netip.Addr]struct{}{}

	for _, address := range machineIPAddresses(machine) {
		addresses[address] = struct{}{}
	}

	for _, m := range machines {
		if m.Name == machine.Name {
			continue
		}

		for _, address := range machineIPAddresses(m) {
			delete(addresses, address)
		}
	}

	for _, member := range members {
		if member.Hostname != "" {
			continue
		}

		for _, peerURL := range member.PeerUrls {
			u, err := url.Parse(peerURL)
			if err != nil {
				continue
			}

			address, err := netip.ParseAddr(u.Hostname())
			if err != nil {
				continue
			}

			if _, ok := addresses[address.Unmap()]; ok {
				return member
			}
		}
	}

	return nil
}

// machineIPAddresses returns the internal and external IP addresses the machine reports.
func machineIPAddresses(machine *clusterv1.Machine) []netip.Addr {
	var addresses []netip.Addr

	for _, address := range machine.Status.Addresses {
		if address.Type != clusterv1.MachineInternalIP && address.Type != clusterv1.MachineExternalIP {
			continue
		}

		if ip, err := netip.ParseAddr(address.Address); err == nil {
			addresses = append(addresses, ip.Unmap())
		}
	}

	return addresses
}

// etcdMemberName returns the host name of the member, or its ID for a member whose etcd has not
// started yet and so has no name.
func etcdMemberName(member *machineapi.EtcdMember) string {
	if member.Hostname != "" {
		return member.Hostname
	}

	return fmt.Sprintf("%x (no name yet)", member.Id)
}

// removeEtcdMember removes the target's etcd member: it asks the target to leave, which also forfeits
// leadership, and when that fails (the machine is unreachable, or its etcd stopped) removes the member
// through the peer.
func (r *TalosControlPlaneReconciler) removeEtcdMember(ctx context.Context, tcp *controlplanev1.TalosControlPlane, target *clusterv1.Machine, check etcdQuorumCheck) error {
	api := r.talosAPI()

	leaveErr := api.LeaveEtcd(ctx, tcp, target)
	if leaveErr == nil {
		return nil
	}

	if check.peer == nil {
		return fmt.Errorf("failed to leave etcd from machine %q, and no other machine to remove the member through: %w", target.Name, leaveErr)
	}

	r.Log.Info("graceful etcd leave failed, removing the member through another control plane machine",
		"machine", target.Name, "via", check.peer.Name, "error", leaveErr)

	if err := api.RemoveEtcdMember(ctx, tcp, check.peer, check.member); err != nil {
		return fmt.Errorf("failed to remove the etcd member of machine %q: %w", target.Name, errors.Join(leaveErr, err))
	}

	return nil
}

// gracefulEtcdLeave removes a machine from the etcd cluster by forfeiting leadership and issuing a
// "leave" request from the machine itself. It fails unless the etcd service runs on the machine: a
// stopped etcd cannot remove itself, and whether its member is gone can only be told by another member.
func (r *TalosControlPlaneReconciler) gracefulEtcdLeave(ctx context.Context, c *talosclient.Client, machineToLeave clusterv1.Machine) error {
	r.Log.Info("verifying etcd status", "machine", machineToLeave.Name)

	svcs, err := c.ServiceInfo(ctx, etcdService)
	if err != nil {
		return err
	}

	if len(svcs) == 0 {
		return fmt.Errorf("service %q not found on machine %q", etcdService, machineToLeave.Name)
	}

	for _, svc := range svcs {
		if state := svc.Service.GetState(); state != "Running" {
			return fmt.Errorf("service %q is not running on machine %q: current state %q", etcdService, machineToLeave.Name, state)
		}
	}

	r.Log.Info("forfeiting leadership", "machine", machineToLeave.Name)

	if _, err = c.EtcdForfeitLeadership(ctx, &machineapi.EtcdForfeitLeadershipRequest{}); err != nil {
		return err
	}

	r.Log.Info("leaving etcd", "machine", machineToLeave.Name)

	return c.EtcdLeaveCluster(ctx, &machineapi.EtcdLeaveClusterRequest{})
}

// forceEtcdLeave removes an etcd member by telling another control plane node to remove it, for a
// member whose machine cannot leave by itself.
func (r *TalosControlPlaneReconciler) forceEtcdLeave(ctx context.Context, c *talosclient.Client, member *machineapi.EtcdMember) error {
	r.Log.Info("removing etcd member", "memberName", etcdMemberName(member), "memberId", member.Id)

	return c.EtcdRemoveMemberByID(
		ctx,
		&machineapi.EtcdRemoveMemberByIDRequest{
			MemberId: member.Id,
		},
	)
}

// auditEtcd removes an etcd member that matches no control plane machine by name, at most one per
// call as etcd takes one membership change at a time, and reports whether it removed one. It does
// nothing while any machine has no noderef: a new machine's member cannot be matched by name yet and
// would be taken for an orphan.
//
// A member with a name belonged to a machine that is gone. Removing it must keep quorum, as at the
// pre-terminate hook; a learner has no vote, so it can always go. A member without a name was added
// by a machine whose etcd never started, or is still starting. It is removed only once every machine
// that is not being deleted runs a healthy etcd that lists the machine's own named member, and it is
// none of those members: then no machine that stays can still own it. This is proved by each
// machine's own state, so unlike KubeadmControlPlane no grace period is needed.
func (r *TalosControlPlaneReconciler) auditEtcd(ctx context.Context, tcp *controlplanev1.TalosControlPlane, ownedMachines []clusterv1.Machine) (bool, error) {
	if len(ownedMachines) == 0 {
		return false, nil
	}

	for _, machine := range ownedMachines {
		if !machine.Status.NodeRef.IsDefined() {
			return false, fmt.Errorf("some CP machines do not have a noderef")
		}
	}

	remaining := remainingMachines(ownedMachines)
	if len(remaining) == 0 {
		return false, fmt.Errorf("no CP machine which is not being deleted")
	}

	machines := make([]*clusterv1.Machine, 0, len(remaining))
	for i := range remaining {
		machines = append(machines, &remaining[i])
	}

	members, via, err := r.listEtcdMembers(ctx, tcp, machines)
	if err != nil {
		return false, err
	}

	// every member must carry a host name of one of the machines, matched the same way the health check does
	owned := etcdMemberNames(ownedMachines)

	var orphans []*machineapi.EtcdMember

	for _, member := range members {
		if _, present := owned[strings.ToLower(member.Hostname)]; !present {
			orphans = append(orphans, member)
		}
	}

	if len(orphans) == 0 {
		return false, nil
	}

	statuses := r.probeEtcdMachines(ctx, tcp, machines)

	healthy := make(map[string]bool, len(statuses))
	for name, status := range statuses {
		healthy[name] = status.healthy()
	}

	started, notStarted := startedEtcdMembers(machines, statuses)

	var errs []error

	for _, member := range orphans {
		name := etcdMemberName(member)

		if member.Hostname == "" {
			if notStarted != nil {
				r.Log.Info("keeping an etcd member without a name while a control plane machine may still own it", "member", name, "reason", notStarted.Error())

				continue
			}

			// a member list that is behind may not have the name of a member that just started yet
			if _, own := started[member.Id]; own {
				continue
			}
		}

		if !member.IsLearner {
			if safe, details := canSafelyRemoveEtcdMember(members, member, machines, healthy); !safe {
				errs = append(errs, fmt.Errorf("etcd member %q matches no control plane machine, but removing it could cost etcd its quorum: %s", name, details))

				continue
			}
		}

		r.Log.Info("removing an etcd member that matches no control plane machine", "member", name, "via", via.Name)

		if err := r.talosAPI().RemoveEtcdMember(ctx, tcp, via, member); err != nil {
			return false, fmt.Errorf("error removing etcd member %q via machine %q: %w", name, via.Name, err)
		}

		return true, nil
	}

	return false, errors.Join(errs...)
}

// listEtcdMembers returns the etcd members as the first machine with a healthy etcd lists them, and
// that machine.
func (r *TalosControlPlaneReconciler) listEtcdMembers(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []*clusterv1.Machine) ([]*machineapi.EtcdMember, *clusterv1.Machine, error) {
	api := r.talosAPI()
	errs := make([]error, 0, len(machines))

	for _, machine := range machines {
		status := api.ProbeEtcd(ctx, tcp, machine)
		if status.healthy() {
			return status.members, machine, nil
		}

		errs = append(errs, status.err())
	}

	return nil, nil, fmt.Errorf("no healthy etcd member to list the etcd members from: %w", errors.Join(errs...))
}

// startedEtcdMembers returns the IDs of the machines' own etcd members, each taken from the member list
// of the machine's own etcd. It returns an error when a machine does not run a healthy etcd that lists
// a named member of the machine: its etcd may still be starting, with a member that has no name yet.
func startedEtcdMembers(machines []*clusterv1.Machine, statuses map[string]etcdMachineStatus) (map[uint64]struct{}, error) {
	ids := make(map[uint64]struct{}, len(machines))

	for _, machine := range machines {
		status := statuses[machine.Name]
		if !status.healthy() {
			return nil, status.err()
		}

		member := findEtcdMember(status.members, machine)
		if member == nil {
			return nil, fmt.Errorf("%s: etcd does not list a member of the machine", machine.Name)
		}

		ids[member.Id] = struct{}{}
	}

	return ids, nil
}
