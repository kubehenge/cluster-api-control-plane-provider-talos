# Changelog

The changes of each release of this fork. The release pages on GitHub list every commit.

## Unreleased

- A control plane machine whose etcd member is still a learner can be deleted and remediated: its own learner no longer blocks the removal of its member, and it no longer counts as a voting member in the quorum check.
- An etcd member without a name, added by a machine whose etcd never started, no longer blocks scaling, rollouts and deletions for good: the pre-terminate hook removes it by the deleted machine's address, and the etcd member audit removes it once every machine runs a healthy etcd that lists its own member.
- The etcd member audit removes one member per reconcile, checks that removing a member of a machine that is gone keeps quorum, and no longer stops at the first member it cannot remove.
- Remediation stops after three retries for as long as the replacement stays unhealthy: a retry is now told by when the machine became unhealthy rather than by the time since the previous remediation, so a template that always fails no longer gets a new machine every hour. A machine that no longer matches the control plane's configuration starts a new retry sequence, so a fixed template can replace it.

## v0.6.0-alpha.2

- The Go module is now `github.com/kubehenge/cluster-api-control-plane-provider-talos`, and it builds on this fork's bootstrap provider module, `github.com/kubehenge/cluster-api-bootstrap-provider-talos` v0.7.0-alpha.5; code that imports the API types has to change its import paths.
- The `cacppt-controller-manager-metrics-service` service now forwards to the manager's metrics port.
- The manager no longer switches off gRPC's ALPN enforcement, which only Talos releases before v1.9 needed.
- The samples in `config/samples/` use the v1beta1 APIs and a Talos 1.14 machine configuration.

## v0.6.0-alpha.1

The first release of this fork, built on Sidero Labs' main branch after v0.5.14.

- The Cluster API v1beta2 contract: the `controlplane.cluster.x-k8s.io/v1beta1` API, with `v1alpha3` still served and converted, built against Cluster API v1.14.3.
- `TalosControlPlaneTemplate` for ClusterClass, with an immutable `spec.template.spec`, and `machineNamingStrategy` for the names of the control plane machines.
- Machine lifecycle the way KubeadmControlPlane handles it: every control plane `Machine` leaves etcd at a pre-terminate hook after Cluster API drains it, preflight checks gate scaling, and remediation replaces one unhealthy machine at a time without costing etcd quorum.
- `Available` and `Paused` are reported the way KubeadmControlPlane reports them.
- Whether etcd is bootstrapped is decided through each machine's own Talos API, and only one machine is bootstrapped.
- The `OnDelete` rollout strategy replaces each machine you delete instead of shrinking the control plane.
- The v1alpha3 conversions are registered with the scheme, and the v1alpha3 infrastructure apiVersion is resolved through the contract labels.
- The finalizers of a control plane being deleted are kept, control plane machines are no longer patched over defaulted timeouts, and the control plane name label is formatted as Cluster API formats it.
- The controller image is `ghcr.io/kubehenge/cluster-api-control-plane-provider-talos`, built with ko, signed with keyless cosign and carrying build provenance; `control-plane-components.yaml` names it by digest.

## Before the fork

Sidero Labs' releases up to v0.5.14 are described in [their changelog](https://github.com/siderolabs/cluster-api-control-plane-provider-talos/blob/v0.5.14/CHANGELOG.md).
