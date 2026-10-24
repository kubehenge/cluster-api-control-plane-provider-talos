# Cluster API Control Plane Provider Talos

The Cluster API Control Plane Provider Talos (CACPPT) is a [Cluster API](https://cluster-api.sigs.k8s.io/) control plane provider for [Talos Linux](https://www.talos.dev/).
A `TalosControlPlane` creates the control plane `Machine` objects of a cluster with their `TalosConfig`, bootstraps etcd on the first one, and scales, rolls out and remediates them the way KubeadmControlPlane does: one machine at a time, behind preflight checks, never at the cost of etcd quorum.
A machine leaves etcd at a pre-terminate hook, after Cluster API has drained its node and before its infrastructure goes.

It works together with the [Cluster API Bootstrap Provider Talos](https://github.com/kubehenge/cluster-api-bootstrap-provider-talos).

This fork is maintained primarily for [Kubehenge](https://kubehenge.dev), and it is open to anyone: issues and pull requests are welcome.
Report bugs and ask questions in [this repository's issues](https://github.com/kubehenge/cluster-api-control-plane-provider-talos/issues), and security problems privately, as [SECURITY.md](.github/SECURITY.md) describes; [CONTRIBUTING.md](.github/CONTRIBUTING.md) covers pull requests.

## Compatibility

| Component | Supported |
| --- | --- |
| Cluster API contract | v1beta2 |
| Cluster API | v1.14, built and tested against v1.14.3 |
| Talos Linux | v1.9 to v1.14, the releases the bootstrap provider generates configuration for |
| Kubernetes | the releases the Talos release supports, listed below |

| Talos Linux | Kubernetes |
| --- | --- |
| v1.14 | v1.32 to v1.37 |
| v1.13 | v1.31 to v1.36 |
| v1.12 | v1.30 to v1.35 |
| v1.11 | v1.29 to v1.34 |
| v1.10 | v1.28 to v1.33 |
| v1.9 | v1.27 to v1.32 |

The API is `controlplane.cluster.x-k8s.io/v1beta1`; `v1alpha3` is still served and converted.

## Installing

clusterctl knows a provider called `talos` that points at the Sidero Labs releases.
To install this fork instead, add both Talos providers to `~/.config/cluster-api/clusterctl.yaml` (or `~/.cluster-api/clusterctl.yaml`):

```yaml
providers:
  - name: talos
    url: https://github.com/kubehenge/cluster-api-bootstrap-provider-talos/releases/latest/bootstrap-components.yaml
    type: BootstrapProvider
  - name: talos
    url: https://github.com/kubehenge/cluster-api-control-plane-provider-talos/releases/latest/control-plane-components.yaml
    type: ControlPlaneProvider
```

Then initialise the management cluster with them:

```shell
clusterctl init --bootstrap talos --control-plane talos --infrastructure <infrastructure provider>
```

clusterctl never picks a prerelease on its own; to install one, name its version, as in `--control-plane talos:<version>`.

## Usage

```yaml
apiVersion: controlplane.cluster.x-k8s.io/v1beta1
kind: TalosControlPlane
metadata:
  name: talos-cp
spec:
  version: v1.37.1
  replicas: 3
  machineTemplate:
    spec:
      infrastructureRef:
        apiGroup: infrastructure.cluster.x-k8s.io
        kind: ProxmoxMachineTemplate
        name: talos-cp
  controlPlaneConfig:
    controlplane:
      generateType: controlplane
      talosVersion: v1.14
      strategicPatches:
        - |
          apiVersion: v1alpha1
          kind: UnattendedInstallConfig
          provisioning:
            diskSelector:
              match: disk.dev_path == "/dev/sda"
```

- `replicas`: an odd number, as etcd needs a majority of its members.
- `machineTemplate`: the infrastructure machine template, and the labels, annotations, readiness gates and deletion timeouts the control plane `Machine` objects get.
- `controlPlaneConfig.controlplane`: the `TalosConfig` spec of every control plane machine, as the bootstrap provider documents it. `generateType: none` with `data` uses a machine configuration you supply as it is.
- `machineNamingStrategy.template`: the names of the `Machine` objects, their infrastructure machines and their `TalosConfig` objects, from `.cluster.name`, `.talosControlPlane.name` and `.random`, which it must contain. The default is `{{ .talosControlPlane.name }}-{{ .random }}`.
- `rolloutStrategy`: `RollingUpdate` (the default) adds a machine with the new spec before it deletes an outdated one, with `maxSurge` 1; with `maxSurge` 0 it deletes an outdated machine first and creates its successor after. `OnDelete` waits for you to delete outdated machines and replaces each one you delete.

For managed topology, a `TalosControlPlaneTemplate` holds the same spec without `machineTemplate.spec.infrastructureRef`, which comes from `ClusterClass.spec.controlPlane.machineInfrastructure.templateRef`; `config/samples/` has an example. Its `spec.template.spec` is immutable, so a change is a new template.

The bootstrap provider stores the `talosconfig` of the cluster in the `<cluster>-talosconfig` secret:

```shell
kubectl get secret --namespace <namespace> <cluster>-talosconfig -o jsonpath='{.data.talosconfig}' | base64 -d > talosconfig
```

### Machine lifecycle

- **Deletion.** Every control plane `Machine` carries the `pre-terminate.delete.hook.machine.cluster.x-k8s.io/tcp-cleanup` hook. When Cluster API has drained a deleted machine and no other pre-terminate hook is left, the machine leaves etcd, or a healthy peer removes its member when its etcd no longer runs. The removal waits while it would cost etcd its quorum or while another member is a learner; the hook is released without touching etcd when the whole cluster goes. A member whose etcd never started has no name yet and is matched by the machine's address.
- **etcd members without a machine.** A member that matches no control plane machine is removed, one per reconcile and only while that keeps quorum. A member without a name, added by a machine whose etcd never started, is removed once every machine that is not being deleted runs a healthy etcd that lists its own member.
- **Remediation.** A machine a MachineHealthCheck marks for remediation by its owner is deleted and replaced, one at a time, never the last one, and only while losing its member keeps etcd quorum. A replacement that becomes unhealthy within an hour of the previous remediation is a retry of it: it is retried at most three times, five minutes apart, and stays held for as long as it remains unhealthy. A machine that no longer matches the control plane's configuration starts over, so a fixed template can replace it.
- **Status.** `Available` is False while the control plane is not initialised, etcd has lost quorum or no machine has a node, and Unknown once the workload cluster has been unreachable for longer than the five minute grace period. A paused Cluster or `TalosControlPlane` reports `Paused` and is left alone.

## Releases

A release is built by the release workflow from its version tag and carries `control-plane-components.yaml`, `metadata.yaml` and `SHA256SUMS`.

- The controller image is `ghcr.io/kubehenge/cluster-api-control-plane-provider-talos`, and `control-plane-components.yaml` names it by digest, so what a release installs cannot change after it is published. Its base image and every tool the build uses are pinned by digest or checksum.
- The image is signed with keyless [cosign](https://docs.sigstore.dev/) and carries a build provenance attestation; the release files carry one too.
- `SHA256SUMS` holds the checksums of the release files.

To check the image a release names before installing it:

```shell
cosign verify ghcr.io/kubehenge/cluster-api-control-plane-provider-talos@sha256:<digest> \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/kubehenge/cluster-api-control-plane-provider-talos/\.github/workflows/release\.yaml@refs/tags/v'
gh attestation verify oci://ghcr.io/kubehenge/cluster-api-control-plane-provider-talos@sha256:<digest> \
  --repo kubehenge/cluster-api-control-plane-provider-talos
```

And the downloaded release files:

```shell
sha256sum -c SHA256SUMS
gh attestation verify control-plane-components.yaml --repo kubehenge/cluster-api-control-plane-provider-talos
```

## Building

The Go module is `github.com/kubehenge/cluster-api-control-plane-provider-talos`.
Everything builds natively with Go and the tools the Makefile installs into `_out/bin`; `make help` lists the targets.

```shell
make generate manifests  # deepcopy and conversion code, CRDs, webhooks and RBAC
make lint unit           # golangci-lint and the unit tests under the race detector
make envtest integration # the integration tests against a local API server and etcd
make image KO_FLAGS=--push=false TAG=dev
```

`make envtest` downloads the linux/amd64 API server and etcd; elsewhere, point `KUBEBUILDER_ASSETS` at a directory holding them.

## Credits

This provider was created by [Sidero Labs](https://www.siderolabs.com/), who keep the original at [siderolabs/cluster-api-control-plane-provider-talos](https://github.com/siderolabs/cluster-api-control-plane-provider-talos).
Some community fixes are pulled in from open pull requests and forks.
[CHANGELOG.md](CHANGELOG.md) lists the changes of this fork's releases and links the history of the Sidero Labs releases.

## Licence

[Mozilla Public License 2.0](LICENSE).
