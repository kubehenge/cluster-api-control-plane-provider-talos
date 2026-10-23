# Everything runs natively with the Go toolchain go.mod names: no container is needed to generate,
# lint, test or build. Tools are installed into $(BIN) at the versions below; `go install` checks
# each one against the Go checksum database, so a version pins its content as a digest would.

REGISTRY ?= ghcr.io/kubehenge
NAME := cluster-api-control-plane-provider-talos
IMAGE ?= $(REGISTRY)/$(NAME)
TAG ?= $(shell git describe --tags --always --dirty)
ARTIFACTS := _out
BIN := $(abspath $(ARTIFACTS)/bin)
PLATFORMS ?= linux/amd64,linux/arm64

GO ?= go
export CGO_ENABLED ?= 0
export GOFLAGS ?= -trimpath

# renovate: datasource=go depName=sigs.k8s.io/controller-tools
CONTROLLER_GEN_VERSION := v0.23.0
# conversion-gen comes from the Kubernetes minor go.mod builds against, so the generated conversions
# match the apimachinery they call.
# renovate: datasource=go depName=k8s.io/code-generator
CONVERSION_GEN_VERSION := v0.36.5
# renovate: datasource=go depName=github.com/golangci/golangci-lint/v2
GOLANGCI_LINT_VERSION := v2.15.0
# renovate: datasource=go depName=sigs.k8s.io/kustomize/kustomize/v5
KUSTOMIZE_VERSION := v5.8.3
# renovate: datasource=go depName=github.com/google/ko
KO_VERSION := v0.19.1
# renovate: datasource=go depName=github.com/sigstore/cosign/v3
COSIGN_VERSION := v3.1.4
# The Kubernetes API server and etcd the integration tests run on linux/amd64, as CI does; elsewhere
# set KUBEBUILDER_ASSETS to a directory holding kube-apiserver and etcd of the same release.
# renovate: datasource=github-release-attachments depName=kubernetes-sigs/controller-tools
ENVTEST_VERSION := envtest-v1.37.0
ENVTEST_SHA256 := f03faedfc68d2a78cee5cb02618bb44e7f67044f4c5fc957f72d3307fda61a9c

# The suffix `go install` gives binaries on the build platform, so the tool targets find what they built.
GOEXE := $(shell $(GO) env GOEXE)
CONTROLLER_GEN := $(BIN)/controller-gen$(GOEXE)
CONVERSION_GEN := $(BIN)/conversion-gen$(GOEXE)
GOLANGCI_LINT := $(BIN)/golangci-lint$(GOEXE)
KUSTOMIZE := $(BIN)/kustomize$(GOEXE)
KO := $(BIN)/ko$(GOEXE)
COSIGN := $(BIN)/cosign$(GOEXE)
ENVTEST_DIR := $(abspath $(ARTIFACTS)/envtest/$(ENVTEST_VERSION))
# Each API version by name: controller-gen can generate nothing for ./... on some platforms, and it
# fails on ./api/..., whose root holds no Go files.
API_PACKAGES := $(patsubst %/,./%/...,$(sort $(wildcard api/*/)))

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show the targets.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'

$(CONTROLLER_GEN):
	GOBIN=$(BIN) $(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

$(CONVERSION_GEN):
	GOBIN=$(BIN) $(GO) install k8s.io/code-generator/cmd/conversion-gen@$(CONVERSION_GEN_VERSION)

$(GOLANGCI_LINT):
	GOBIN=$(BIN) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(KUSTOMIZE):
	GOBIN=$(BIN) $(GO) install sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)

$(KO):
	GOBIN=$(BIN) $(GO) install github.com/google/ko@$(KO_VERSION)

$(COSIGN):
	GOBIN=$(BIN) $(GO) install github.com/sigstore/cosign/v3/cmd/cosign@$(COSIGN_VERSION)

.PHONY: tools
tools: $(CONTROLLER_GEN) $(CONVERSION_GEN) $(GOLANGCI_LINT) $(KUSTOMIZE) $(KO) $(COSIGN) ## Install the pinned tools into _out/bin.

.PHONY: generate
generate: $(CONTROLLER_GEN) $(CONVERSION_GEN) ## Generate deepcopy and conversion code.
	$(CONTROLLER_GEN) object:headerFile=./hack/boilerplate.go.txt $(addprefix paths=,$(API_PACKAGES))
	$(CONVERSION_GEN) --output-file=zz_generated.conversion.go --go-header-file=./hack/boilerplate.go.txt ./api/v1alpha3

.PHONY: manifests
manifests: $(CONTROLLER_GEN) ## Generate the CRDs, webhooks and RBAC rules.
	$(CONTROLLER_GEN) crd:crdVersions=v1 $(addprefix paths=,$(API_PACKAGES)) output:crd:dir=config/crd/bases output:webhook:dir=config/webhook webhook
	$(CONTROLLER_GEN) rbac:roleName=manager-role paths="./controllers/..." output:rbac:dir=config/rbac

.PHONY: check-dirty
check-dirty: ## Fail when the tree differs from what is committed, as after generate or manifests.
	@if test -n "`git status --porcelain`"; then echo "Source tree is dirty"; git status; git diff; exit 1; fi

.PHONY: lint
lint: $(GOLANGCI_LINT) ## Run golangci-lint.
	$(GOLANGCI_LINT) run --allow-serial-runners ./...

.PHONY: unit
unit: ## Run the unit tests under the race detector.
	mkdir -p $(ARTIFACTS)
	CGO_ENABLED=1 $(GO) test -race -count=1 -covermode=atomic -coverprofile=$(ARTIFACTS)/coverage.txt ./api/... ./controllers/... ./pkg/...

$(ENVTEST_DIR)/kube-apiserver:
	mkdir -p $(ENVTEST_DIR)
	curl -fsSL --proto '=https' --retry 5 -o $(ENVTEST_DIR)/envtest.tar.gz \
		https://github.com/kubernetes-sigs/controller-tools/releases/download/$(ENVTEST_VERSION)/$(ENVTEST_VERSION)-linux-amd64.tar.gz
	echo "$(ENVTEST_SHA256)  $(ENVTEST_DIR)/envtest.tar.gz" | sha256sum -c -
	tar -xzf $(ENVTEST_DIR)/envtest.tar.gz -C $(ENVTEST_DIR) --strip-components=2
	rm $(ENVTEST_DIR)/envtest.tar.gz

.PHONY: envtest
envtest: $(ENVTEST_DIR)/kube-apiserver ## Download the API server and etcd the integration tests run.

.PHONY: integration
integration: ## Run the integration tests against a local API server (run `make envtest` first, or set KUBEBUILDER_ASSETS).
	KUBEBUILDER_ASSETS=$${KUBEBUILDER_ASSETS:-$(ENVTEST_DIR)} $(GO) test -count=1 ./internal/integration/...

.PHONY: image
image: $(KO) ## Build the controller image with ko and push it to $(IMAGE); prints IMAGE@digest.
	KO_DOCKER_REPO=$(IMAGE) $(KO) build --bare --sbom=none --platform=$(PLATFORMS) \
		--image-label org.opencontainers.image.source=https://github.com/kubehenge/$(NAME) \
		--image-label org.opencontainers.image.licenses=MPL-2.0 \
		--tags $(TAG) $(KO_FLAGS) .

.PHONY: release-manifests
release-manifests: $(KUSTOMIZE) ## Render control-plane-components.yaml and metadata.yaml for CONTROLLER_IMAGE into _out/release.
	@test -n "$(CONTROLLER_IMAGE)" || { echo "set CONTROLLER_IMAGE to the image reference, by digest"; exit 1; }
	rm -rf $(ARTIFACTS)/release $(ARTIFACTS)/config
	mkdir -p $(ARTIFACTS)/release
	cp -r config $(ARTIFACTS)/config
	cd $(ARTIFACTS)/config/manager && $(KUSTOMIZE) edit set image controller=$(CONTROLLER_IMAGE)
	$(KUSTOMIZE) build $(ARTIFACTS)/config/default > $(ARTIFACTS)/release/control-plane-components.yaml
	cp config/metadata/metadata.yaml $(ARTIFACTS)/release/metadata.yaml
	rm -rf $(ARTIFACTS)/config

.PHONY: clean
clean: ## Remove _out.
	rm -rf $(ARTIFACTS)
