#!/bin/sh
# Installs the Go toolchain CI builds with into <dir>, checked against its SHA-256, for linux/amd64.
# go.mod names the same release, so with GOTOOLCHAIN=local nothing downloads another one.
#
#   hack/install-go.sh <dir>
set -eu

# renovate: datasource=golang-version depName=go
GO=1.27.3
GO_SHA256=65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40

dir=${1:?usage: $0 <dir>}
root=$(cd "$(dirname "$0")/.." && pwd)
if ! grep -qx "go ${GO}" "${root}/go.mod"; then
    echo "install-go: go.mod does not name go ${GO}" >&2
    exit 1
fi

archive=$(mktemp)
trap 'rm -f "${archive}"' EXIT
curl -fsSL --proto '=https' --retry 5 --retry-all-errors -o "${archive}" "https://go.dev/dl/go${GO}.linux-amd64.tar.gz"
echo "${GO_SHA256}  ${archive}" | sha256sum -c - >/dev/null
mkdir -p "${dir}"
tar -xzf "${archive}" -C "${dir}" --strip-components=1
