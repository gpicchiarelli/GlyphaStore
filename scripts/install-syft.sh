#!/usr/bin/env bash
# Install a checksum-pinned Syft release binary into /usr/local/bin (or DESTDIR).
# Syft (Anchore) is Apache-2.0; see THIRD_PARTY_NOTICES.md. This script downloads
# an upstream release artifact — it does not relicense Syft.
# Override SYFT_VERSION / SYFT_SHA256 when bumping; keep supply-chain.yml in sync.
set -euo pipefail

SYFT_VERSION="${SYFT_VERSION:-v1.20.0}"
DEST_DIR="${DESTDIR:-/usr/local/bin}"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64 | amd64)
    SYFT_ARCH="amd64"
    DEFAULT_SHA="689e12c5cbf67521ce61b9c126068f9eaabe1223e77971b2fede50033ff6b5cc"
    ;;
  aarch64 | arm64)
    SYFT_ARCH="arm64"
    DEFAULT_SHA="53f76737ddbf425c89240d5b0be0990b1a71e66890b44f19743221b17e6ee635"
    ;;
  *)
    echo "unsupported architecture for Syft install: $ARCH" >&2
    exit 1
    ;;
esac
SYFT_SHA256="${SYFT_SHA256:-$DEFAULT_SHA}"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
archive="syft_${SYFT_VERSION#v}_linux_${SYFT_ARCH}.tar.gz"
url="https://github.com/anchore/syft/releases/download/${SYFT_VERSION}/${archive}"
curl -sSfL -o "$workdir/$archive" "$url"
echo "${SYFT_SHA256}  $workdir/$archive" | sha256sum -c -
tar -xzf "$workdir/$archive" -C "$workdir" syft
install -m 0755 "$workdir/syft" "$DEST_DIR/syft"
syft version
