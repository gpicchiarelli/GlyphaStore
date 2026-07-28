#!/usr/bin/env bash
# Write SHA-256 checksums (and optional SBOM) for packaged SDK artifacts.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
outdir="${1:-$root/dist/sdk-artifacts}"
rm -rf "$outdir"
mkdir -p "$outdir"

if command -v shasum >/dev/null 2>&1; then
  digest() { shasum -a 256 "$1"; }
elif command -v sha256sum >/dev/null 2>&1; then
  digest() { sha256sum "$1"; }
else
  echo "need shasum or sha256sum" >&2
  exit 1
fi

manifest="$outdir/SHA256SUMS"
: >"$manifest"

add() {
  local path="$1"
  local dest_name="${2:-$(basename "$path")}"
  [[ -f "$path" ]] || return 0
  cp "$path" "$outdir/$dest_name"
  (
    cd "$outdir"
    digest "$dest_name"
  ) >>"$manifest"
  echo "checksummed $dest_name"
}

shopt -s nullglob
# Distinct names: macOS default FS is case-insensitive (glyphastore vs GlyphaStore).
for f in "$root/sdk/python/dist"/glyphastore-*.whl; do
  add "$f"
done
for f in "$root/sdk/python/dist"/glyphastore-*.tar.gz; do
  add "$f" "python-$(basename "$f")"
done
for f in "$root/sdk/perl/dist"/GlyphaStore-*.tar.gz; do
  add "$f" "perl-$(basename "$f")"
done
for f in "$root/sdk/ruby/dist"/glyphastore-*.gem; do
  add "$f"
done
add "$root/sdk/go/dist/package-info.txt" "go-package-info.txt"
add "$root/sdk/erlang/dist/package-info.txt" "erlang-package-info.txt"
add "$root/sdk/cpp-dist/package-info.txt" "cpp-package-info.txt"
shopt -u nullglob

if [[ -s "$manifest" ]]; then
  sort -u "$manifest" -o "$manifest"
fi

if command -v syft >/dev/null 2>&1; then
  for f in "$outdir"/*; do
    [[ -f "$f" && "$(basename "$f")" != "SHA256SUMS" && "$f" != *.spdx.json && "$f" != SBOM.README ]] || continue
    base="$(basename "$f")"
    syft "path:$f" -o "spdx-json=$outdir/${base}.spdx.json" 2>/dev/null || true
  done
  echo "SBOM: syft SPDX JSON written alongside artifacts (best-effort)"
else
  cat >"$outdir/SBOM.README" <<'EOF'
No SBOM generator found on PATH.

For release perfection, generate SPDX/CycloneDX with one of:
  - syft path:./artifact -o spdx-json=artifact.spdx.json
  - cyclonedx-py / cyclonedx-gomod / etc.

Attach SBOM + SHA256SUMS to the GitHub Release. Sign with Sigstore (cosign)
or project GPG once keys/OIDC are configured.
EOF
fi

echo "Artifact checksums written to $manifest"
