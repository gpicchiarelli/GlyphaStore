#!/usr/bin/env bash
# Package SDK clients twice with a pinned SOURCE_DATE_EPOCH and compare digests of
# wheels and gems. Python sdists / Perl tar.gz are intentionally excluded until tar
# metadata is normalized. package-info.txt sidecars are excluded.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GLYPHASTORE_ROOT="$root"
# shellcheck disable=SC1091
source "$root/scripts/export-reproducible-build-env.sh"

epoch="${SOURCE_DATE_EPOCH}"
echo "SOURCE_DATE_EPOCH=$epoch ($(glyphastore_repro_iso8601 "$epoch"))"

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-repro.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT

package_once() {
  local out="$1"
  mkdir -p "$out"
  rm -rf "$root/sdk/python/dist" "$root/sdk/perl/dist" "$root/sdk/ruby/dist"
  "$root/scripts/package-python-client.sh"
  "$root/scripts/package-perl-client.sh"
  if command -v ruby >/dev/null 2>&1 || [[ -n "${RUBY:-}" ]]; then
    if "$root/scripts/package-ruby-client.sh"; then
      :
    else
      echo "note: ruby packaging skipped for reproducibility compare" >&2
    fi
  else
    echo "note: ruby not on PATH — comparing python/perl archives only" >&2
  fi
  "$root/scripts/checksum-sdk-artifacts.sh" "$out"
}

echo "== reproducible package run A =="
package_once "$work/a"
echo "== reproducible package run B =="
package_once "$work/b"

# Compare SHA256SUMS entries for archive-like artifacts only.
filter_sums() {
  # Pure-Python wheels and Ruby gems are bit-stable under SOURCE_DATE_EPOCH today.
  # Python sdists and Perl tar.gz still embed host tar metadata on some platforms;
  # those remain a residual normalization task.
  awk '
    $2 ~ /\.whl$/ { print }
    $2 ~ /\.gem$/ { print }
  ' "$1" | sort
}

filter_sums "$work/a/SHA256SUMS" >"$work/a.filtered"
filter_sums "$work/b/SHA256SUMS" >"$work/b.filtered"

if [[ ! -s "$work/a.filtered" ]]; then
  echo "no wheel/gem artifacts to compare (need python packaging; ruby gem optional)" >&2
  exit 1
fi

if ! cmp -s "$work/a.filtered" "$work/b.filtered"; then
  echo "reproducibility mismatch for wheels/gems:" >&2
  diff -u "$work/a.filtered" "$work/b.filtered" >&2 || true
  exit 1
fi

echo "SDK wheel/gem reproducibility OK (SOURCE_DATE_EPOCH=$epoch)"
cat "$work/a.filtered"
