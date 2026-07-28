#!/usr/bin/env bash
# Package in-tree golden fixtures as a labeled released-compatibility tree.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <label>" >&2
  echo "example: $0 0.1.0-alpha.1" >&2
  exit 2
fi

label="$1"
root="$(cd "$(dirname "$0")/.." && pwd)"
source_fixtures="${root}/tests/fixtures"
destination="${source_fixtures}/released/${label}"

mkdir -p "${destination}"
shopt -s nullglob
copied=0
for fixture in "${source_fixtures}"/*.hex; do
  cp "${fixture}" "${destination}/"
  copied=$((copied + 1))
done
shopt -u nullglob

{
  echo "label=${label}"
  echo "packaged_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if git -C "${root}" rev-parse HEAD >/dev/null 2>&1; then
    echo "git_commit=$(git -C "${root}" rev-parse HEAD)"
    echo "git_describe=$(git -C "${root}" describe --always --dirty 2>/dev/null || true)"
  fi
  echo "fixture_count=${copied}"
} >"${destination}/METADATA.txt"

echo "packaged ${copied} fixtures into ${destination}"
