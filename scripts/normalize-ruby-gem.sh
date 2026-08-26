#!/usr/bin/env bash
# Canonicalize RubyGems' nested gzip streams and outer tar metadata.
# Usage: normalize-ruby-gem.sh <artifact.gem>
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
  echo "usage: $0 <artifact.gem>" >&2
  exit 2
fi

archive="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
epoch="${SOURCE_DATE_EPOCH:-0}"
export COPYFILE_DISABLE=1
export COPY_EXTENDED_ATTRIBUTES_DISABLE=1

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-norm-gem.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT
mkdir -p "$work/parts"
tar -xf "$archive" -C "$work/parts"

for member in metadata.gz data.tar.gz checksums.yaml.gz; do
  if [[ ! -f "$work/parts/$member" ]]; then
    echo "normalize-ruby-gem: missing $member in $archive" >&2
    exit 1
  fi
done

for member in metadata.gz data.tar.gz; do
  gzip -dc "$work/parts/$member" >"$work/${member%.gz}"
  gzip -n -9 <"$work/${member%.gz}" >"$work/parts/$member"
done

sha256_hex() { shasum -a 256 "$1" | awk '{print $1}'; }
sha512_hex() { shasum -a 512 "$1" | awk '{print $1}'; }

metadata_sha256="$(sha256_hex "$work/parts/metadata.gz")"
data_sha256="$(sha256_hex "$work/parts/data.tar.gz")"
metadata_sha512="$(sha512_hex "$work/parts/metadata.gz")"
data_sha512="$(sha512_hex "$work/parts/data.tar.gz")"
printf '%s\n' \
  '---' \
  'SHA256:' \
  "  metadata.gz: $metadata_sha256" \
  "  data.tar.gz: $data_sha256" \
  'SHA512:' \
  "  metadata.gz: $metadata_sha512" \
  "  data.tar.gz: $data_sha512" \
  >"$work/checksums.yaml"
gzip -n -9 <"$work/checksums.yaml" >"$work/parts/checksums.yaml.gz"

chmod 0444 "$work/parts/metadata.gz" "$work/parts/data.tar.gz" \
  "$work/parts/checksums.yaml.gz"
if touch -d "@${epoch}" "$work/parts/metadata.gz" >/dev/null 2>&1; then
  touch -d "@${epoch}" "$work/parts/metadata.gz" "$work/parts/data.tar.gz" \
    "$work/parts/checksums.yaml.gz"
else
  stamp="$(date -u -r "${epoch}" +%Y%m%d%H%M.%S)"
  touch -t "$stamp" "$work/parts/metadata.gz" "$work/parts/data.tar.gz" \
    "$work/parts/checksums.yaml.gz"
fi

out="$work/out.gem"
(
  cd "$work/parts"
  if tar --version >/dev/null 2>&1 && tar --version 2>&1 | head -1 | grep -qi gnu; then
    tar --owner=0 --group=0 --numeric-owner --mtime="@${epoch}" -cf "$out" \
      metadata.gz data.tar.gz checksums.yaml.gz
  elif tar --uid 0 --gid 0 -cf /dev/null metadata.gz >/dev/null 2>&1; then
    tar --uid 0 --gid 0 -cf "$out" metadata.gz data.tar.gz checksums.yaml.gz
  else
    tar -cf "$out" metadata.gz data.tar.gz checksums.yaml.gz
  fi
)
mv -f "$out" "$archive"
echo "normalized $archive (SOURCE_DATE_EPOCH=$epoch)"
