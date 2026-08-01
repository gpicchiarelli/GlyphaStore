#!/usr/bin/env bash
# Rewrite a .tar.gz with SOURCE_DATE_EPOCH mtimes, uid/gid 0 when supported, and gzip -n.
# Usage: normalize-tar-gz.sh <archive.tar.gz>
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
  echo "usage: $0 <archive.tar.gz>" >&2
  exit 2
fi

archive="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
epoch="${SOURCE_DATE_EPOCH:-0}"

stamp_files() {
  local root="$1"
  if touch -d "@${epoch}" "$root" >/dev/null 2>&1; then
    find "$root" -exec touch -d "@${epoch}" {} +
  else
    local stamp
    stamp="$(date -u -r "${epoch}" +%Y%m%d%H%M.%S)"
    find "$root" -exec touch -t "$stamp" {} +
  fi
}

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-norm-tar.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT

mkdir -p "$work/src"
tar -xzf "$archive" -C "$work/src"
stamp_files "$work/src"

top=""
count=0
for p in "$work/src"/* "$work/src"/.[!.]* "$work/src"/..?*; do
  [[ -e "$p" ]] || continue
  top="$(basename "$p")"
  count=$((count + 1))
done
if [[ "$count" -ne 1 ]]; then
  echo "normalize-tar-gz: expected one top-level entry in $archive (got $count)" >&2
  ls -la "$work/src" >&2 || true
  exit 1
fi

out="$work/out.tar.gz"
(
  cd "$work/src"
  if tar --version >/dev/null 2>&1 && tar --version 2>&1 | head -1 | grep -qi gnu; then
    tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@${epoch}" -cf - "$top"
  elif tar --uid 0 --gid 0 -cf /dev/null "$top" >/dev/null 2>&1; then
    tar --uid 0 --gid 0 -cf - "$top"
  else
    tar -cf - "$top"
  fi
) | gzip -n -9 >"$out"

mv -f "$out" "$archive"
echo "normalized $archive (SOURCE_DATE_EPOCH=$epoch)"
