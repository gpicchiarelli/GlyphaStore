#!/usr/bin/env bash
# Build the tagged OpenBSD port and prove the installed package/service lifecycle.
set -euo pipefail

usage() {
  echo "usage: $0 <candidate-directory> <output-directory>" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
[[ "$(uname -s)" == "OpenBSD" ]] || {
  echo "error: native OpenBSD is required" >&2
  exit 1
}

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
candidate="$(cd "$1" && pwd -P)"
mkdir -p "$2"
output="$(cd "$2" && pwd -P)"
version="$(<"$root/VERSION")"
abi_version="$(tr -d '[:space:]' <"$root/ABI_VERSION")"
ports_root="${PORTSDIR:-/usr/ports}"

[[ -n "${CANDIDATE_SEAL_SHA256:-}" ]] || {
  echo "error: CANDIDATE_SEAL_SHA256 is required" >&2
  exit 1
}
command -v python3 >/dev/null 2>&1 || { echo "error: python3 required" >&2; exit 1; }
command -v pkg_add >/dev/null 2>&1 || { echo "error: pkg_add required" >&2; exit 1; }
[[ -f "$ports_root/infrastructure/mk/bsd.port.mk" ]] || {
  echo "error: complete native ports tree required at $ports_root" >&2
  exit 1
}
[[ -f "$root/packaging/openbsd/PORTS_ACCOUNT_REGISTERED" ]] || {
  echo "error: packaging/openbsd/PORTS_ACCOUNT_REGISTERED is required" >&2
  exit 1
}

python3 - "$candidate/candidate-seal.json" "$CANDIDATE_SEAL_SHA256" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
actual = hashlib.sha256(path.read_bytes()).hexdigest()
if actual != sys.argv[2]:
    raise SystemExit(f"candidate seal digest mismatch: {actual}")
PY
python3 "$root/engineering/tools/release_bundle.py" verify-seal \
  --directory "$candidate" --seal candidate-seal.json
python3 "$root/engineering/tools/validate_bsd_packaging.py" --root "$root" --release

mapfile -t source_archives < <(
  find "$candidate" -maxdepth 1 -type f -name "GlyphaStore-$version.tar.xz" -print
)
[[ ${#source_archives[@]} -eq 1 ]] || {
  echo "error: expected exactly one sealed source archive" >&2
  exit 1
}

work="$(mktemp -d /tmp/glyphastore-openbsd-package.XXXXXX)"
cleanup() {
  rcctl stop glyphastored >/dev/null 2>&1 || true
  if pkg_info -e glyphastore >/dev/null 2>&1; then
    pkg_delete glyphastore >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

if pkg_info -e glyphastore >/dev/null 2>&1; then
  echo "error: lifecycle proof requires a clean host without glyphastore installed" >&2
  exit 1
fi

mkdir -p "$work/distfiles" "$work/packages" "$work/pobj"
rm -rf "$ports_root/databases/glyphastore"
mkdir -p "$ports_root/databases"
cp -R "$root/packaging/openbsd/." "$ports_root/databases/glyphastore/"
cp "${source_archives[0]}" "$work/distfiles/GlyphaStore-$version.tar.xz"

package_build_log="$output/openbsd-package-build.log"
{
  make -C "$ports_root/databases/glyphastore" \
    DISTDIR="$work/distfiles" WRKOBJDIR="$work/pobj" makesum
  make -C "$ports_root/databases/glyphastore" \
    DISTDIR="$work/distfiles" WRKOBJDIR="$work/pobj" checksum
  make -C "$ports_root/databases/glyphastore" \
    DISTDIR="$work/distfiles" WRKOBJDIR="$work/pobj" \
    PACKAGE_REPOSITORY="$work/packages" package
  echo "OPENBSD-PACKAGE package-build PASSED"
} 2>&1 | tee "$package_build_log"
test -s "$ports_root/databases/glyphastore/distinfo"
cp "$ports_root/databases/glyphastore/distinfo" "$output/openbsd-distinfo"

mapfile -t built_packages < <(find "$work/packages" -type f -name 'glyphastore-*.tgz' -print)
[[ ${#built_packages[@]} -eq 1 ]] || {
  echo "error: expected exactly one native OpenBSD package" >&2
  exit 1
}
openbsd_version="$(uname -r)"
architecture="$(uname -m)"
package="$output/glyphastore-$version-openbsd$openbsd_version-$architecture.tgz"
cp "${built_packages[0]}" "$package"

{
  pkg_add -D unsigned "$package"
  pkg_info -e "glyphastore-$version"
  id _glyphastore
  echo "OPENBSD-PACKAGE package-install PASSED"
} 2>&1 | tee "$output/openbsd-package-install.log"

{
  pkg_info -L glyphastore
  test -x /usr/local/bin/glyphastored
  test -f /etc/glyphastored.conf
  test -f "/usr/local/lib/libglyphastore.so.${abi_version}"
  python3 "$root/engineering/tools/check_abi_symbols.py" \
    --library "/usr/local/lib/libglyphastore.so.${abi_version}" \
    --allowlist "$root/abi/symbols-v1.txt"
  echo "OPENBSD-PACKAGE file-inventory PASSED"
} 2>&1 | tee "$output/openbsd-file-inventory.log"

{
  rcctl enable glyphastored
  rcctl start glyphastored
  for _ in $(jot 50 1); do
    netstat -an -f inet | grep -Eq '127\.0\.0\.1\.7379.*LISTEN' && break
    sleep 1
  done
  netstat -an -f inet | grep -Eq '127\.0\.0\.1\.7379.*LISTEN'
  pgrep -U _glyphastore -f '/usr/local/bin/glyphastored' >/dev/null
  echo "OPENBSD-PACKAGE service-start PASSED"
} 2>&1 | tee "$output/openbsd-service-start.log"

key_hex="6f70656e6273642d7061636b616765"
value_hex="6e61746976652d6c6966656379636c65"
recovery_key_hex="6f70656e6273642d7265636f76657279"
recovery_value_hex="64757261626c652d72657374617274"
client=(python3 "$root/scripts/sdk_interop_py.py" --host 127.0.0.1 --port 7379)
{
  PYTHONPATH="$root/sdk/python/src" "${client[@]}" put \
    --key-hex "$key_hex" --value-hex "$value_hex"
  got="$(PYTHONPATH="$root/sdk/python/src" "${client[@]}" get --key-hex "$key_hex")"
  [[ "$got" == "$value_hex" ]]
  PYTHONPATH="$root/sdk/python/src" "${client[@]}" erase --key-hex "$key_hex"
  PYTHONPATH="$root/sdk/python/src" "${client[@]}" expect-not-found --key-hex "$key_hex"
  echo "OPENBSD-PACKAGE put-get-erase PASSED"
} 2>&1 | tee "$output/openbsd-put-get-erase.log"

{
  PYTHONPATH="$root/sdk/python/src" "${client[@]}" put \
    --key-hex "$recovery_key_hex" --value-hex "$recovery_value_hex"
  rcctl stop glyphastored
  ! pgrep -U _glyphastore -f '/usr/local/bin/glyphastored' >/dev/null
  /usr/local/bin/glyphastore_verify_store -- /var/glyphastore
  echo "OPENBSD-PACKAGE graceful-shutdown PASSED"
} 2>&1 | tee "$output/openbsd-graceful-shutdown.log"

{
  rcctl start glyphastored
  for _ in $(jot 50 1); do
    netstat -an -f inet | grep -Eq '127\.0\.0\.1\.7379.*LISTEN' && break
    sleep 1
  done
  got="$(PYTHONPATH="$root/sdk/python/src" "${client[@]}" get \
    --key-hex "$recovery_key_hex")"
  [[ "$got" == "$recovery_value_hex" ]]
  rcctl stop glyphastored
  echo "OPENBSD-PACKAGE restart-recovery PASSED"
} 2>&1 | tee "$output/openbsd-restart-recovery.log"

config_marker="# retained-config-${GITHUB_RUN_ID:-local}"
{
  echo "$config_marker" >>/etc/glyphastored.conf
  pkg_add -r -D unsigned "$package"
  grep -Fqx "$config_marker" /etc/glyphastored.conf
  echo "OPENBSD-PACKAGE config-preservation PASSED"
} 2>&1 | tee "$output/openbsd-config-preservation.log"

{
  pkg_delete glyphastore
  ! pkg_info -e glyphastore >/dev/null 2>&1
  test ! -e /usr/local/bin/glyphastored
  grep -Fqx "$config_marker" /etc/glyphastored.conf
  test -d /var/glyphastore
  echo "OPENBSD-PACKAGE uninstall PASSED"
} 2>&1 | tee "$output/openbsd-uninstall.log"

cat >"$work/checks.json" <<'JSON'
[
  {"id":"package-build","command":"native OpenBSD ports checksum and package against the sealed source archive","evidence_ref":"openbsd-package-build.log"},
  {"id":"package-install","command":"pkg_add the native package and prove the dedicated service account","evidence_ref":"openbsd-package-install.log"},
  {"id":"file-inventory","command":"pkg inventory plus exact C ABI symbol allowlist","evidence_ref":"openbsd-file-inventory.log"},
  {"id":"service-start","command":"enable and start the rc.d service as _glyphastore on loopback","evidence_ref":"openbsd-service-start.log"},
  {"id":"put-get-erase","command":"protocol-v2 PUT, exact GET, ERASE and NOT_FOUND through the packaged service","evidence_ref":"openbsd-put-get-erase.log"},
  {"id":"graceful-shutdown","command":"persist a recovery key, stop through rcctl, prove process exit and verify the Store","evidence_ref":"openbsd-graceful-shutdown.log"},
  {"id":"restart-recovery","command":"restart the packaged service and recover the exact durable value","evidence_ref":"openbsd-restart-recovery.log"},
  {"id":"uninstall","command":"pkg_delete removes package-owned executables while preserving non-empty state","evidence_ref":"openbsd-uninstall.log"},
  {"id":"config-preservation","command":"force reinstall preserves an operator-modified @sample configuration","evidence_ref":"openbsd-config-preservation.log"}
]
JSON

env RUNNER_OS=OpenBSD RUNNER_ARCH="$architecture" \
  python3 "$root/engineering/tools/release_evidence.py" create \
  --root "$root" --type openbsd_package --subject "$package" \
  --output "$output/openbsd-package-evidence.json" --check-plan "$work/checks.json" \
  --limitation 'native OpenBSD VM package/service lifecycle; not FFS power-loss certification or an official ports-tree acceptance claim' \
  --require-ci

echo "OpenBSD native package lifecycle PASSED"
