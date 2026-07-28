#!/usr/bin/env bash
# OpenBSD / LibreSSL CI gate (security roadmap Phase 2.6 + Phase 6.5, ADR 0020).
#
# Builds glyphastored with system LibreSSL, runs the unit/integration suite,
# and smokes TLS PUT→GET with the official Go client (including pledge/unveil
# apply after Server::create).
#
# Intended for:
#   - GitHub Actions via vmactions/openbsd-vm
#   - Native OpenBSD developer hosts
#
# Environment:
#   GLYPHASTORE_OPENBSD_PRESET   cmake preset (default: unix-release)
#   GLYPHASTORE_SKIP_GO_SMOKE    set to 1 to skip Go TLS smoke
#   CC / CXX                     optional compiler overrides
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

preset="${GLYPHASTORE_OPENBSD_PRESET:-unix-release}"
build_dir="$root/build/${preset}"

if [[ "$(uname -s)" != "OpenBSD" ]]; then
  echo "error: this gate must run on OpenBSD (got $(uname -s))" >&2
  echo "hint: use .github/workflows/openbsd-libressl.yml or a native OpenBSD host" >&2
  exit 1
fi

echo "== OpenBSD / LibreSSL CI gate =="
echo "os=$(uname -a)"
echo "cwd=$root"
echo "preset=$preset"

if [[ ! -f /usr/include/openssl/ssl.h ]]; then
  echo "error: system LibreSSL headers missing (/usr/include/openssl/ssl.h)" >&2
  exit 1
fi
# OpenBSD ships versioned shared objects (libssl.so.*); accept those or a static archive.
if ! ls /usr/lib/libssl.so* >/dev/null 2>&1 && [[ ! -e /usr/lib/libssl.a ]]; then
  echo "error: system LibreSSL libssl missing under /usr/lib" >&2
  exit 1
fi

command -v cmake >/dev/null 2>&1 || { echo "error: cmake required (pkg_add cmake)" >&2; exit 1; }
command -v ninja >/dev/null 2>&1 || { echo "error: ninja required (pkg_add ninja)" >&2; exit 1; }
command -v openssl >/dev/null 2>&1 || { echo "error: openssl CLI required (LibreSSL base)" >&2; exit 1; }

echo "== configure (GLYPHASTORE_ENABLE_TLS=ON) =="
rm -rf "$build_dir"
cmake --preset "$preset" -DGLYPHASTORE_ENABLE_TLS=ON | tee /tmp/glyphastore-cmake-tls.log
if ! grep -q 'GlyphaStore TLS: enabled (LibreSSL)' /tmp/glyphastore-cmake-tls.log; then
  echo "error: expected CMake to enable TLS with LibreSSL backend" >&2
  exit 1
fi

echo "== build =="
cmake --build --preset "$preset" --target glyphastored glyphastore_tests glyphastore_interop_client

daemon="$build_dir/glyphastored"
tests="$build_dir/glyphastore_tests"
if [[ ! -x "$daemon" ]]; then
  echo "error: missing $daemon" >&2
  exit 1
fi
if ! "$daemon" --help 2>&1 | grep -q -- '--tls-cert'; then
  echo "error: glyphastored was built without TLS CLI flags" >&2
  exit 1
fi

echo "== ctest =="
ctest --preset "$preset" --output-on-failure

if [[ "${GLYPHASTORE_SKIP_GO_SMOKE:-0}" == "1" ]]; then
  echo "== skip Go TLS smoke (GLYPHASTORE_SKIP_GO_SMOKE=1) =="
  echo "OpenBSD / LibreSSL CI gate OK (tests only)"
  exit 0
fi

command -v go >/dev/null 2>&1 || { echo "error: go required for TLS smoke (pkg_add go)" >&2; exit 1; }

echo "== Go TLS smoke (PUT→GET) =="
work="$(mktemp -d /tmp/glyphastore-openbsd-tls.XXXXXX)"
cleanup() {
  if [[ -f "$work/daemon.pid" ]]; then
    kill "$(cat "$work/daemon.pid")" 2>/dev/null || true
    wait "$(cat "$work/daemon.pid")" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# Mint a SAN-bearing leaf with a portable openssl.cnf (LibreSSL may lack -addext).
cat >"$work/openssl.cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1
EOF

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$work/server.key" -out "$work/server.crt" -days 1 \
  -config "$work/openssl.cnf" >/dev/null 2>&1

mkdir -p "$root/sdk/go/bin"
(cd "$root/sdk/go" && go build -o bin/glyphastore-interop ./cmd/glyphastore-interop)
go_helper="$root/sdk/go/bin/glyphastore-interop"

"$daemon" --bind 127.0.0.1 --port 0 --workers 1 \
  --storage-mode volatile --executor-affinity \
  --tls-cert "$work/server.crt" --tls-key "$work/server.key" \
  >"$work/daemon.log" 2>&1 &
echo $! >"$work/daemon.pid"

port=""
for _ in $(jot 50 1); do
  if ! kill -0 "$(cat "$work/daemon.pid")" 2>/dev/null; then
    echo "error: glyphastored exited early; log:" >&2
    cat "$work/daemon.log" >&2
    exit 1
  fi
  if grep -q 'transport=tls1.3 backend=LibreSSL' "$work/daemon.log" 2>/dev/null; then
    port="$(sed -n 's/.* port=\([0-9][0-9]*\) .*/\1/p' "$work/daemon.log" | head -1)"
    if [[ -n "$port" ]]; then
      break
    fi
  fi
  sleep 1
done
if [[ -z "$port" ]]; then
  echo "error: could not parse TLS listen port / LibreSSL backend from daemon log:" >&2
  cat "$work/daemon.log" >&2
  exit 1
fi
echo "daemon tls_port=$port backend=LibreSSL"
if ! grep -q 'openbsd-sandbox=pledge+unveil' "$work/daemon.log"; then
  echo "error: expected openbsd-sandbox=pledge+unveil in daemon log after Server::create" >&2
  cat "$work/daemon.log" >&2
  exit 1
fi
echo "openbsd sandbox applied"

key_hex="$(printf 'openbsd-tls-smoke' | od -An -tx1 | tr -d ' \n')"
value_hex="$(printf 'libressl-ok' | od -An -tx1 | tr -d ' \n')"
"$go_helper" --port "$port" --tls --tls-ca "$work/server.crt" --server-name localhost \
  --key-hex "$key_hex" --value-hex "$value_hex" put
got="$("$go_helper" --port "$port" --tls --tls-ca "$work/server.crt" --server-name localhost \
  --key-hex "$key_hex" get | tr -d '\n')"
if [[ "$got" != "$value_hex" ]]; then
  echo "error: TLS GET mismatch got='$got' want='$value_hex'" >&2
  exit 1
fi

echo "OpenBSD / LibreSSL CI gate OK"
