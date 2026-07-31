#!/usr/bin/env bash
# Focused secure-profile interop smoke (ADR 0020–0022 + ADR 0030).
# Proves: mTLS connect, --authz-map allows PUT/GET, keyed SipHash routing under
# --worker-hash-seed with --secure-profile. Also covers authz deny, prefix scope,
# and --tls-crl rejection. Happy path: cpp/python/go, plus perl when IO::Socket::SSL
# is available.
#
# Usage:
#   ./scripts/test-secure-profile-interop.sh
# Env:
#   GLYPHASTORED / GLYPHASTORE_INTEROP_CLIENT / GLYPHASTORE_GO_INTEROP
#   INTEROP_WORKER_HASH_SEED (default 13957458623937596)
#   INTEROP_SECURE_WORKERS (default 2)
#   PYTHON / GO
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3}"
perl="${PERL:-perl}"
daemon="${GLYPHASTORED:-}"
cpp_client="${GLYPHASTORE_INTEROP_CLIENT:-}"
hash_seed="${INTEROP_WORKER_HASH_SEED:-13957458623937596}"
workers="${INTEROP_SECURE_WORKERS:-2}"
# Principal extracted by daemon: URI SAN → DNS SAN → CN (secure-profile.md §2).
client_principal="interop.client"

prefer_bins=(
  "$root/build/macos-debug"
  "$root/build/macos-release"
  "$root/build/macos-native-release"
  "$root/build/unix-release"
  "$root/build/unix-debug"
)

if [[ -z "$daemon" ]]; then
  for dir in "${prefer_bins[@]}"; do
    if [[ -x "$dir/glyphastored" ]] && "$dir/glyphastored" --help 2>&1 | grep -q -- '--secure-profile'; then
      daemon="$dir/glyphastored"
      break
    fi
  done
fi
if [[ -z "$cpp_client" ]]; then
  daemon_dir="$(dirname "${daemon:-.}")"
  if [[ -n "$daemon" && -x "$daemon_dir/glyphastore_interop_client" ]]; then
    cpp_client="$daemon_dir/glyphastore_interop_client"
  else
    for dir in "${prefer_bins[@]}"; do
      if [[ -x "$dir/glyphastore_interop_client" ]]; then
        cpp_client="$dir/glyphastore_interop_client"
        break
      fi
    done
  fi
fi

if [[ -z "$daemon" || ! -x "$daemon" ]]; then
  echo "missing TLS-capable glyphastored with --secure-profile; build first" >&2
  exit 1
fi
if [[ -z "$cpp_client" || ! -x "$cpp_client" ]]; then
  echo "missing glyphastore_interop_client; build target glyphastore_interop_client first" >&2
  exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl CLI required to mint mTLS material" >&2
  exit 1
fi
if ! "$daemon" --help 2>&1 | grep -q -- '--tls-client-ca'; then
  echo "daemon built without mTLS (--tls-client-ca); rebuild with GLYPHASTORE_ENABLE_TLS=ON" >&2
  exit 1
fi

export PYTHONPATH="$root/sdk/python/src${PYTHONPATH:+:$PYTHONPATH}"
export PERL5LIB="$root/sdk/perl/lib${PERL5LIB:+:$PERL5LIB}"
py_helper="$root/scripts/sdk_interop_py.py"
pl_helper="$root/scripts/sdk_interop_perl.pl"
go_helper="${GLYPHASTORE_GO_INTEROP:-}"
if [[ -z "$go_helper" || ! -x "$go_helper" ]]; then
  mkdir -p "$root/sdk/go/bin"
  (cd "$root/sdk/go" && "${GO:-go}" build -o bin/glyphastore-interop ./cmd/glyphastore-interop)
  go_helper="$root/sdk/go/bin/glyphastore-interop"
fi
chmod +x "$py_helper" 2>/dev/null || true

perl_ready=0
if "$perl" -MIO::Socket::SSL -e1 >/dev/null 2>&1; then
  perl_ready=1
else
  echo "note: Perl without IO::Socket::SSL — excluding perl from secure-profile matrix" >&2
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-secure-interop.XXXXXX")"
port_file="$work/port"
log_file="$work/server.log"
active_pid=""

cleanup() {
  if [[ -n "$active_pid" ]] && kill -0 "$active_pid" 2>/dev/null; then
    kill "$active_pid" 2>/dev/null || true
    wait "$active_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

to_hex() {
  od -An -tx1 | tr -d ' \n'
}

# Mint CA + server leaf + allowed client + stranger client (valid mTLS, unmapped).
make_mtls_material() {
  local directory="$1"
  local ca_cnf="$directory/ca.cnf"
  local server_cnf="$directory/server.cnf"
  local client_cnf="$directory/client.cnf"
  local stranger_cnf="$directory/stranger.cnf"

  cat >"$ca_cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[req_distinguished_name]
CN = glyphastore-interop-ca

[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF

  cat >"$server_cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1
EOF

  cat >"$client_cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = ${client_principal}

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = DNS:${client_principal}
EOF

  cat >"$stranger_cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = stranger.client

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = DNS:stranger.client
EOF

  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$directory/ca.key" -out "$directory/ca.crt" -days 1 \
    -config "$ca_cnf" >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -keyout "$directory/server.key" -out "$directory/server.csr" \
    -config "$server_cnf" >/dev/null 2>&1
  openssl x509 -req -in "$directory/server.csr" \
    -CA "$directory/ca.crt" -CAkey "$directory/ca.key" -CAcreateserial \
    -out "$directory/server.crt" -days 1 \
    -extfile "$server_cnf" -extensions v3_req >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -keyout "$directory/client.key" -out "$directory/client.csr" \
    -config "$client_cnf" >/dev/null 2>&1
  openssl x509 -req -in "$directory/client.csr" \
    -CA "$directory/ca.crt" -CAkey "$directory/ca.key" -CAcreateserial \
    -out "$directory/client.crt" -days 1 \
    -extfile "$client_cnf" -extensions v3_req >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -keyout "$directory/stranger.key" -out "$directory/stranger.csr" \
    -config "$stranger_cnf" >/dev/null 2>&1
  openssl x509 -req -in "$directory/stranger.csr" \
    -CA "$directory/ca.crt" -CAkey "$directory/ca.key" -CAcreateserial \
    -out "$directory/stranger.crt" -days 1 \
    -extfile "$stranger_cnf" -extensions v3_req >/dev/null 2>&1
}

# Issue + immediately revoke a client cert; write PEM CRL (openssl ca database).
make_revoked_client_and_crl() {
  local directory="$1"
  local ca_dir="$directory/ca-db"
  mkdir -p "$ca_dir"
  touch "$ca_dir/index.txt"
  echo 1000 >"$ca_dir/serial"
  echo 1000 >"$ca_dir/crlnumber"

  cat >"$ca_dir/openssl.cnf" <<EOF
[ ca ]
default_ca = CA_default

[ CA_default ]
dir = $ca_dir
database = \$dir/index.txt
new_certs_dir = \$dir
certificate = $directory/ca.crt
serial = \$dir/serial
crlnumber = \$dir/crlnumber
private_key = $directory/ca.key
default_md = sha256
default_days = 1
default_crl_days = 1
policy = policy_any

[ policy_any ]
commonName = supplied
EOF

  cat >"$directory/revoked.cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = revoked.client

[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = DNS:revoked.client
EOF

  openssl req -newkey rsa:2048 -nodes \
    -keyout "$directory/revoked.key" -out "$directory/revoked.csr" \
    -config "$directory/revoked.cnf" >/dev/null 2>&1
  openssl ca -batch -config "$ca_dir/openssl.cnf" \
    -in "$directory/revoked.csr" -out "$directory/revoked.crt" \
    -extensions v3_req -extfile "$directory/revoked.cnf" >/dev/null 2>&1
  openssl ca -batch -config "$ca_dir/openssl.cnf" \
    -revoke "$directory/revoked.crt" >/dev/null 2>&1
  openssl ca -batch -config "$ca_dir/openssl.cnf" \
    -gencrl -out "$directory/clients.crl" >/dev/null 2>&1
}

stop_server() {
  if [[ -n "$active_pid" ]] && kill -0 "$active_pid" 2>/dev/null; then
    kill "$active_pid" 2>/dev/null || true
    wait "$active_pid" 2>/dev/null || true
  fi
  active_pid=""
  rm -f "$port_file" "${port_file}.pid"
}

start_server() {
  local dump_cmd=(
    "$daemon" --dump-config --workers "$workers" --max-connections 4096
    --secure-profile
    --tls-cert "$work/server.crt" --tls-key "$work/server.key"
    --tls-client-ca "$work/ca.crt" --authz-map "$work/authz.map"
    --worker-hash-seed "$hash_seed"
  )
  local run_cmd=(
    "$daemon" --secure-profile
    --bind 127.0.0.1 --port 0 --workers "$workers"
    --storage-mode volatile --executor-affinity --quiet
    --tls-cert "$work/server.crt" --tls-key "$work/server.key"
    --tls-client-ca "$work/ca.crt" --authz-map "$work/authz.map"
    --worker-hash-seed "$hash_seed"
  )
  if [[ $# -gt 0 ]]; then
    dump_cmd+=("$@")
    run_cmd+=("$@")
  fi

  local resolved
  resolved="$("${dump_cmd[@]}")"
  if { ! grep -qx "workers=$workers" <<<"$resolved" &&
      ! grep -qx "shard-pairs=$workers" <<<"$resolved"; } ||
      ! grep -qx "secure-profile=true" <<<"$resolved" ||
      ! grep -qx "worker-hash-seed=$hash_seed" <<<"$resolved"; then
    echo "glyphastored secure-profile configuration mismatch" >&2
    echo "$resolved" >&2
    return 1
  fi

  : >"$log_file"
  "${run_cmd[@]}" >"$log_file" 2>&1 &
  active_pid=$!
  echo "$active_pid" >"${port_file}.pid"

  local port=""
  for _ in $(seq 1 50); do
    if ! kill -0 "$active_pid" 2>/dev/null; then
      echo "glyphastored exited early; see $log_file" >&2
      cat "$log_file" >&2 || true
      return 1
    fi
    # lsof exits 1 before LISTEN; keep pipefail from aborting the smoke loop.
    port="$(lsof -nP -iTCP -sTCP:LISTEN -a -p "$active_pid" 2>/dev/null \
      | awk 'NR==2 {split($9,a,":"); print a[length(a)]}' || true)"
    if [[ -n "$port" ]]; then
      echo "$port" >"$port_file"
      return 0
    fi
    sleep 0.1
  done
  echo "could not discover glyphastored listen port; log:" >&2
  cat "$log_file" >&2 || true
  return 1
}

tls_args=(
  --tls
  --tls-ca "$work/ca.crt"
  --tls-cert "$work/client.crt"
  --tls-key "$work/client.key"
  --server-name localhost
)

put_sdk() {
  local sdk="$1" port="$2" key_hex="$3" value_hex="$4"
  case "$sdk" in
    cpp) "$cpp_client" --port "$port" "${tls_args[@]}" --key-hex "$key_hex" --value-hex "$value_hex" put ;;
    python) "$python" "$py_helper" --port "$port" "${tls_args[@]}" --key-hex "$key_hex" --value-hex "$value_hex" put ;;
    go) "$go_helper" --port "$port" "${tls_args[@]}" --key-hex "$key_hex" --value-hex "$value_hex" put ;;
    perl) "$perl" "$pl_helper" --port "$port" "${tls_args[@]}" --key-hex "$key_hex" --value-hex "$value_hex" put ;;
    *) return 1 ;;
  esac
}

expect_get() {
  local sdk="$1" port="$2" key_hex="$3" want_hex="$4"
  local got
  case "$sdk" in
    cpp) got="$("$cpp_client" --port "$port" "${tls_args[@]}" --key-hex "$key_hex" get | tr -d '\n')" ;;
    python) got="$("$python" "$py_helper" --port "$port" "${tls_args[@]}" --key-hex "$key_hex" get | tr -d '\n')" ;;
    go) got="$("$go_helper" --port "$port" "${tls_args[@]}" --key-hex "$key_hex" get | tr -d '\n')" ;;
    perl) got="$("$perl" "$pl_helper" --port "$port" "${tls_args[@]}" --key-hex "$key_hex" get | tr -d '\n')" ;;
    *) return 1 ;;
  esac
  if [[ "$got" != "$want_hex" ]]; then
    echo "GET mismatch sdk=$sdk want=$want_hex got=$got" >&2
    return 1
  fi
}

echo "== mint mTLS material + authz map =="
make_mtls_material "$work"
cat >"$work/authz.map" <<EOF
# Secure-profile interop principal (DNS SAN → ${client_principal})
${client_principal} write
EOF

echo "== start secure-profile daemon workers=$workers seed=$hash_seed =="
start_server
port="$(cat "$port_file")"
echo "daemon port=$port principal=$client_principal"

sdks=(cpp python go)
if [[ "$perl_ready" == "1" ]]; then
  sdks+=(perl)
fi
echo "== secure-profile interop routing=keyed workers=$workers sdks=${sdks[*]} =="

case_id=0
for writer in "${sdks[@]}"; do
  for reader in "${sdks[@]}"; do
    case_id=$((case_id + 1))
    key_hex="$(printf 'secure-%02d-w%s\x00\xff' "$case_id" "$workers" | to_hex)"
    value_hex="$(printf 'val-%s-%s-\x00\xff' "$writer" "$reader" | to_hex)"
    echo "  $writer PUT → $reader GET (mTLS+authz)"
    put_sdk "$writer" "$port" "$key_hex" "$value_hex"
    expect_get "$reader" "$port" "$key_hex" "$value_hex"
  done
done

# Deterministic SipHash owner keys prove INIT keyed routing under secure-profile.
while IFS=: read -r owner route_key; do
  route_value="$(printf 'owner-%s-of-%s' "$owner" "$workers" | to_hex)"
  echo "  cpp PUT → python GET (keyed owner $owner)"
  put_sdk cpp "$port" "$route_key" "$route_value"
  expect_get python "$port" "$route_key" "$route_value"
  echo "  go GET (keyed owner $owner)"
  expect_get go "$port" "$route_key" "$route_value"
  if [[ "$perl_ready" == "1" ]]; then
    echo "  perl GET (keyed owner $owner)"
    expect_get perl "$port" "$route_key" "$route_value"
  fi
done < <("$python" - "$workers" "$hash_seed" <<'PY'
import sys

from glyphastore.protocol import (
    ROUTING_ALG_SIPHASH24_V1,
    WorkerRouting,
    hash_key_routing,
)

workers = int(sys.argv[1])
seed = int(sys.argv[2])
state = WorkerRouting(algorithm=ROUTING_ALG_SIPHASH24_V1, seed=seed)
found = {}
candidate = 0
while len(found) < workers:
    key = f"secure-route-w{workers}-{candidate}".encode()
    found.setdefault(hash_key_routing(key, state) % workers, key.hex())
    candidate += 1
for owner in range(workers):
    print(f"{owner}:{found[owner]}")
PY
)

echo "== authz deny: unmapped stranger.client =="
deny_key="$(printf 'deny-stranger' | to_hex)"
deny_val="$(printf 'nope' | to_hex)"
if "$python" "$py_helper" --port "$port" \
  --tls --tls-ca "$work/ca.crt" \
  --tls-cert "$work/stranger.crt" --tls-key "$work/stranger.key" \
  --server-name localhost \
  --key-hex "$deny_key" --value-hex "$deny_val" \
  expect-permission-denied; then
  echo "  python PUT as stranger.client → PERMISSION_DENIED OK"
else
  echo "stranger.client PUT was not denied" >&2
  exit 1
fi

echo "== prefix scope restart =="
stop_server
cat >"$work/authz.map" <<EOF
${client_principal} write prefix=tenant-a/
EOF
start_server
port="$(cat "$port_file")"

ok_key="$(printf 'tenant-a/ok-\x00' | to_hex)"
ok_val="$(printf 'scoped' | to_hex)"
bad_key="$(printf 'tenant-b/nope' | to_hex)"
bad_val="$(printf 'out' | to_hex)"
echo "  cpp PUT tenant-a/… → python GET"
put_sdk cpp "$port" "$ok_key" "$ok_val"
expect_get python "$port" "$ok_key" "$ok_val"
echo "  python PUT outside prefix → PERMISSION_DENIED"
if ! "$python" "$py_helper" --port "$port" "${tls_args[@]}" \
  --key-hex "$bad_key" --value-hex "$bad_val" expect-permission-denied; then
  echo "prefix mismatch PUT was not denied" >&2
  exit 1
fi

echo "== CRL revoke restart =="
make_revoked_client_and_crl "$work"
# Allowlisted principal still present for healthy path after CRL load.
cat >"$work/authz.map" <<EOF
${client_principal} write
revoked.client write
EOF
stop_server
start_server --tls-crl "$work/clients.crl"
port="$(cat "$port_file")"

crl_key="$(printf 'crl-alive' | to_hex)"
crl_val="$(printf 'still' | to_hex)"
echo "  allowed client still works under --tls-crl"
put_sdk python "$port" "$crl_key" "$crl_val"
expect_get go "$port" "$crl_key" "$crl_val"

echo "  revoked.client must fail handshake / connect"
set +e
"$python" "$py_helper" --port "$port" \
  --tls --tls-ca "$work/ca.crt" \
  --tls-cert "$work/revoked.crt" --tls-key "$work/revoked.key" \
  --server-name localhost \
  --key-hex "$crl_key" --value-hex "$crl_val" put >/tmp/glyphastore-crl-put.err 2>&1
crl_rc=$?
set -e
if [[ "$crl_rc" -eq 0 ]]; then
  echo "revoked client PUT unexpectedly succeeded" >&2
  cat /tmp/glyphastore-crl-put.err >&2 || true
  exit 1
fi
echo "  revoked client rejected (exit=$crl_rc) OK"

echo "secure-profile interop PASSED (mTLS + authz + keyed + prefix + CRL; ${sdks[*]})"
