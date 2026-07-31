#!/usr/bin/env bash
# Cross-SDK interoperability matrix against a real volatile glyphastored.
# Runs cleartext by default, then an opt-in TLS 1.3 matrix (Phase 2.4) when the
# daemon was built with TLS and openssl is available. Erlang is included in both
# matrices when OTP/rebar3 are available. Ruby ships the same TLS train as peers.
#
# Default routing is FNV (plain GlyphaStore/2 INIT). Keyed SipHash INIT decode +
# hash are covered by SDK unit tests; a --worker-hash-seed interop matrix is not
# required here yet (optional follow-up once harness wiring is convenient).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3}"
perl="${PERL:-perl}"
daemon="${GLYPHASTORED:-}"
cpp_client="${GLYPHASTORE_INTEROP_CLIENT:-}"

prefer_bins=(
  "$root/build/macos-debug"
  "$root/build/macos-release"
  "$root/build/macos-native-release"
  "$root/build/unix-release"
  "$root/build/unix-debug"
)

if [[ -z "$daemon" ]]; then
  for dir in "${prefer_bins[@]}"; do
    if [[ -x "$dir/glyphastored" ]]; then
      daemon="$dir/glyphastored"
      break
    fi
  done
fi
# Prefer a TLS-capable glyphastored when available (Phase 2.4 matrix).
if [[ -z "${GLYPHASTORED:-}" ]]; then
  for dir in "${prefer_bins[@]}"; do
    if [[ -x "$dir/glyphastored" ]] && "$dir/glyphastored" --help 2>&1 | grep -q -- '--tls-cert'; then
      daemon="$dir/glyphastored"
      break
    fi
  done
fi
if [[ -z "$cpp_client" ]]; then
  # Prefer interop client from the same build tree as the chosen daemon when possible.
  daemon_dir="$(dirname "$daemon")"
  if [[ -x "$daemon_dir/glyphastore_interop_client" ]]; then
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
  echo "missing glyphastored; build a preset that produces it first" >&2
  exit 1
fi
if [[ -z "$cpp_client" || ! -x "$cpp_client" ]]; then
  echo "missing glyphastore_interop_client; build target glyphastore_interop_client first" >&2
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
ruby_bin="${RUBY:-}"
if [[ -z "$ruby_bin" ]]; then
  if [[ -x "$HOME/.local/bin/mise" ]]; then
    ruby_bin="$("$HOME/.local/bin/mise" exec ruby@3.3 -- which ruby 2>/dev/null || true)"
  fi
fi
if [[ -z "$ruby_bin" ]]; then
  echo "missing Ruby >= 3.2 for interop (set RUBY= or install via mise)" >&2
  exit 1
fi
ruby_helper="$root/sdk/ruby/exe/glyphastore-interop"
export RUBYLIB="$root/sdk/ruby/lib${RUBYLIB:+:$RUBYLIB}"
erlang_helper="${GLYPHASTORE_ERLANG_INTEROP:-}"
erlang_ready=0
if [[ -z "$erlang_helper" ]]; then
  erlang_helper="$root/sdk/erlang/scripts/glyphastore-interop.escript"
fi
if command -v erl >/dev/null 2>&1 && command -v rebar3 >/dev/null 2>&1; then
  (cd "$root/sdk/erlang" && rebar3 compile >/dev/null)
  erlang_ready=1
elif [[ "${INTEROP_REQUIRE_ERLANG:-0}" == "1" ]]; then
  echo "missing Erlang/OTP and rebar3 for interop (set INTEROP_REQUIRE_ERLANG=0 to skip)" >&2
  exit 1
else
  echo "note: Erlang SDK interop skipped (install OTP + rebar3 to include erlang in matrix)" >&2
fi
chmod +x "$ruby_helper" "$py_helper" "$pl_helper" 2>/dev/null || true
chmod +x "$erlang_helper" 2>/dev/null || true

# Optional TLS client flags populated by run_matrix_for_workers when mode=tls.
tls_args=()

echo "== wire golden fixtures =="
"$python" "$root/scripts/generate_wire_fixtures.py" --verify "$root/tests/fixtures"
for fixture in wire_requests_v2.hex wire_responses_v2.hex; do
  if ! cmp -s "$root/tests/fixtures/$fixture" "$root/sdk/python/tests/fixtures/$fixture"; then
    echo "Python vendored fixture drift: $fixture (run scripts/sync-sdk-fixtures.sh)" >&2
    exit 1
  fi
  if ! cmp -s "$root/tests/fixtures/$fixture" "$root/sdk/perl/t/fixtures/$fixture"; then
    echo "Perl vendored fixture drift: $fixture (run scripts/sync-sdk-fixtures.sh)" >&2
    exit 1
  fi
  if ! cmp -s "$root/tests/fixtures/$fixture" "$root/sdk/go/testdata/$fixture"; then
    echo "Go vendored fixture drift: $fixture (run scripts/sync-sdk-fixtures.sh)" >&2
    exit 1
  fi
  if ! cmp -s "$root/tests/fixtures/$fixture" "$root/sdk/ruby/test/fixtures/$fixture"; then
    echo "Ruby vendored fixture drift: $fixture (run scripts/sync-sdk-fixtures.sh)" >&2
    exit 1
  fi
  if ! cmp -s "$root/tests/fixtures/$fixture" "$root/sdk/erlang/test/fixtures/$fixture"; then
    echo "Erlang vendored fixture drift: $fixture (run scripts/sync-sdk-fixtures.sh)" >&2
    exit 1
  fi
done
echo "wire fixtures OK"

to_hex() {
  if command -v xxd >/dev/null 2>&1; then
    xxd -p -c 256 | tr -d '\n'
  else
    od -An -tx1 | tr -d ' \n'
  fi
}

put_sdk() {
  local sdk="$1" port="$2" key_hex="$3" value_hex="$4" expire="${5:-0}"
  local extra=()
  if [[ ${#tls_args[@]} -gt 0 ]]; then
    extra=("${tls_args[@]}")
  fi
  case "$sdk" in
    cpp)
      "$cpp_client" --port "$port" "${extra[@]+"${extra[@]}"}" put --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire"
      ;;
    python)
      "$python" "$py_helper" --port "$port" "${extra[@]+"${extra[@]}"}" put --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire"
      ;;
    perl)
      "$perl" "$pl_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire" put
      ;;
    go)
      "$go_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire" put
      ;;
    ruby)
      "$ruby_bin" "$ruby_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire" put
      ;;
    erlang)
      escript "$erlang_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire" put
      ;;
    *)
      echo "unknown sdk $sdk" >&2
      return 1
      ;;
  esac
}

get_sdk() {
  local sdk="$1" port="$2" key_hex="$3"
  local extra=()
  if [[ ${#tls_args[@]} -gt 0 ]]; then
    extra=("${tls_args[@]}")
  fi
  case "$sdk" in
    cpp) "$cpp_client" --port "$port" "${extra[@]+"${extra[@]}"}" get --key-hex "$key_hex" ;;
    python) "$python" "$py_helper" --port "$port" "${extra[@]+"${extra[@]}"}" get --key-hex "$key_hex" ;;
    perl) "$perl" "$pl_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" get ;;
    go) "$go_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" get ;;
    ruby) "$ruby_bin" "$ruby_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" get ;;
    erlang) escript "$erlang_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" get ;;
    *)
      echo "unknown sdk: $sdk" >&2
      return 1
      ;;
  esac
}

pipeline_sdk() {
  local sdk="$1" port="$2" key_hex="$3" value_hex="$4"
  local extra=()
  if [[ ${#tls_args[@]} -gt 0 ]]; then
    extra=("${tls_args[@]}")
  fi
  case "$sdk" in
    cpp) "$cpp_client" --port "$port" "${extra[@]+"${extra[@]}"}" pipeline-put-get --key-hex "$key_hex" --value-hex "$value_hex" ;;
    python) "$python" "$py_helper" --port "$port" "${extra[@]+"${extra[@]}"}" pipeline-put-get --key-hex "$key_hex" --value-hex "$value_hex" ;;
    perl) "$perl" "$pl_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" --value-hex "$value_hex" pipeline-put-get ;;
    go) "$go_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" --value-hex "$value_hex" pipeline-put-get ;;
    ruby) "$ruby_bin" "$ruby_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" --value-hex "$value_hex" pipeline-put-get ;;
    erlang) escript "$erlang_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" --value-hex "$value_hex" pipeline-put-get ;;
    *) return 1 ;;
  esac
}

expect_get() {
  local sdk="$1" port="$2" key_hex="$3" want_hex="$4"
  local got
  got="$(get_sdk "$sdk" "$port" "$key_hex" | tr -d '\n')"
  if [[ "$got" != "$want_hex" ]]; then
    echo "GET mismatch ($sdk): got='$got' want='$want_hex'" >&2
    return 1
  fi
}

expect_not_found_sdk() {
  local sdk="$1" port="$2" key_hex="$3"
  local extra=()
  if [[ ${#tls_args[@]} -gt 0 ]]; then
    extra=("${tls_args[@]}")
  fi
  case "$sdk" in
    cpp) "$cpp_client" --port "$port" "${extra[@]+"${extra[@]}"}" expect-not-found --key-hex "$key_hex" ;;
    python) "$python" "$py_helper" --port "$port" "${extra[@]+"${extra[@]}"}" expect-not-found --key-hex "$key_hex" ;;
    perl) "$perl" "$pl_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" expect-not-found ;;
    go) "$go_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" expect-not-found ;;
    ruby) "$ruby_bin" "$ruby_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" expect-not-found ;;
    erlang) escript "$erlang_helper" --port "$port" "${extra[@]+"${extra[@]}"}" --key-hex "$key_hex" expect-not-found ;;
    *) return 1 ;;
  esac
}

expect_frame_limit_sdk() {
  local sdk="$1" port="$2"
  local extra=()
  if [[ ${#tls_args[@]} -gt 0 ]]; then
    extra=("${tls_args[@]}")
  fi
  case "$sdk" in
    cpp) "$cpp_client" --port "$port" "${extra[@]+"${extra[@]}"}" expect-frame-limit ;;
    python) "$python" "$py_helper" --port "$port" "${extra[@]+"${extra[@]}"}" expect-frame-limit ;;
    perl) "$perl" "$pl_helper" --port "$port" "${extra[@]+"${extra[@]}"}" expect-frame-limit ;;
    go) "$go_helper" --port "$port" "${extra[@]+"${extra[@]}"}" expect-frame-limit ;;
    ruby) "$ruby_bin" "$ruby_helper" --port "$port" "${extra[@]+"${extra[@]}"}" expect-frame-limit ;;
    erlang) escript "$erlang_helper" --port "$port" "${extra[@]+"${extra[@]}"}" expect-frame-limit ;;
    *) return 1 ;;
  esac
}

make_tls_material() {
  local directory="$1"
  local key="$directory/server.key"
  local cert="$directory/server.crt"
  if ! command -v openssl >/dev/null 2>&1; then
    return 1
  fi
  # SAN covers loopback IP + localhost SNI used by clients.
  if openssl req -x509 -newkey rsa:2048 -nodes \
      -keyout "$key" -out "$cert" -days 1 \
      -subj "/CN=localhost" \
      -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
      >/dev/null 2>&1; then
    return 0
  fi
  # LibreSSL / older openssl without -addext.
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$key" -out "$cert" -days 1 \
    -subj "/CN=localhost" >/dev/null 2>&1
}

daemon_supports_tls() {
  "$daemon" --help 2>&1 | grep -q -- '--tls-cert'
}

start_server() {
  local workers="$1"
  local port_file="$2"
  local log_file="$3"
  shift 3
  local resolved
  resolved="$("$daemon" --dump-config --workers "$workers" --max-connections 4096)"
  if ! grep -qx "workers=$workers" <<<"$resolved" ||
      ! grep -qx "max-connections=4096" <<<"$resolved"; then
    echo "glyphastored configuration mismatch for workers=$workers" >&2
    echo "$resolved" >&2
    return 1
  fi
  "$daemon" --bind 127.0.0.1 --port 0 --workers "$workers" \
    --storage-mode volatile --executor-affinity --quiet \
    "$@" \
    >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"${port_file}.pid"
  local port=""
  for _ in $(seq 1 50); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "glyphastored exited early; see $log_file" >&2
      return 1
    fi
    port="$(lsof -nP -iTCP -sTCP:LISTEN -a -p "$pid" 2>/dev/null | awk 'NR==2 {split($9,a,":"); print a[length(a)]}')"
    if [[ -n "$port" ]]; then
      echo "$port" >"$port_file"
      return 0
    fi
    sleep 0.1
  done
  echo "could not discover glyphastored listen port" >&2
  return 1
}

stop_server() {
  local port_file="$1"
  if [[ -f "${port_file}.pid" ]]; then
    local pid
    pid="$(cat "${port_file}.pid")"
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    rm -f "${port_file}.pid" "$port_file"
  fi
}

active_port_file=""
active_work=""
cleanup_active_run() {
  if [[ -n "$active_port_file" ]]; then
    stop_server "$active_port_file"
  fi
  if [[ -n "$active_work" ]]; then
    rm -rf "$active_work"
  fi
}
trap cleanup_active_run EXIT

run_matrix_for_workers() {
  local workers="$1"
  local mode="${2:-cleartext}" # cleartext | tls
  local work
  work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-interop.XXXXXX")"
  local port_file="$work/port"
  local log_file="$work/server.log"
  active_port_file="$port_file"
  active_work="$work"
  local server_extra=()
  tls_args=()

  local writers=(cpp python perl go ruby)
  local readers=(cpp python perl go ruby)
  if [[ "$erlang_ready" == "1" ]]; then
    # Erlang ships TLS 1.3 (Phase 2); include in both cleartext and TLS matrices.
    writers+=(erlang)
    readers+=(erlang)
  fi

  echo "== interop mode=$mode workers=$workers =="
  if [[ "$mode" == "tls" ]]; then
    if ! make_tls_material "$work"; then
      echo "skipping TLS interop: openssl could not mint a test certificate" >&2
      rm -rf "$work"
      return 0
    fi
    server_extra=(--tls-cert "$work/server.crt" --tls-key "$work/server.key")
    tls_args=(--tls --tls-ca "$work/server.crt" --server-name localhost)
    if ! "$perl" -MIO::Socket::SSL -e1 >/dev/null 2>&1; then
      echo "  note: Perl without IO::Socket::SSL — excluding perl from TLS matrix"
      local filtered_w=()
      local filtered_r=()
      local sdk
      for sdk in "${writers[@]}"; do
        [[ "$sdk" == "perl" ]] && continue
        filtered_w+=("$sdk")
      done
      for sdk in "${readers[@]}"; do
        [[ "$sdk" == "perl" ]] && continue
        filtered_r+=("$sdk")
      done
      writers=("${filtered_w[@]}")
      readers=("${filtered_r[@]}")
    fi
  fi

  if [[ ${#server_extra[@]} -gt 0 ]]; then
    start_server "$workers" "$port_file" "$log_file" "${server_extra[@]}"
  else
    start_server "$workers" "$port_file" "$log_file"
  fi
  local port
  port="$(cat "$port_file")"

  local case_id=0
  for writer in "${writers[@]}"; do
    for reader in "${readers[@]}"; do
      case_id=$((case_id + 1))
      local key_hex value_hex
      key_hex="$(printf 'interop-%s-%02d-w%d\x00\xff' "$mode" "$case_id" "$workers" | to_hex)"
      value_hex="$(printf 'val-%s-%s-%s-\x00\xff' "$mode" "$writer" "$reader" | to_hex)"
      echo "  $writer PUT → $reader GET (binary)"
      put_sdk "$writer" "$port" "$key_hex" "$value_hex"
      expect_get "$reader" "$port" "$key_hex" "$value_hex"
    done
  done

  local empty_key
  empty_key="$(printf 'empty-%s-w%d' "$mode" "$workers" | to_hex)"
  echo "  python PUT empty value → cross-SDK GET"
  put_sdk python "$port" "$empty_key" ""
  for sdk in "${readers[@]}"; do
    [[ "$sdk" == python ]] && continue
    expect_get "$sdk" "$port" "$empty_key" ""
  done

  while IFS=: read -r owner route_key; do
    local route_value
    route_value="$(printf 'owner-%s-of-%s' "$owner" "$workers" | to_hex)"
    echo "  cpp PUT → python GET (deterministic owner $owner)"
    put_sdk cpp "$port" "$route_key" "$route_value"
    expect_get python "$port" "$route_key" "$route_value"
  done < <("$python" - "$workers" <<'PY'
import sys

workers = int(sys.argv[1])
found = {}
candidate = 0
while len(found) < workers:
    key = f"route-w{workers}-{candidate}".encode()
    value = 14695981039346656037
    for byte in key:
        value = ((value ^ byte) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    found.setdefault(value % workers, key.hex())
    candidate += 1
for owner in range(workers):
    print(f"{owner}:{found[owner]}")
PY
)

  for sdk in "${writers[@]}"; do
    local pkey pval
    pkey="$(printf 'pipe-%s-%s-w%d' "$mode" "$sdk" "$workers" | to_hex)"
    pval="$(printf 'pipe-val-\xff' | to_hex)"
    echo "  $sdk pipeline PUT/GET"
    local got
    got="$(pipeline_sdk "$sdk" "$port" "$pkey" "$pval" | tr -d '\n')"
    if [[ "$got" != "$pval" ]]; then
      echo "pipeline mismatch for $sdk" >&2
      stop_server "$port_file"
      rm -rf "$work"
      return 1
    fi
  done

  if [[ "$mode" == "cleartext" ]]; then
    local missing_key exp_key exp_val expire_at
    missing_key="$(printf 'missing-%s-w%d' "$mode" "$workers" | to_hex)"
    for sdk in "${readers[@]}"; do
      echo "  $sdk structured NOT_FOUND"
      expect_not_found_sdk "$sdk" "$port" "$missing_key"
    done
    for sdk in "${writers[@]}"; do
      echo "  $sdk local 2MiB frame-limit rejection"
      expect_frame_limit_sdk "$sdk" "$port"
    done

    exp_key="$(printf 'exp-w%d' "$workers" | to_hex)"
    exp_val="$(printf 'soon' | to_hex)"
    expire_at="$("$python" - <<'PY'
import time
print(int((time.time() + 0.4) * 1_000_000_000))
PY
)"
    echo "  cpp PUT with expiry → go GET then miss"
    put_sdk cpp "$port" "$exp_key" "$exp_val" "$expire_at"
    expect_get go "$port" "$exp_key" "$exp_val"
    sleep 0.6
    expect_not_found_sdk perl "$port" "$exp_key"
  fi

  stop_server "$port_file"
  rm -rf "$work"
  active_port_file=""
  active_work=""
  echo "mode=$mode workers=$workers OK"
}

workers_list=(1 2 4 8)
if [[ "${INTEROP_WORKERS:-}" != "" ]]; then
  # shellcheck disable=SC2206
  workers_list=(${INTEROP_WORKERS})
fi

for w in "${workers_list[@]}"; do
  run_matrix_for_workers "$w" cleartext
done

if [[ "${INTEROP_SKIP_TLS:-0}" == "1" ]]; then
  echo "TLS interop skipped (INTEROP_SKIP_TLS=1)"
elif ! daemon_supports_tls; then
  echo "TLS interop skipped (daemon built without --tls-cert)"
else
  # Keep TLS matrix smaller by default; INTEROP_TLS_WORKERS overrides.
  tls_workers=(1 2)
  if [[ "${INTEROP_TLS_WORKERS:-}" != "" ]]; then
    # shellcheck disable=SC2206
    tls_workers=(${INTEROP_TLS_WORKERS})
  fi
  for w in "${tls_workers[@]}"; do
    run_matrix_for_workers "$w" tls
  done
fi

echo "SDK interoperability matrix PASSED"
