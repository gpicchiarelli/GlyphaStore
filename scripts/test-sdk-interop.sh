#!/usr/bin/env bash
# Cross-SDK interoperability matrix against a real volatile glyphastored.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3}"
perl="${PERL:-perl}"
daemon="${GLYPHASTORED:-}"
cpp_client="${GLYPHASTORE_INTEROP_CLIENT:-}"

prefer_bins=(
  "$root/build/macos-native-release"
  "$root/build/macos-release"
  "$root/build/macos-debug"
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
if [[ -z "$cpp_client" ]]; then
  for dir in "${prefer_bins[@]}"; do
    if [[ -x "$dir/glyphastore_interop_client" ]]; then
      cpp_client="$dir/glyphastore_interop_client"
      break
    fi
  done
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
chmod +x "$ruby_helper" 2>/dev/null || true
chmod +x "$py_helper" "$pl_helper" 2>/dev/null || true

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
done
echo "wire fixtures OK"

to_hex() {
  # stdin bytes -> hex; empty input -> empty string
  if command -v xxd >/dev/null 2>&1; then
    xxd -p -c 256 | tr -d '\n'
  else
    od -An -tx1 | tr -d ' \n'
  fi
}

put_sdk() {
  local sdk="$1" port="$2" key_hex="$3" value_hex="$4" expire="${5:-0}"
  case "$sdk" in
    cpp)
      "$cpp_client" --port "$port" put --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire"
      ;;
    python)
      "$python" "$py_helper" --port "$port" put --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire"
      ;;
    perl)
      "$perl" "$pl_helper" --port "$port" --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire" put
      ;;
    go)
      "$go_helper" --port "$port" --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire" put
      ;;
    ruby)
      "$ruby_bin" "$ruby_helper" --port "$port" --key-hex "$key_hex" --value-hex "$value_hex" --expire-at-ns "$expire" put
      ;;
    *)
      echo "unknown sdk $sdk" >&2
      return 1
      ;;
  esac
}

get_sdk() {
  local sdk="$1" port="$2" key_hex="$3"
  case "$sdk" in
    cpp) "$cpp_client" --port "$port" get --key-hex "$key_hex" ;;
    python) "$python" "$py_helper" --port "$port" get --key-hex "$key_hex" ;;
    perl) "$perl" "$pl_helper" --port "$port" --key-hex "$key_hex" get ;;
    go) "$go_helper" --port "$port" --key-hex "$key_hex" get ;;
    ruby) "$ruby_bin" "$ruby_helper" --port "$port" --key-hex "$key_hex" get ;;
    *) return 1 ;;
  esac
}

pipeline_sdk() {
  local sdk="$1" port="$2" key_hex="$3" value_hex="$4"
  case "$sdk" in
    cpp) "$cpp_client" --port "$port" pipeline-put-get --key-hex "$key_hex" --value-hex "$value_hex" ;;
    python) "$python" "$py_helper" --port "$port" pipeline-put-get --key-hex "$key_hex" --value-hex "$value_hex" ;;
    perl) "$perl" "$pl_helper" --port "$port" --key-hex "$key_hex" --value-hex "$value_hex" pipeline-put-get ;;
    go) "$go_helper" --port "$port" --key-hex "$key_hex" --value-hex "$value_hex" pipeline-put-get ;;
    ruby) "$ruby_bin" "$ruby_helper" --port "$port" --key-hex "$key_hex" --value-hex "$value_hex" pipeline-put-get ;;
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

expect_missing() {
  local sdk="$1" port="$2" key_hex="$3"
  if get_sdk "$sdk" "$port" "$key_hex" >/dev/null 2>&1; then
    echo "expected missing key for $sdk" >&2
    return 1
  fi
}

start_server() {
  local workers="$1"
  local port_file="$2"
  local log_file="$3"
  "$daemon" --bind 127.0.0.1 --port 0 --workers "$workers" \
    --storage-mode volatile --executor-affinity --quiet \
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

run_matrix_for_workers() {
  local workers="$1"
  local work
  work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-interop.XXXXXX")"
  local port_file="$work/port"
  local log_file="$work/server.log"
  echo "== interop workers=$workers =="
  start_server "$workers" "$port_file" "$log_file"
  local port
  port="$(cat "$port_file")"

  local writers=(cpp python perl go ruby)
  local readers=(cpp python perl go ruby)
  local case_id=0

  # Binary key/value cross-language PUT→GET.
  for writer in "${writers[@]}"; do
    for reader in "${readers[@]}"; do
      case_id=$((case_id + 1))
      local key_hex value_hex
      key_hex="$(printf 'interop-%02d-w%d\x00\xff' "$case_id" "$workers" | to_hex)"
      value_hex="$(printf 'val-%s-%s-\x00\xff' "$writer" "$reader" | to_hex)"
      echo "  $writer PUT → $reader GET (binary)"
      put_sdk "$writer" "$port" "$key_hex" "$value_hex"
      expect_get "$reader" "$port" "$key_hex" "$value_hex"
    done
  done

  # Empty value.
  local empty_key
  empty_key="$(printf 'empty-w%d' "$workers" | to_hex)"
  echo "  python PUT empty value → cpp/perl/go/ruby GET"
  put_sdk python "$port" "$empty_key" ""
  expect_get cpp "$port" "$empty_key" ""
  expect_get perl "$port" "$empty_key" ""
  expect_get go "$port" "$empty_key" ""
  expect_get ruby "$port" "$empty_key" ""

  # Pipeline put/get within each SDK.
  for sdk in cpp python perl go ruby; do
    local pkey pval
    pkey="$(printf 'pipe-%s-w%d' "$sdk" "$workers" | to_hex)"
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

  # Short expiry (Unix ns). Volatile Store must hide expired keys.
  local exp_key exp_val expire_at
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
  expect_missing perl "$port" "$exp_key"

  stop_server "$port_file"
  rm -rf "$work"
  echo "workers=$workers OK"
}

workers_list=(1 2 4)
if [[ "${INTEROP_WORKERS:-}" != "" ]]; then
  # shellcheck disable=SC2206
  workers_list=(${INTEROP_WORKERS})
fi

for w in "${workers_list[@]}"; do
  run_matrix_for_workers "$w"
done

echo "SDK interoperability matrix PASSED"
