#!/usr/bin/env bash
# CI-friendly durable daemon soak/stress entry point.
#
# Default (smoke): ~45s of PUT/GET, reconnect, compact-friendly churn, and
# graceful drain. Suitable for PR/nightly labeled CI.
#
# Long soak: set SOAK_SECONDS (e.g. 3600) or pass --seconds N. Multi-hour
# hardware matrices remain a release gate outside this script.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
prefer_bins=(
  "$root/build/macos-debug"
  "$root/build/macos-release"
  "$root/build/macos-native-release"
  "$root/build/unix-debug"
  "$root/build/unix-release"
)

seconds="${SOAK_SECONDS:-45}"
workers=1
storage_mode=durable-periodic
while [[ $# -gt 0 ]]; do
  case "$1" in
    --seconds)
      seconds="$2"
      shift 2
      ;;
    --workers)
      workers="$2"
      shift 2
      ;;
    --storage-mode)
      storage_mode="$2"
      shift 2
      ;;
    -h|--help)
      cat <<EOF
Usage: $0 [--seconds N] [--workers N] [--storage-mode MODE]
Env: SOAK_SECONDS, GLYPHASTORED, GLYPHASTORE_INTEROP_CLIENT
EOF
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

resolve_bin() {
  local name="$1"
  local override="$2"
  if [[ -n "$override" && -x "$override" ]]; then
    printf '%s\n' "$override"
    return 0
  fi
  for dir in "${prefer_bins[@]}"; do
    if [[ -x "$dir/$name" ]]; then
      printf '%s\n' "$dir/$name"
      return 0
    fi
  done
  return 1
}

discover_port() {
  local pid="$1"
  lsof -nP -iTCP -sTCP:LISTEN -a -p "$pid" 2>/dev/null |
    awk 'NR==2 {split($9,a,":"); print a[length(a)]}'
}

to_hex() {
  if command -v xxd >/dev/null 2>&1; then
    xxd -p -c 256 | tr -d '\n'
  else
    od -An -tx1 | tr -d ' \n'
  fi
}

daemon="$(resolve_bin glyphastored "${GLYPHASTORED:-}" || true)"
client="$(resolve_bin glyphastore_interop_client "${GLYPHASTORE_INTEROP_CLIENT:-}" || true)"
if [[ -z "$daemon" || -z "$client" ]]; then
  echo "missing glyphastored or glyphastore_interop_client" >&2
  exit 1
fi
if ! command -v lsof >/dev/null 2>&1; then
  echo "lsof is required" >&2
  exit 1
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-soak-XXXXXX")"
daemon_pid=""
cleanup() {
  if [[ -n "$daemon_pid" ]] && kill -0 "$daemon_pid" 2>/dev/null; then
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

data_dir="$work/data"
"$daemon" --quiet --bind 127.0.0.1 --port 0 --workers "$workers" \
  --storage-mode "$storage_mode" --data-dir "$data_dir" --open-mode create-new \
  --shutdown-drain-ms 5000 --maintenance-mode background \
  >"$work/daemon.out" 2>"$work/daemon.err" &
daemon_pid=$!

port=""
for _ in $(seq 1 100); do
  port="$(discover_port "$daemon_pid" || true)"
  if [[ -n "$port" ]]; then
    break
  fi
  if ! kill -0 "$daemon_pid" 2>/dev/null; then
    echo "soak daemon exited early:" >&2
    cat "$work/daemon.out" "$work/daemon.err" >&2 || true
    exit 1
  fi
  sleep 0.05
done
if [[ -z "$port" ]]; then
  echo "failed to discover soak daemon port" >&2
  exit 1
fi

echo "soak start seconds=$seconds workers=$workers storage=$storage_mode port=$port"
deadline=$((SECONDS + seconds))
ops=0
reconnects=0
while (( SECONDS < deadline )); do
  key="soak-key-$ops"
  value="soak-value-$ops-$(date +%s)"
  key_hex="$(printf '%s' "$key" | to_hex)"
  value_hex="$(printf '%s' "$value" | to_hex)"
  if ! "$client" --host 127.0.0.1 --port "$port" put --key-hex "$key_hex" --value-hex "$value_hex" >/dev/null; then
    echo "PUT failed at op=$ops" >&2
    exit 1
  fi
  if ! "$client" --host 127.0.0.1 --port "$port" get --key-hex "$key_hex" >/dev/null; then
    echo "GET failed at op=$ops" >&2
    exit 1
  fi
  ops=$((ops + 1))
  # Periodic reconnect stress: open a fresh client path every 25 ops.
  if (( ops % 25 == 0 )); then
    reconnects=$((reconnects + 1))
    # Force a new TCP session by invoking the client again (each invocation connects).
    "$client" --host 127.0.0.1 --port "$port" get --key-hex "$key_hex" >/dev/null
  fi
  # Overwrite churn to create reclaimable sealed history for background vacuum.
  if (( ops % 7 == 0 )); then
    overwrite_hex="$(printf 'overwrite-%s' "$ops" | to_hex)"
    "$client" --host 127.0.0.1 --port "$port" put --key-hex "$key_hex" --value-hex "$overwrite_hex" >/dev/null
  fi
done

PYTHONPATH="$root/sdk/python/src${PYTHONPATH:+:$PYTHONPATH}" python3 - "$port" <<'PY'
import socket
import struct
import sys
from glyphastore.protocol import Opcode, Status, encode_request, decode_response

port = int(sys.argv[1])
frame = encode_request(Opcode.STATS, 42)
with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
    sock.sendall(frame)
    buf = b""
    while len(buf) < 40:
        chunk = sock.recv(4096)
        if not chunk:
            raise SystemExit("short STATS response")
        buf += chunk
    frame_size = struct.unpack_from("<I", buf, 0)[0]
    while len(buf) < frame_size:
        chunk = sock.recv(frame_size - len(buf))
        if not chunk:
            break
        buf += chunk
    response = decode_response(buf)
    if response.status != Status.OK:
        raise SystemExit(f"STATS status={response.status}")
    text = response.value.decode("ascii", errors="replace")
    required = (
        "lane[0].queue_wait_ns.count=",
        "lane[0].service_ns.le_inf=",
        "maintenance_rate_window_bytes_copied=",
        "useful_compactions=",
    )
    for needle in required:
        if needle not in text:
            raise SystemExit(f"STATS missing {needle}")
    print("soak STATS ok")
PY

kill -TERM "$daemon_pid"
wait "$daemon_pid"
daemon_pid=""
echo "soak ok ops=$ops reconnects=$reconnects seconds=$seconds"
