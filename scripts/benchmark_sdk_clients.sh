#!/usr/bin/env bash
# Complete native SDK client benchmark suite against an external glyphastored.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stamp="$(date -u +%Y%m%d-%H%M%S)"
sdk_version="0.1.0"
outdir="${1:-$root/benchmark-results-sdk-${sdk_version}-${stamp}}"
daemon="${GLYPHASTORED:-$root/build/macos-native-release/glyphastored}"
python="${PYTHON:-python3}"
perl="${PERL:-perl}"
host="127.0.0.1"
ops="${OPS:-100000}"
warmup="${WARMUP:-1}"
repeats="${REPEATS:-7}"

if [[ ! -x "$daemon" ]]; then
  echo "missing glyphastored at $daemon; build macos-native-release first" >&2
  exit 1
fi

mkdir -p "$outdir/python" "$outdir/perl" "$outdir/go" "$outdir/logs"
export PYTHONPATH="$root/sdk/python/src"
export PERL5LIB="$root/sdk/perl/lib${PERL5LIB:+:$PERL5LIB}"
go_bin="${GO:-go}"
mkdir -p "$root/sdk/go/bin"
(cd "$root/sdk/go" && "$go_bin" build -o bin/glyphastore-bench ./cmd/glyphastore-bench)
go_bench="$root/sdk/go/bin/glyphastore-bench"

capture_environment() {
  {
    echo "captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_sha=$(git -C "$root" rev-parse HEAD)"
    echo "git_status=$(git -C "$root" status --porcelain | wc -l | tr -d ' ') dirty paths"
    echo "kernel=$(uname -a)"
    echo "hardware=$(uname -m)"
    if command -v sw_vers >/dev/null 2>&1; then
      echo "product=$(sw_vers -productName) $(sw_vers -productVersion) ($(sw_vers -buildVersion))"
    fi
    if command -v sysctl >/dev/null 2>&1; then
      echo "cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
      echo "logical_cpu=$(sysctl -n hw.logicalcpu 2>/dev/null || true)"
      echo "physical_cpu=$(sysctl -n hw.physicalcpu 2>/dev/null || true)"
      echo "memory_bytes=$(sysctl -n hw.memsize 2>/dev/null || true)"
    fi
    echo "python=$($python - <<'PY'
import sys
print(sys.version.replace("\n", " "))
PY
)"
    echo "python_sdk_version=$($python -c 'import glyphastore; print(glyphastore.__version__)')"
    echo "perl=$($perl -V:version -V:archname | tr '\n' ' ')"
    echo "perl_sdk_version=$($perl -MGlyphaStore -e 'print \$GlyphaStore::VERSION')"
    echo "go=$($go_bin version)"
    echo "go_sdk_version=0.1.0"
    echo "glyphastored=$daemon"
    echo "glyphastored_version=$("$daemon" --version 2>&1 | tr '\n' ' ')"
    echo "ops=$ops warmup=$warmup repeats=$repeats"
    echo "workload=ordered PUT/GET pipeline read-after-write, value_size=64"
    echo "storage_mode=volatile"
    echo "note=same-host loopback; load generator and server share the machine"
  } >"$outdir/environment.txt"
}

start_server() {
  local workers="$1"
  local port_file="$2"
  local log_file="$3"
  "$daemon" \
    --bind "$host" \
    --port 0 \
    --workers "$workers" \
    --storage-mode volatile \
    --executor-affinity \
    --quiet \
    >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"${port_file}.pid"
  # glyphastored with --port 0 prints nothing when quiet; discover via lsof.
  local port=""
  for _ in $(seq 1 50); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "glyphastored exited early; see $log_file" >&2
      cat "$log_file" >&2 || true
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
  kill "$pid" 2>/dev/null || true
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

run_matrix() {
  local workers pipelines
  workers=(1 2 4)
  pipelines=(1 8 32 128)

  {
    echo "# SDK client benchmark commands"
    echo
    echo "Generated under \`$outdir\`."
    echo
    echo "- SDK version: \`$sdk_version\`"
    echo "- Ops (PUT/GET pairs): \`$ops\`"
    echo "- Warmup/repeats: \`$warmup\` / \`$repeats\`"
    echo "- Server: volatile \`glyphastored\` with \`--executor-affinity\`"
    echo
  } >"$outdir/commands.md"

  for w in "${workers[@]}"; do
    local port_file="$outdir/logs/server-w${w}.port"
    local log_file="$outdir/logs/server-w${w}.log"
    start_server "$w" "$port_file" "$log_file"
    local port
    port="$(cat "$port_file")"
    echo "- workers=$w listen=$host:$port" >>"$outdir/commands.md"

    for p in "${pipelines[@]}"; do
      local label="w${w}-p${p}"
      echo "running python sync concurrent $label"
      "$python" "$root/sdk/python/benchmarks/client_benchmark.py" \
        --host "$host" --port "$port" --workers "$w" --ops "$ops" \
        --pipeline "$p" --warmup "$warmup" --repeats "$repeats" \
        --runtime sync --execution concurrent \
        | tee "$outdir/python/sync-concurrent-${label}.txt"

      echo "running python sync sequential $label"
      "$python" "$root/sdk/python/benchmarks/client_benchmark.py" \
        --host "$host" --port "$port" --workers "$w" --ops "$ops" \
        --pipeline "$p" --warmup "$warmup" --repeats "$repeats" \
        --runtime sync --execution sequential \
        | tee "$outdir/python/sync-sequential-${label}.txt"

      echo "running python async $label"
      "$python" "$root/sdk/python/benchmarks/client_benchmark.py" \
        --host "$host" --port "$port" --workers "$w" --ops "$ops" \
        --pipeline "$p" --warmup "$warmup" --repeats "$repeats" \
        --runtime async \
        | tee "$outdir/python/async-concurrent-${label}.txt"

      echo "running perl sequential $label"
      "$perl" "$root/sdk/perl/benchmarks/client_benchmark.pl" \
        --host "$host" --port "$port" --workers "$w" --ops "$ops" \
        --pipeline "$p" --warmup "$warmup" --repeats "$repeats" \
        --no-concurrent \
        | tee "$outdir/perl/sequential-${label}.txt"

      if [[ "$w" -gt 1 ]]; then
        echo "running perl concurrent $label"
        "$perl" "$root/sdk/perl/benchmarks/client_benchmark.pl" \
          --host "$host" --port "$port" --workers "$w" --ops "$ops" \
          --pipeline "$p" --warmup "$warmup" --repeats "$repeats" \
          --concurrent \
          | tee "$outdir/perl/concurrent-${label}.txt"
      fi

      echo "running go concurrent $label"
      "$go_bench" --host "$host" --port "$port" --workers "$w" --ops "$ops" \
        --pipeline "$p" --warmup "$warmup" --repeats "$repeats" --execution concurrent \
        | tee "$outdir/go/concurrent-${label}.txt"

      echo "running go sequential $label"
      "$go_bench" --host "$host" --port "$port" --workers "$w" --ops "$ops" \
        --pipeline "$p" --warmup "$warmup" --repeats "$repeats" --execution sequential \
        | tee "$outdir/go/sequential-${label}.txt"
    done

    stop_server "$port_file"
  done
}

write_summary() {
  "$python" - "$outdir" "$sdk_version" <<'PY'
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

outdir = Path(sys.argv[1])
sdk_version = sys.argv[2]
pattern = re.compile(
    r"name=(?P<name>\S+)\s+"
    r"sdk_version=(?P<sdk_version>\S+)\s+"
    r"runtime=(?P<runtime>\S+)\s+"
    r"execution=(?P<execution>\S+)\s+"
    r"workers=(?P<workers>\d+)\s+"
    r"pipeline_pairs=(?P<pipeline_pairs>\d+)\s+"
    r"operations=(?P<operations>\d+)\s+"
    r"samples=(?P<samples>\d+)\s+"
    r"median_seconds=(?P<median_seconds>[0-9.eE+-]+)\s+"
    r"min_seconds=(?P<min_seconds>[0-9.eE+-]+)\s+"
    r"max_seconds=(?P<max_seconds>[0-9.eE+-]+)\s+"
    r"median_ops_per_second=(?P<median_ops_per_second>[0-9.eE+-]+)\s+"
    r"min_ops_per_second=(?P<min_ops_per_second>[0-9.eE+-]+)\s+"
    r"max_ops_per_second=(?P<max_ops_per_second>[0-9.eE+-]+)"
)

rows = []
for path in (
    sorted(outdir.glob("python/*.txt"))
    + sorted(outdir.glob("perl/*.txt"))
    + sorted(outdir.glob("go/*.txt"))
):
    text = path.read_text(encoding="utf-8")
    match = pattern.search(text)
    if not match:
        raise SystemExit(f"could not parse result line in {path}")
    row = match.groupdict()
    row["file"] = str(path.relative_to(outdir))
    for key in (
        "workers",
        "pipeline_pairs",
        "operations",
        "samples",
    ):
        row[key] = int(row[key])
    for key in (
        "median_seconds",
        "min_seconds",
        "max_seconds",
        "median_ops_per_second",
        "min_ops_per_second",
        "max_ops_per_second",
    ):
        row[key] = float(row[key])
    rows.append(row)

(outdir / "results.json").write_text(
    json.dumps({"sdk_version": sdk_version, "results": rows}, indent=2) + "\n",
    encoding="utf-8",
)

lines = [
    f"# GlyphaStore SDK client benchmarks — version `{sdk_version}`",
    "",
    f"Parsed `{len(rows)}` result files from `{outdir.name}`.",
    "",
    "Workload: validated ordered `PUT`/`GET` pipeline read-after-write, value size 64 bytes,",
    "volatile `glyphastored`, same-host loopback. Median ops/s is the comparison statistic.",
    "",
    "| SDK | Runtime | Execution | Workers | Pipeline pairs | Median ops/s | Min ops/s | Max ops/s | Median s |",
    "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
]
for row in rows:
    if row["file"].startswith("python/"):
        sdk = "Python"
    elif row["file"].startswith("perl/"):
        sdk = "Perl"
    else:
        sdk = "Go"
    lines.append(
        "| {sdk} | {runtime} | {execution} | {workers} | {pipeline_pairs} | {median_ops_per_second:,.0f} | "
        "{min_ops_per_second:,.0f} | {max_ops_per_second:,.0f} | {median_seconds:.6f} |".format(
            sdk=sdk, **row
        )
    )
lines.extend(
    [
        "",
        "## Notes",
        "",
        "- Python `concurrent` uses one OS thread per Worker against one shared `Client`.",
        "- Python `async` uses one `asyncio` task per Worker against one shared `AsyncClient`.",
        "- Python `sequential`, Perl sequential, and Go `sequential` drain Workers one after another.",
        "- Go `concurrent` uses one goroutine per Worker against one shared `Client`.",
        "- Perl has no shared-client multi-threaded mode; ithreads are not used.",
        "- Do not treat same-host loopback numbers as production capacity.",
        "",
    ]
)
(outdir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
print(f"wrote {outdir / 'summary.md'} and {outdir / 'results.json'}")
PY
}

write_readme() {
  cat >"$outdir/README.md" <<EOF
# GlyphaStore SDK benchmarks — ${sdk_version}

Published client-side pipeline benchmarks for the native Python, Perl, and Go SDKs at version
\`${sdk_version}\`.

## Contents

| Path | Purpose |
| --- | --- |
| \`environment.txt\` | Host, toolchain, SDK, and daemon metadata |
| \`commands.md\` | Workload matrix and listen ports |
| \`summary.md\` | Comparison table (median ops/s) |
| \`results.json\` | Machine-readable parsed results |
| \`python/\` | Raw Python sync/async result files |
| \`perl/\` | Raw Perl result files |
| \`go/\` | Raw Go result files |
| \`logs/\` | Server stdout/stderr |

## How to reproduce

\`\`\`bash
./scripts/benchmark_sdk_clients.sh
\`\`\`

Go-only:

\`\`\`bash
./scripts/benchmark_go_client.sh
./scripts/benchmark_perl_client.sh
\`\`\`

Optional overrides: \`OPS\`, \`WARMUP\`, \`REPEATS\`, \`GLYPHASTORED\`, \`PYTHON\`, \`PERL\`, \`GO\`.
EOF
}

cleanup() {
  for pidfile in "$outdir"/logs/*.pid; do
    [[ -f "$pidfile" ]] || continue
    kill "$(cat "$pidfile")" 2>/dev/null || true
  done
}
trap cleanup EXIT

capture_environment
write_readme
run_matrix
write_summary
trap - EXIT
echo "SDK benchmarks published under $outdir"
