#!/usr/bin/env bash
# Paired-shards Linux hard-pinned A/B orchestrator (Phase 2 P1).
# macOS advisory runs are allowed for harness smoke only and MUST be marked
# inconclusive; they do not close the gate.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
commit="$(git -C "$root" rev-parse --short HEAD)"
dirty=""
if ! git -C "$root" diff --quiet || ! git -C "$root" diff --cached --quiet; then
  dirty="-dirty"
fi

platform="linux"
if [[ "$(uname -s)" == "Darwin" ]]; then
  platform="macos-advisory"
fi

run_id="${RUN_ID:-${stamp}-paired-linux-ab}"
output="${OUTPUT_DIR:-$root/benchmark-results/paired-shards/${commit}${dirty}/${platform}/${run_id}}"
preset="${BENCHMARK_PRESET:-}"
if [[ -z "$preset" ]]; then
  if [[ "$(uname -s)" == "Darwin" ]]; then
    preset="macos-release"
  else
    preset="unix-release"
  fi
fi
cmake_bin="${CMAKE:-cmake}"
if [[ -x /Library/Perl/5.34/darwin-thread-multi-2level/auto/share/dist/Alien-cmake3/bin/cmake ]]; then
  cmake_bin="${CMAKE:-/Library/Perl/5.34/darwin-thread-multi-2level/auto/share/dist/Alien-cmake3/bin/cmake}"
fi

old_bin="${OLD_BIN:-}"
new_bin="${NEW_BIN:-}"
pairs_list="${PAIRS:-1 2 4 8}"
pipelines="${PIPELINES:-1 8 32}"
value_sizes="${VALUE_SIZES:-64}"
workloads="${WORKLOADS:-get-only read-after-write read-99-write-1}"
storage_modes="${STORAGE_MODES:-volatile}"
ops="${BENCHMARK_OPS:-100000}"
warmup="${BENCHMARK_WARMUP:-1}"
repeats="${BENCHMARK_REPEATS:-7}"
clients_per_pair="${CLIENTS_PER_PAIR:-1}"
require_linux_tools="${REQUIRE_LINUX_TOOLS:-1}"
server_cpu_list="${SERVER_CPUS:-}"
client_cpu_list="${CLIENT_CPUS:-}"
numa_node="${NUMA_NODE:-}"

mkdir -p "$output"/{samples,perf,numa,topology}
status="pass-candidate"
notes=()

if [[ "$(uname -s)" != "Linux" ]]; then
  status="inconclusive-not-linux-hard-pinned"
  notes+=("host=$(uname -s): hard pin/perf/NUMA gate cannot close here")
  require_linux_tools=0
fi

need() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    if [[ "$require_linux_tools" == "1" ]]; then
      echo "missing required tool: $cmd" >&2
      exit 2
    fi
    notes+=("missing-tool=$cmd")
    status="inconclusive-missing-tools"
    return 1
  fi
  return 0
}

need_linux() {
  [[ "$(uname -s)" == "Linux" ]] || return 1
  need "$1"
}

{
  echo "generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_sha=$(git -C "$root" rev-parse HEAD)"
  echo "git_describe=$(git -C "$root" describe --always --dirty)"
  echo "preset=$preset"
  echo "pairs=$pairs_list"
  echo "pipelines=$pipelines"
  echo "value_sizes=$value_sizes"
  echo "workloads=$workloads"
  echo "storage_modes=$storage_modes"
  echo "ops=$ops"
  echo "warmup=$warmup"
  echo "repeats=$repeats"
  echo "clients_per_pair=$clients_per_pair"
  echo "server_cpus=${server_cpu_list:-unset}"
  echo "client_cpus=${client_cpu_list:-unset}"
  echo "numa_node=${numa_node:-unset}"
  echo "old_bin=${old_bin:-build-in-tree}"
  echo "new_bin=${new_bin:-build-in-tree}"
  uname -a
  command -v lscpu >/dev/null && lscpu || true
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
} > "$output/environment.txt"

if need_linux lscpu; then
  lscpu -e > "$output/topology/lscpu-e.txt" || true
  lscpu > "$output/topology/lscpu.txt" || true
fi
if need_linux numactl; then
  numactl --hardware > "$output/numa/hardware.txt" || true
fi

if [[ -z "$new_bin" ]]; then
  "$cmake_bin" --preset "$preset"
  "$cmake_bin" --build --preset "$preset" --target glyphastore_server_benchmarks
  new_bin="$root/build/$preset/glyphastore_server_benchmarks"
fi
if [[ -z "$old_bin" ]]; then
  old_bin="$new_bin"
  notes+=("single-binary-mode=no-baseline-OLD_BIN; scale matrix only")
  if [[ "$status" == "pass-candidate" ]]; then
    status="incomplete-no-old-binary"
  fi
fi

wrap_server() {
  local bin="$1"
  shift
  local cmd=()
  if [[ -n "$numa_node" ]] && command -v numactl >/dev/null 2>&1; then
    cmd+=(numactl --cpunodebind="$numa_node" --membind="$numa_node")
  fi
  if [[ -n "$server_cpu_list" ]] && command -v taskset >/dev/null 2>&1; then
    cmd+=(taskset -c "$server_cpu_list")
  fi
  cmd+=("$bin" "$@")
  "${cmd[@]}"
}

wrap_note_client() {
  if [[ -n "$client_cpu_list" ]]; then
    echo "client_cpu_list=$client_cpu_list (disjoint from server required for valid combined-load)" \
      >> "$output/topology/affinity-notes.txt"
  else
    echo "client_cpu_list=unset; mark combined-load if shared host" \
      >> "$output/topology/affinity-notes.txt"
    if [[ "$status" == "pass-candidate" && "$(uname -s)" == "Linux" ]]; then
      status="inconclusive-no-client-cpu-set"
    fi
  fi
}
wrap_note_client

run_cell() {
  local label="$1"
  local bin="$2"
  local pairs="$3"
  local pipeline="$4"
  local value_size="$5"
  local workload="$6"
  local mode="$7"
  local clients=$((pairs * clients_per_pair))
  local stem="${label}-p${pairs}-c${clients}-pipe${pipeline}-v${value_size}-${workload}-${mode}"
  local args=(
    --ops "$ops"
    --workers "$pairs"
    --clients "$clients"
    --pipeline "$pipeline"
    --value-size "$value_size"
    --workload "$workload"
    --storage-mode "$mode"
    --warmup "$warmup"
    --repeats "$repeats"
    --latency
    --executor-affinity
  )
  echo "$stem ${args[*]}" >> "$output/command-lines.txt"
  if [[ "$(uname -s)" == "Linux" ]] && command -v perf >/dev/null 2>&1; then
    if [[ -n "$numa_node" ]] && command -v numactl >/dev/null 2>&1 && \
       [[ -n "$server_cpu_list" ]] && command -v taskset >/dev/null 2>&1; then
      perf stat -o "$output/perf/${stem}.stat.txt" -- \
        numactl --cpunodebind="$numa_node" --membind="$numa_node" \
        taskset -c "$server_cpu_list" \
        "$bin" "${args[@]}" > "$output/samples/${stem}.txt"
    elif [[ -n "$server_cpu_list" ]] && command -v taskset >/dev/null 2>&1; then
      perf stat -o "$output/perf/${stem}.stat.txt" -- \
        taskset -c "$server_cpu_list" \
        "$bin" "${args[@]}" > "$output/samples/${stem}.txt"
    else
      perf stat -o "$output/perf/${stem}.stat.txt" -- \
        "$bin" "${args[@]}" > "$output/samples/${stem}.txt"
    fi
  elif [[ -n "$server_cpu_list" ]] && command -v taskset >/dev/null 2>&1; then
    taskset -c "$server_cpu_list" "$bin" "${args[@]}" > "$output/samples/${stem}.txt"
  else
    "$bin" "${args[@]}" > "$output/samples/${stem}.txt"
  fi
}

# Interleaved old/new/new/old when two binaries differ.
for pairs in $pairs_list; do
  for pipeline in $pipelines; do
    for value_size in $value_sizes; do
      for workload in $workloads; do
        for mode in $storage_modes; do
          if [[ "$old_bin" == "$new_bin" ]]; then
            run_cell "solo" "$new_bin" "$pairs" "$pipeline" "$value_size" "$workload" "$mode"
          else
            run_cell "old-a" "$old_bin" "$pairs" "$pipeline" "$value_size" "$workload" "$mode"
            run_cell "new-a" "$new_bin" "$pairs" "$pipeline" "$value_size" "$workload" "$mode"
            run_cell "new-b" "$new_bin" "$pairs" "$pipeline" "$value_size" "$workload" "$mode"
            run_cell "old-b" "$old_bin" "$pairs" "$pipeline" "$value_size" "$workload" "$mode"
          fi
        done
      done
    done
  done
done

{
  echo "# Paired Linux hard-pinned A/B — ${run_id}"
  echo
  echo "## Status"
  echo
  echo "**${status}**"
  echo
  echo "Platform: ${platform}. Notes:"
  for n in "${notes[@]:-}"; do
    echo "- $n"
  done
  echo
  echo "Artifacts: \`$output\`"
  echo
  echo "This report is auto-generated by \`scripts/benchmark_paired_linux_ab.sh\`."
  echo "A gate close requires Linux hard pins, disjoint client/server CPUs, perf TID"
  echo "separation, NUMA placement, and working set > LLC as documented in"
  echo "docs/benchmarks/paired-shards-linux-p1.md."
} > "$output/report.md"

printf 'status=%s\noutput=%s\n' "$status" "$output" | tee "$output/manifest.txt"
echo "$output"
