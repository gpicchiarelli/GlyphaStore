# Paired shards Linux hard-pinned A/B — P1 harness (2026-07-31)

## Status

**Harness ready; gate not closed on macOS.** Hosted GitHub `ubuntu-24.04` runners and any macOS
advisory affinity remain **inconclusive** for this P1. Closure needs an isolated Linux machine with
hard CPU pins, NUMA binding, `perf`, and a working set above LLC.

## Why macOS cannot close the gate

- Mach affinity is advisory (`executor_affinity_semantics=mach-advisory`).
- No `taskset` / `numactl` / `perf` TID separation equivalent to the methodology in
  [`paired-shards-plan.md`](paired-shards-plan.md).
- Prior multi-pair and PUT→GET cells left inconclusive for that reason (compact base ~−3% volatile,
  durable multi-pair scale, PUT→GET 1-pair).

## Harness

```bash
# Linux self-hosted / dedicated box
SERVER_CPUS=0-7 CLIENT_CPUS=8-15 NUMA_NODE=0 \
OLD_BIN=/path/to/baseline/glyphastore_server_benchmarks \
NEW_BIN=/path/to/candidate/glyphastore_server_benchmarks \
PAIRS='1 2 4 8' \
./scripts/benchmark_paired_linux_ab.sh

# macOS smoke (explicitly inconclusive)
REQUIRE_LINUX_TOOLS=0 PAIRS='1 2' PIPELINES='1 8' REPEATS=1 WARMUP=0 \
  ./scripts/benchmark_paired_linux_ab.sh
```

Artifacts land in:

```text
benchmark-results/paired-shards/<commit>/<platform>/<run-id>/
  environment.txt
  topology/
  numa/
  perf/
  samples/
  command-lines.txt
  report.md
  manifest.txt
```

`platform` is `linux` or `macos-advisory`. Any missing pin, disjoint client set, or perf counter
forces `inconclusive-*` in `manifest.txt` — never a silent pass.

## Minimum matrix for gate close

| Asse | Valori |
|---|---|
| pairs | 1, 2, 4, 8 (capped to physical cores) |
| mix | GET; PUT→GET p1; 99/1 |
| values | 64 B (extend 1 KiB–256 KiB after first pass) |
| pipeline | 1, 8, 32 |
| durability | volatile first; durable-periodic for multi-pair follow-up |
| topology | disjoint client/server CPUs; no SMT sibling sharing in baseline |
| working set | declare LLC size; include a >LLC cell |

Order: interleaved `old/new/new/old`. Invalid if responses, request ids, or final verify fail.

## CI

`.github/workflows/paired-linux-performance.yml` is `workflow_dispatch` only and skips unless the
runner label `glyphastore-linux-perf` is present (self-hosted). Hosted Ubuntu jobs must not claim
this gate.

## Residual

Until a labeled Linux runner publishes `status=pass-candidate` with the evidence above, the P1
items that depended on hard-pinned A/B stay open. macOS samples under this harness are advisory.
