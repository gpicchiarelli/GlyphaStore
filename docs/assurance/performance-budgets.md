Status: normative policy for budget classes; descriptive for advisory snapshots
Applies to: hosted CI regression, hardware self-hosted gates, soak/overload linkage
Owner: performance / ops maintainers
Last reviewed: 2026-08-27

# Performance and soak budgets

Machine-readable authority: [`engineering/performance/budgets.yaml`](../../engineering/performance/budgets.yaml).
Validator: `python3 engineering/tools/validate_perf_budgets.py`.

GlyphaStore claim ceiling remains **architectural prototype**. Hosted GitHub runners produce
**regression signals only**. Absolute throughput or p99 product claims require a labeled
hardware runner (`glyphastore-linux-perf`) and a `pass-candidate` manifest
([evidence taxonomy](evidence-taxonomy.md) label `hardware`).

Wave 6 scaffolding prepares absolute placeholders below. They stay
`specified_waiting_for_runner` until retained hardware evidence exists. This document does
**not** claim `ACCETTATA_PER_RILASCIO`, E3, or E4.

## Budget classes

| ID | Gate | Enforcement |
| --- | --- | --- |
| `HOSTED-MEDIAN-REGRESSION` | `GATE-PERFORMANCE` | Advisory hosted-CI signal; 10% median review threshold only for environment-compatible, disjoint ranges (`GS-PERF-REGRESSION-001`) |
| `HARDWARE-PAIRED-LINUX-AB` | `GATE-PERFORMANCE` | Self-hosted Linux hard-pin A/B (`GS-PERF-BUDGET-001`); `specified_waiting_for_runner` |
| `HARDWARE-SCALE-EFFICIENCY-1-8` | `GATE-PERFORMANCE` | Absolute placeholder: ≥80% efficiency 1→8 pairs vs physical cores (or named bottleneck); `specified_waiting_for_runner` / evidence `hardware` |
| `HARDWARE-ABSOLUTE-P99-GET` | `GATE-PERFORMANCE` | Absolute placeholder: p99 GET latency (ns TBD from runner); `specified_waiting_for_runner` / evidence `hardware` |
| `HARDWARE-ABSOLUTE-P99-PUT` | `GATE-PERFORMANCE` | Absolute placeholder: p99 PUT latency (ns TBD from runner); `specified_waiting_for_runner` / evidence `hardware` |
| `ADVISORY-EMBEDDED-DURABLE-GET-P99` | `GATE-PERFORMANCE` | Advisory macOS snapshot only — not a release gate (`local`) |
| `SOAK-SOFTWARE-SMOKE` | `GATE-SOAK` | PR/push smoke (`GS-OPS-SOAK-001`) |
| `SOAK-SOFTWARE-EXTENDED` | `GATE-SOAK` | Scheduled/manual long/1h/4h software soak |
| `OVERLOAD-RUNBOOK` | `GATE-OPS-RUNBOOKS` | Drain/overload runbook exercised in ops-runbooks CI |

## Absolute placeholders (Wave 6 — not enforced)

Fill numeric thresholds only from a `glyphastore-linux-perf` `pass-candidate` artifact. Until then,
treat every cell as scaffolding:

| Placeholder | Intended metric | Draft rule (not enforced) | Evidence class | Status |
| --- | --- | --- | --- | --- |
| Scale 1/2/4/8 | efficiency vs 1-pair median ops/s on same pin | ≥ 80% to physical-core count, or documented bottleneck removal | `hardware` | `specified_waiting_for_runner` |
| No CPU migration | `perf` `cpu-migrations` per cell | == 0 on hard-pinned run (harness flags otherwise) | `hardware` | `specified_waiting_for_runner` |
| Client/server split | disjoint `SERVER_CPUS` / `CLIENT_CPUS` | required for pass-candidate | `hardware` | `specified_waiting_for_runner` |
| Absolute p99 GET | GET p99 latency (ns) | **TBD** — do not invent | `hardware` | `specified_waiting_for_runner` |
| Absolute p99 PUT | PUT p99 latency (ns) | **TBD** — do not invent | `hardware` | `specified_waiting_for_runner` |
| Interleaved A/B | old/new/new/old | hard-reject >5% affine PUT 2t regression when closing ADR 0036 V12 | `hardware` | `specified_waiting_for_runner` |

Harness: [`scripts/benchmark_paired_linux_ab.sh`](../../scripts/benchmark_paired_linux_ab.sh).
Methodology: [`paired-shards-linux-p1.md`](../benchmarks/paired-shards-linux-p1.md).

## Wave 6 blockers (honest open)

1. Self-hosted runner labeled `glyphastore-linux-perf` publishing a retained `pass-candidate`
   manifest (hard pins, NUMA, disjoint client/server CPUs, migration counters).
2. Physical E3 durability lab (separate from this A/B harness); E3/E4 remain open — rehearsal ≠
   certification ([evidence taxonomy](evidence-taxonomy.md)).
3. No `ACCETTATA_PER_RILASCIO` promotion from this scaffolding alone.

## Related

- [Benchmark standard](../spec/benchmark-standard.md)
- [Paired Linux P1 harness](../benchmarks/paired-shards-linux-p1.md)
- [Evidence taxonomy](evidence-taxonomy.md)
- [Debt remediation lanes](debt-remediation-lanes.md) (Wave 6)
- [Soak profiles](../operations/soak.md)
- [Graceful drain and overload](../operations/graceful-drain-and-overload.md)
