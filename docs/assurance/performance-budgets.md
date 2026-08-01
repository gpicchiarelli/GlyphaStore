Status: normative policy for budget classes; descriptive for advisory snapshots
Applies to: hosted CI regression, hardware self-hosted gates, soak/overload linkage
Owner: performance / ops maintainers
Last reviewed: 2026-08-01

# Performance and soak budgets

Machine-readable authority: [`engineering/performance/budgets.yaml`](../../engineering/performance/budgets.yaml).
Validator: `python3 engineering/tools/validate_perf_budgets.py`.

GlyphaStore claim ceiling remains **architectural prototype**. Hosted GitHub runners produce
**regression signals only**. Absolute throughput or p99 product claims require a labeled
hardware runner (`glyphastore-linux-perf`) and a `pass-candidate` manifest.

## Budget classes

| ID | Gate | Enforcement |
| --- | --- | --- |
| `HOSTED-MEDIAN-REGRESSION` | `GATE-PERFORMANCE` | Hosted CI ≤10% median ops/s regression (`GS-PERF-REGRESSION-001`) |
| `HARDWARE-PAIRED-LINUX-AB` | `GATE-PERFORMANCE` | Self-hosted Linux hard-pin A/B (`GS-PERF-BUDGET-001`); waiting for runner |
| `ADVISORY-EMBEDDED-DURABLE-GET-P99` | `GATE-PERFORMANCE` | Advisory macOS snapshot only — not a release gate |
| `SOAK-SOFTWARE-SMOKE` | `GATE-SOAK` | PR/push smoke (`GS-OPS-SOAK-001`) |
| `SOAK-SOFTWARE-EXTENDED` | `GATE-SOAK` | Scheduled/manual long/1h/4h software soak |
| `OVERLOAD-RUNBOOK` | `GATE-OPS-RUNBOOKS` | Drain/overload runbook exercised in ops-runbooks CI |

## Related

- [Benchmark standard](../spec/benchmark-standard.md)
- [Paired Linux P1 harness](../benchmarks/paired-shards-linux-p1.md)
- [Soak profiles](../operations/soak.md)
- [Graceful drain and overload](../operations/graceful-drain-and-overload.md)
