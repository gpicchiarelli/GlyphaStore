# GlyphaStore Ruby client benchmarks — analysis (`0.1.0`, 2026-07-20)

## Purpose

Baseline loopback matrix for the pure-Ruby sync client after Phase 2 hot-path pass.
Folder: `benchmark-results-ruby-0.1.0-20260720-175511`.

Reproduce: `./scripts/benchmark_ruby_client.sh` (Ruby ≥ 3.2; set `RUBY=` if needed).

## Counting

Median ops/s counts **wire operations** (`OPS × 2`: PUT + GET), same as current Perl/Go harnesses.

## Findings

### Peak rates (wire ops/s)

| Config | Median |
| --- | ---: |
| sequential W=1 p=128 | ~76–80 k (see summary) |
| concurrent W=4 p=128 | **~80.5 k** |
| sequential W=4 p=128 | ~76.4 k |

Deep pipelines remain the dominant lever. Concurrent overlap helps modestly at multi-Worker deep
pipelines on this host (~1.05× at W=4 p=128) and more at shallower depths (see summary gain table).

### Vs peers (same units, loopback)

Approximate deep-pipeline ceiling on one process:

| Client | ~ops/s (wire) at p=128 |
| --- | ---: |
| Go | ~1M+ concurrent (different order of magnitude) |
| Python sequential | ~200 k if counted as wire ops from pair-based 0.1.0 suite ×2, or ~100 k pairs |
| Ruby (this folder) | ~76–80 k wire |
| Perl | ~80–86 k wire |

Ruby lands near pure-Perl: MRI framing, not the daemon, is the limiter. Product scale remains
**one client per prefork process** (Puma/Unicorn). `AsyncClient` raises app concurrency under
Falcon/`async`, not this sync microbench ceiling.

### Optional C extension

Only worth pursuing if a measured FNV/pack hot path shows ≥15–20% median gain (roadmap Phase 2.6).

## Bottom line

Ship deep pipelines + `execute_batch`; scale with processes; use `AsyncClient` when the app is
Fiber-based. Do not expect Go-class sync microbench from pure Ruby.
