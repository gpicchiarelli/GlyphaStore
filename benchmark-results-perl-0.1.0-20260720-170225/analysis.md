# GlyphaStore Perl client benchmarks — analysis (`0.1.0`, 2026-07-20)

## Purpose

Re-measure the pure-Perl client with both **sequential** Worker drain and
`execute_worker_pipelines` (**concurrent**) on the same machine/daemon class as the published
SDK 0.1.0 baseline. Folder: `benchmark-results-perl-0.1.0-20260720-170225`.

Reproduce: `./scripts/benchmark_perl_client.sh`.

## Counting note

This harness reports `operations = OPS × 2` (PUT + GET), matching current Python/Go benches.
The [SDK 0.1.0 suite](../benchmark-results-sdk-0.1.0-20260719-161956/) Perl (and Python) files used
`operations = OPS` (pairs). Wall-clock work is the same: **halve** these median ops/s to compare
pair rates to that folder, or **double** those older rates to compare wire-op rates here.

| Suite | Unit | Sequential W=1 p=128 (approx.) |
| --- | --- | ---: |
| SDK 0.1.0 Perl | pairs/s | ~45 k |
| This folder | wire ops/s | ~86 k (≈ **43 k** pairs/s) |

Throughput is unchanged; only the reported unit moved.

## Findings

### Concurrent vs sequential (same unit)

| Workers | Pipeline | Sequential | Concurrent | Gain |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 1 | 24.2 k | 23.7 k | 0.98× |
| 2 | 8 | 59.5 k | 62.3 k | 1.05× |
| 2 | 32 | 78.2 k | 76.4 k | 0.98× |
| 2 | 128 | 47.3 k | 81.8 k | **1.73×** |
| 4 | 1 | 26.2 k | 30.8 k | **1.18×** |
| 4 | 8 | 61.9 k | 65.8 k | 1.06× |
| 4 | 32 | 79.8 k | 80.3 k | 1.01× |
| 4 | 128 | 85.1 k | 80.9 k | 0.95× |

At deep pipelines, concurrent is usually **~1.0×** sequential: the process is already CPU-bound on
pure-Perl framing. Prefer concurrent anyway — W=2 p=128 sequential showed a collapsed median and
wide min/max; concurrent held ~82 k and was stable. Shallow pipelines (p=1) still gain from overlap.

### Pipeline depth

Still the dominant lever: ~26 k → ~86 k wire ops/s from p=1 to p=128 (sequential W=1).

### Vs Python (pair units)

Against SDK 0.1.0 Python sequential (~98–107 k pairs/s at p=128), Perl remains about **2–2.5×**
slower on one process. That gap does not close with Worker overlap; it needs processes (or XS).

### What this does not change

- Product scale: **one client per prefork process**, not ithreads.
- Event-loop adapters raise web throughput, not this sync microbench.
- Optional XS on encode/decode/FNV remains the only large SDK-side microbench leap.

## Bottom line

Deep pipelines yes; default to `execute_worker_pipelines` for multi-Worker stability; scale with
processes. Concurrent does not invent a 2× sync microbench win over sequential at saturation —
and that is expected.
