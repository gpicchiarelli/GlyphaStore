# GlyphaStore SDK benchmarks — 0.1.0

Published client-side pipeline benchmarks for the native Python and Perl SDKs at version
`0.1.0` (2026-07-19, Apple M4, same-host loopback against `macos-native-release` volatile
`glyphastored`).

This is a **public SDK baseline**: it measures official-client overhead, not absolute server
capacity. See [analysis.md](analysis.md) for interpretation.

## Contents

| Path | Purpose |
| --- | --- |
| `analysis.md` | Engineering judgment and what the suite does / does not prove |
| `environment.txt` | Host, toolchain, SDK, and daemon metadata |
| `commands.md` | Workload matrix and listen ports |
| `summary.md` | Full comparison table (median ops/s) |
| `results.json` | Machine-readable parsed results (48 runs) |
| `python/` | Raw Python sync/async result files |
| `perl/` | Raw Perl result files |
| `logs/` | Server stdout/stderr |

## Matrix

- Workers: 1, 2, 4
- Pipeline depths (PUT/GET pairs per batch): 1, 8, 32, 128
- Operations: 50 000 PUT/GET pairs (100 000 frames) per sample
- Sampling: 1 warmup + 7 measured repeats (median is the comparison statistic)
- Modes:
  - Python sync concurrent (one OS thread per Worker)
  - Python sync sequential (Workers drained in order)
  - Python async concurrent (`AsyncClient`, one task per Worker)
  - Perl sequential (single-process, Workers drained in order)

Every sample validates response count, success outcomes, and GET payload bytes.

## Headline medians (ops/s)

| Configuration | Python sync concurrent | Python async | Python sync sequential | Perl sequential |
| --- | ---: | ---: | ---: | ---: |
| w=1 p=1 | 37.1 k | 26.2 k | 38.6 k | 20.6 k |
| w=1 p=128 | 107.8 k | 98.1 k | 107.2 k | 44.7 k |
| w=4 p=1 | 42.3 k | 44.0 k | 39.3 k | 20.9 k |
| w=4 p=128 | 113.0 k | 100.1 k | 98.6 k | 43.3 k |

Pipeline depth dominates (~3× for Python). At deep pipelines sync and async converge, and adding
server Workers barely moves the needle—evidence the client is the limiter in this suite.

## How to reproduce

```bash
./scripts/benchmark_sdk_clients.sh
```

Optional overrides: `OPS`, `WARMUP`, `REPEATS`, `GLYPHASTORED`, `PYTHON`, `PERL`.
