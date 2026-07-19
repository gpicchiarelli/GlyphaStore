# GlyphaStore Go SDK benchmarks — 0.1.0

Published client-side pipeline benchmarks for the native Go SDK at version `0.1.0`.

## Contents

| Path | Purpose |
| --- | --- |
| `environment.txt` | Host, toolchain, SDK, and daemon metadata |
| `commands.md` | Workload matrix and listen ports |
| `summary.md` | Comparison table (median ops/s) |
| `results.json` | Machine-readable parsed results |
| `go/` | Raw Go concurrent/sequential result files |
| `logs/` | Server stdout/stderr |

## How to reproduce

```bash
./scripts/benchmark_go_client.sh
```

Optional overrides: `OPS`, `WARMUP`, `REPEATS`, `GLYPHASTORED`, `GO`.
