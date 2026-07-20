# GlyphaStore Perl SDK benchmarks — 0.1.0

Sequential and concurrent (`execute_worker_pipelines`) matrix for the pure-Perl client.

See [analysis.md](analysis.md) for concurrent-vs-sequential findings and the ops counting note
versus the earlier SDK 0.1.0 suite.

## Contents

| Path | Purpose |
| --- | --- |
| `environment.txt` | Host, toolchain, SDK, and daemon metadata |
| `commands.md` | Workload matrix and listen ports |
| `summary.md` | Comparison table (median ops/s) |
| `analysis.md` | Interpretation |
| `results.json` | Machine-readable parsed results |
| `perl/` | Raw sequential/concurrent result files |
| `logs/` | Server stdout/stderr |

## Reproduce

```bash
./scripts/benchmark_perl_client.sh
```

Optional: `OPS`, `WARMUP`, `REPEATS`, `GLYPHASTORED`, `PERL`.
