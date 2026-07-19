# GlyphaStore performance profile — 2026-07-19

This directory records the complete locally available benchmark suite for clean commit
`28a12ae8b056e2f43d16fb8208f31fb575d943c7` on an Apple M4 Mac.

- `analysis.md` contains the engineering judgment and prioritized findings.
- `summary.md` and `results.json` are generated from the canonical result files.
- `ab-summary.md` and `ab-results.json` cover the controlled comparison with `7f54681`.
- `commands.md` records the exact workload matrix and exclusions.
- `environment.txt` records hardware, build, storage, power, and thermal state.
- `cpu-sample-server-w4-p32.txt` is the raw macOS `sample` call graph.
- All other `.txt` files are raw benchmark output, except the explicitly named expected-failure record.

These measurements are exploratory rather than a release baseline because macOS Low Power Mode was
enabled, affinity is advisory, and server plus load generator shared one process and one CPU set. Every
reported benchmark sample validated operation counts, status, response identity, and payload bytes.
