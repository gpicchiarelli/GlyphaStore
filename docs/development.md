# Development

The project uses CMake 3.25+, C++23, strict warnings, CTest, sanitizer presets, and standalone
fuzz/benchmark targets. macOS instructions are in [development-macos.md](development-macos.md).

Portable commands:

```bash
cmake --preset unix-debug
cmake --build --preset unix-debug
ctest --preset unix-debug
```

Before review, run formatting, unit/integration tests, the sanitizer appropriate to the change,
and any focused benchmark. Build directories and generated Xcode projects are never committed.

## Automated benchmark reports

`.github/workflows/benchmarks.yml` runs the fixed Release benchmark suite after pushes to `main`,
on a weekly schedule, and by manual dispatch. The workflow records the runner environment and
uploads raw benchmark output, `results.json`, and the rendered `summary.md` for 90 days. The same
summary is shown directly on the GitHub Actions run page and includes throughput deltas against
the latest successful retained run on `main`.

The report parser can also be used locally with one or more benchmark output files:

```bash
python3 scripts/benchmark_report.py benchmark-results/*.txt \
  --json benchmark-results/results.json \
  --markdown benchmark-results/summary.md
```

Hosted-runner measurements are deliberately informational because runner allocation, contention,
and CPU models can change. Use a controlled, thermally stable runner before turning changes into
hard regression gates or publishing absolute throughput claims.
