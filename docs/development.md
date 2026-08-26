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
the latest successful retained run on `main` only when the machine-readable runner identity matches.
CPU, runner image, kernel, compiler, architecture, logical CPU count, and build-preset changes
suppress deltas and are listed in the report rather than being mislabeled as code regressions.
For compatible environments, the report classifies overlapping min/max throughput ranges as
inconclusive. Only disjoint ranges become improvement or regression candidates.
The workflow invokes the report parser in strict mode: empty suites, duplicate identities, missing
metadata, count mismatches, invalid numbers, and inconsistent statistical ordering fail the report.
The machine-readable `engineering/performance/hosted-benchmark-contract.json` enumerates all 21
expected core, parallel, durable, TCP-scaling, and TCP-latency source files. Missing or unexpected
files invalidate the report before baseline comparison.

To avoid spending hosted-runner time on unrelated changes, the push trigger is limited to engine,
server, public headers, benchmark sources, CMake configuration, the canonical version, and the
report generator. Documentation, artwork, and non-C++ SDK-only changes rely on their focused CI;
the weekly and manual full-benchmark safety nets remain unconditional.

The TCP portion is a scalability matrix with 1, 2, and 4 owner-bound clients/workers and pipeline
depths 1, 8, 32, and 128. A separate `--latency` run reports p50, p95, p99, and p99.9 pipelined
response latency. Keeping latency instrumentation separate avoids charging a clock read to every
response in throughput measurements.

The report parser can also be used locally with one or more benchmark output files:

```bash
python3 scripts/benchmark_report.py benchmark-results/*.txt \
  --json benchmark-results/results.json \
  --markdown benchmark-results/summary.md
```

Add `--strict` when the local outputs are intended to exercise the CI evidence contract.
Pass `--source-contract engineering/performance/hosted-benchmark-contract.json` with the complete
hosted matrix to enforce its exact source set.

The durable TCP audit matrix runs v1 sync, strict group, and periodic policies at pipeline depths
1, 8, and 32. It includes both 1/2/4 client-to-Worker scaling and four clients per Worker at each
scale, because strict group commit cannot form multi-record batches with only one outstanding
mutation producer per Worker. It records raw output, exact queue/Store/batch metrics, environment
metadata, JSON, and Markdown below an ignored timestamped directory:

```bash
./scripts/benchmark_durable_server.sh
# Override duration without changing the matrix contract:
BENCHMARK_OPS=1000 BENCHMARK_REPEATS=5 ./scripts/benchmark_durable_server.sh
```

Throughput and latency runs are separate so steady-clock sampling does not contaminate the throughput
baseline. `durable-periodic` measures its relaxed acknowledgement contract; its later store-wide sync
must not be compared as though it were included in response latency. Unbatched `durable-sync` reports
zero for the batch-commit metric; its complete filesystem cost remains included in Store service time.

Hosted-runner measurements are deliberately informational because runner allocation, contention,
and CPU models can change. Use a controlled, thermally stable runner before turning changes into
hard regression gates or publishing absolute throughput claims.

Local maintenance and durable matrices write under gitignored `benchmark-results*/` via
`./scripts/dev.sh benchmark`, `benchmark-durable`, and related harness scripts. Do not commit raw
run outputs. Comparative GET/hot-cache notes live under `docs/benchmarks/` (methodology + medians
only); reproduce with `./scripts/benchmark_get_path.sh` or the focused filters in those notes.

`glyphastore_server_benchmarks` also has an opt-in controlled maintenance-overlap profile. Set
`--maintenance-overlap-seed-operations` to prebuild a reclaimable multi-Segment dataset on the last
Worker; owner-bound client 0 then drives the first Worker. The benchmark holds the initial background
space probe until `--maintenance-overlap-release-ms` after foreground start, so compaction admission
cannot race ahead of latency sampling. Seed key/value size and evaluation cadence are controlled by
`--maintenance-overlap-seed-keys`, `--maintenance-overlap-seed-value-bytes`, and
`--maintenance-overlap-eval-ms`. Run alternating threshold-off/on pairs; setup is outside the timed
region, but every repeat creates and recovers a fresh Store.

Cleartext vs TLS 1.3 tax on the Go pipeline harness:
`./scripts/benchmark_tls_tax.sh` (see [TLS performance note](security/tls-performance.md)).

OpenBSD / LibreSSL correctness gate (not a throughput bench):
`bash scripts/ci-openbsd-libressl.sh` on OpenBSD, or the
[OpenBSD LibreSSL](../.github/workflows/openbsd-libressl.yml) workflow.

FreeBSD native build/test gate (portability signal; not UFS/ZFS durability certification):
`bash scripts/ci-freebsd.sh` on FreeBSD, or the
[FreeBSD](../.github/workflows/freebsd.yml) workflow.

Continuous fuzz smoke (Linux Clang libFuzzer):

```bash
cmake --preset unix-fuzz
cmake --build --preset unix-fuzz
GLYPHASTORE_FUZZ_SECONDS=30 ./scripts/run-fuzzers.sh
```

CI runs each target in `.github/workflows/sanitizers.yml` (`fuzz-run`) against
`fuzz/corpus/<target>/` (60s on PR/push, 120s on the Monday schedule / manual dispatch).
