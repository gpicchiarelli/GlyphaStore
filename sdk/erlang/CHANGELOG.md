# Changelog

## 0.1.0

- Initial OTP client: wire protocol v2 codec, structured errors, pipelines, batch,
  `execute_worker_pipelines`, TLS 1.3 connect options, monotonic deadlines, per-call timeouts,
  interop CLI, and golden fixtures.
- Benchmark harness (`benchmarks/client_benchmark.escript`) and
  `scripts/benchmark_erlang_client.sh` with sequential + concurrent matrices.
- `glyphastore_version:version/0` exported for packaging and lockstep checks.
