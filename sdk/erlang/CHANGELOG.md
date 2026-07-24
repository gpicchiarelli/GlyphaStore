# Changelog

## Unreleased

- **Concurrency:** `glyphastore_client` is a coordinator; normal I/O is delegated via
  `spawn_monitor` so concurrent callers targeting different Workers proceed in parallel.
  Each `glyphastore_conn` still serializes its stream. Fan-out uses monitors, deadline
  timers, and safe late-message handling (no unmonitored `spawn/1`).
- Request IDs remain allocated only inside the client gen_server (wrap
  `16#FFFFFFFFFFFFFFFF → 1`). Wire `request_id` stays correlation-only.
- Synchronous `close/1` drains in-flight work after rejecting new requests.
- Metadata mismatch (`routing_epoch` / `worker_count`) marks the client unhealthy.
- Connect uses `gen_server:start/3` so bootstrap failure cannot kill the caller via link.
- CT: `glyphastore_concurrency_SUITE` (25 targeted cases) plus extended fake server barriers.
- Benchmark harness measures single/multi caller, same vs different Workers, and pipeline APIs
  with p50/p95/p99.

## 0.1.0

- Initial OTP client: wire protocol v2 codec, structured errors, pipelines, batch,
  `execute_worker_pipelines`, TLS 1.3 connect options, monotonic deadlines, per-call timeouts,
  interop CLI, and golden fixtures.
- Benchmark harness (`benchmarks/client_benchmark.escript`) and
  `scripts/benchmark_erlang_client.sh` with sequential + concurrent matrices.
- `glyphastore_version:version/0` exported for packaging and lockstep checks.
- Synchronous `close/1`, exported `permission_denied/1`, pipeline disconnect classification
  aligned with Go/Python (`bytes_sent > begin_offset`), and TLS send via `ssl:send/2`
  (OTP has no timeout arity).
- Hot-path: iolist pipeline send (no forced contig copy), O(1) response slots via `array`,
  O(n) batch frame offsets, selective-receive multi-worker fan-out.
- Benchmark escript uses `+S 4:4` so concurrent `execute_worker_pipelines` can exercise
  multiple schedulers (was `+S 1:1`, which hid fan-out gains).
