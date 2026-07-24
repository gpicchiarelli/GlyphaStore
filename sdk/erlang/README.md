# GlyphaStore Erlang client

Native Erlang/OTP client for GlyphaStore wire protocol v2.

**Concurrency model:** `glyphastore_client` is a coordinator only (config, routing
metadata, request-ID allocation, health/epoch, lifecycle). Normal request I/O is
delegated to monitored processes that call the per-Worker `glyphastore_conn`
`gen_server`. Callers targeting **different Workers proceed in parallel** on the
BEAM; each Worker connection still **serializes** request/response on its single
TCP/TLS stream. Many Erlang processes may share one client safely.

Also: canonical FNV-1a routing, monotonic request deadlines, routing-epoch
fail-closed checks, and `committed` / `rejected` / `indeterminate` mutation
outcomes per [client semantics v1](../../docs/spec/client-semantics-v1.md).

Version: `glyphastore_version:version/0` (must match repository `VERSION`)  
License: BSD-3-Clause  
Requires: Erlang/OTP ≥ 25 and rebar3 (on macOS: MacPorts `erlang` + `rebar3`).

```erlang
{ok, Client} = glyphastore_client:connect(#{host => "127.0.0.1", port => 7379}),
#{outcome := committed} = glyphastore_client:put(Client, <<"session\00042">>, <<"payload">>),
{ok, Value} = glyphastore_client:get(Client, <<"session\00042">>),
ok = glyphastore_client:close(Client).
```

## Install toolchain (macOS)

Use MacPorts — do not install OTP via kerl, asdf, or Homebrew for this project:

```bash
sudo port install erlang rebar3
```

Confirm `/opt/local/bin` is on `PATH`, then `erl` and `rebar3` resolve from MacPorts.

## Layout

| Module | Role |
| --- | --- |
| `glyphastore_protocol` | Wire codec, FNV-1a routing, golden fixture tests |
| `glyphastore_error` | Structured client-semantics v1 errors |
| `glyphastore_conn` | Per-Worker TCP/TLS connection `gen_server` (serial I/O) |
| `glyphastore_client` | Public sync API + coordinator (noreply I/O delegation) |
| `scripts/glyphastore-interop` | CLI for `scripts/test-sdk-interop.sh` |
| `benchmarks/client_benchmark.escript` | Throughput / latency harness |

## Concurrency invariants

| Rule | Behavior |
| --- | --- |
| One connection per Worker | Bootstrap opens `worker_count` conns; reconnect re-`INIT`+`BIND_WORKER` |
| Serialize per Worker | `glyphastore_conn` queues exchanges; pipelines use atomic `run_pipeline/4` (send+collect in one call) |
| Parallel across Workers | Coordinator returns `{noreply, …}` while I/O runs off-callback |
| Many callers, one client | Safe; request IDs allocated only inside the coordinator |
| Request ID wrap | `16#FFFFFFFFFFFFFFFF → 1` (correlation-only on the wire; no server dedup) |
| Fan-out | `spawn_monitor` + deadline timer; DOWN/timeout/late messages handled safely |
| `close/1` | **Synchronous**: rejects new work, drains/fails in-flight, stops conns |
| Retry | At most one auto-retry for reads / zero-byte mutations; shared monotonic deadline |
| Routing epoch | Metadata mismatch → `unavailable`, client unhealthy (fail-closed) |

### Known limits

- No permanent OTP supervisor tree yet (monitor + explicit lifecycle; reconnect verifies epoch/count).
- Bootstrap/reconnect still runs on the coordinator (rare path).
- Same-Worker throughput remains connection-bound by design.

## Intentional differences vs other SDKs

| Topic | Erlang choice | Why |
| --- | --- | --- |
| Concurrency | Coordinator + per-Worker conn + `spawn_monitor` | Parallel across Workers without weakening mutation semantics |
| Async client | None in 0.1.x | OTP processes *are* the concurrency model |
| `execute_worker_pipelines/2,3` | Public (Perl parity) | Pre-shard work without re-hashing in the client |
| Pipeline framing | Iodata/`iolist` send | Avoids an extra contiguous copy of aggregated frames |
| TLS send | `ssl:send/2` + deadline checks | OTP `ssl` has no `send/3` timeout arity |
| Client start | `gen_server:start/3` | Bootstrap failure must not kill the caller via link |

Server `HEALTH`/`READY`/`STATS` opcodes exist in the codec only — same as Go/Python/Perl/Ruby
(no public client RPC for lifecycle probes).

## Build and test

```bash
cd sdk/erlang
rebar3 compile
rebar3 ct
rebar3 dialyzer
```

From the repository root:

```bash
./scripts/test-erlang-client.sh
./scripts/package-erlang-client.sh
./scripts/sync-sdk-fixtures.sh   # refresh vendored wire hex fixtures
```

## Benchmarks

Against a local `glyphastored` (same matrix as Python/Perl/Go/Ruby):

```bash
./scripts/benchmark_erlang_client.sh
# short smoke:
OPS=2000 WARMUP=0 REPEATS=1 ./scripts/benchmark_erlang_client.sh /tmp/erlang-bench-smoke
# meaningful concurrent matrix:
OPS=20000 WARMUP=1 REPEATS=3 CALLERS=8 ./scripts/benchmark_erlang_client.sh
```

The harness reports separately:

- single caller vs multi-caller sharing one client
- traffic across different Workers vs concentrated on one Worker
- `execute_pipeline` / `execute_batch` / `execute_worker_pipelines`
- pipeline sizes 1, 8, 32, 128 and Worker counts 1, 2, 4 (cleartext; TLS when available)

Metrics: throughput, p50/p95/p99 latency, error and indeterminate counts. Primary
criterion: many Erlang processes sharing one client against different Workers.

Artifacts land under `benchmark-results-erlang-*`. The escript pins `+S 4:4` for
cross-machine comparability.

## TLS

Optional TLS 1.3 is supported when the `ssl` application is available (standard OTP).
Cleartext remains the default. If `ssl` is missing, TLS connect fails closed with
`unavailable` (same soft-exclude pattern as Perl without `IO::Socket::SSL`). The root interop
harness includes Erlang in both cleartext and TLS matrices when OTP/rebar3 are available; Ruby
remains cleartext-only until its Phase 3 TLS train.

```erlang
Config = #{
    host => "127.0.0.1",
    port => 7379,
    tls => #{enable => true, ca_file => "/path/to/ca.pem", server_name => "localhost"}
},
{ok, Client} = glyphastore_client:connect(Config).
```

## Interop

After `rebar3 compile`:

```bash
sdk/erlang/scripts/glyphastore-interop.escript --port "$PORT" get --key-hex 6161
```

The root interop harness builds beams and invokes this script when `erl` and `rebar3`
are on `PATH`.
