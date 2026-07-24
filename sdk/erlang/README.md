# GlyphaStore Erlang client

Native Erlang/OTP client for GlyphaStore wire protocol v2. One `gen_server` serializes
client state; each Worker owns a dedicated connection process (`glyphastore_conn`) with
`TCP_NODELAY`, canonical FNV-1a routing, monotonic request deadlines, and
`committed` / `rejected` / `indeterminate` mutation outcomes per
[client semantics v1](../../docs/spec/client-semantics-v1.md). Multi-Worker
`execute_batch` and `execute_worker_pipelines` overlap pipelines across Workers.

Version: `glyphastore_version:version/0` (must match repository `VERSION`)  
License: BSD-3-Clause  
Requires: Erlang/OTP ≥ 25 and rebar3 (on macOS: MacPorts `erlang` + `rebar3`).

```erlang
{ok, Client} = glyphastore_client:connect(#{host => "127.0.0.1", port => 7379}),
#{outcome := committed} = glyphastore_client:put(Client, <<"session\00042">>, <<"payload">>),
{ok, Value} = glyphastore_client:get(Client, <<"session\00042">>),
glyphastore_client:close(Client).
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
| `glyphastore_conn` | Per-Worker TCP/TLS connection `gen_server` |
| `glyphastore_client` | Public sync API (get/put/erase/ping, pipeline, batch, worker pipelines) |
| `scripts/glyphastore-interop` | CLI for `scripts/test-sdk-interop.sh` |
| `benchmarks/client_benchmark.escript` | PUT/GET pipeline throughput harness |

## Build and test

```bash
cd sdk/erlang
rebar3 compile
rebar3 ct
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
```

Artifacts land under `benchmark-results-erlang-*` (`summary.md`, `results.json`,
`environment.txt`, `commands.md`, raw logs). Concurrent mode uses
`execute_worker_pipelines` when `workers > 1`.

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
