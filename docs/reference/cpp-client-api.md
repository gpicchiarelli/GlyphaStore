# C++ TCP client API

Status: normative for the current experimental C++ client
Applies to: `GlyphaStore::client`, wire protocol v2
Owner: networking maintainers
Last reviewed: 2026-07-19

## Purpose and ownership

`glyphastore::client::Client` is the reference implementation of wire protocol v2. It is separate
from the embedded Store and server runtime: applications link `GlyphaStore::client`, while the
client links only the small `GlyphaStore::wire` codec library.

`Client::connect()` performs `INIT`, records the server's Worker count and routing epoch, then opens
and binds exactly one TCP connection for every Worker. The client routes the complete binary key
with protocol-v2 FNV-1a. A mutex serializes traffic on each connection; calls routed to different
Workers can execute concurrently. A `Client` may therefore be shared between threads, but a single
Worker has either one synchronous request or one ordered pipeline in flight through one client
instance.

The API is synchronous and supports explicitly bounded pipelines. It deliberately does not promise
asynchronous completion, connection pooling beyond one connection per Worker, TLS, authentication,
or automatic topology rebalance.

## Basic use

```cpp
#include <glyphastore/client/client.hpp>

auto opened = glyphastore::client::Client::connect({
    .host = "127.0.0.1",
    .port = 7379,
});
if (!opened) {
    // inspect opened.error()
}
auto cache = std::move(*opened);

auto written = cache.put("session:42", "payload");
if (!written.committed()) {
    // distinguish written.outcome and inspect written.error
}

auto value = cache.get("session:42");
if (!value && value.error().code != glyphastore::ErrorCode::not_found) {
    // transport, protocol, or resource failure
}
```

Keys and values are bytes. The `string_view` overloads preserve embedded NUL bytes and perform no
encoding conversion. Returned values own their storage.

`PutOptions::expire_at_ns` is an absolute Unix timestamp in nanoseconds; zero disables expiration.
The client does not compensate for clock skew.

## Ordered pipelines

`execute_pipeline()` sends a bounded sequence without waiting between individual requests, then
returns one positional result per request. The complete pipeline must route to one Worker; use
`worker_for(key)` to group application work before submission. Request key/value spans need to remain
valid only until the synchronous call returns.

A failed outer `Result` means the whole batch was rejected before transmission, for example by
validation, limits, or connection admission. Once transmission starts, the call returns the
positional response vector even when the transport fails partway through.

```cpp
const std::array requests{
    glyphastore::client::PipelineRequest{
        .opcode = glyphastore::client::PipelineOpcode::put,
        .key = key,
        .value = value,
    },
    glyphastore::client::PipelineRequest{
        .opcode = glyphastore::client::PipelineOpcode::get,
        .key = key,
    },
};
auto completed = cache.execute_pipeline(requests);
```

The default limits are 256 request frames and 1 MiB of aggregate encoded request data. They are
controlled by `ClientConfig::maximum_pipeline_requests` and `maximum_pipeline_bytes`; each frame
must also satisfy `maximum_frame_bytes`. The request timeout covers sending and receiving the whole
batch. A pipeline holds that Worker's connection mutex for its duration, while other Workers remain
independent. Large depths improve throughput but also increase batch-completion latency and
head-of-line blocking. Start around 32–64 `PUT`/`GET` pairs (64–128 request frames) per Worker and
tune against the application's tail-latency objective; this is guidance, not a universal optimum.

`PipelineResponse::outcome` has per-position transport semantics:

| Outcome | Meaning |
|---|---|
| `succeeded` | A valid matching `OK` response was received; GET data is owned by `value`. |
| `failed` | A read failed, or a mutation is known unsent or explicitly rejected. |
| `indeterminate` | Mutation bytes may have reached the server without a trustworthy definitive response, or the server reported `INTERNAL_ERROR`. |

The client does not retry a pipeline automatically. Earlier valid responses remain successful after
a later disconnect; unresolved mutations and reads are classified independently. This avoids both
silently duplicating mutations and discarding outcomes already proven by the server.

## Mutation outcomes

`PUT` and `ERASE` return `MutationResult`, not `Result<void>`, because TCP disconnect alone cannot
prove whether a server-side mutation linearized.

| Outcome | Meaning | Safe default action |
|---|---|---|
| `committed` | An `OK` response with matching request and routing metadata was received. | Treat the mutation as applied. |
| `rejected` | The mutation request was not sent, or the server explicitly rejected it before applying it. | Correct the cause; a retry does not duplicate a known commit. |
| `indeterminate` | Request bytes may have reached the server but no trustworthy definitive response was received, or the server returned `INTERNAL_ERROR`. | Reconcile with a read or application idempotency policy before retrying. |

Protocol v2 has no idempotency token. The client automatically retries `GET` and `PING` once after
a transport failure because they are read-only. It retries a mutation only when zero bytes of its
request were sent. Reconnection repeats `INIT` and `BIND_WORKER` and accepts only the original
Worker count and routing epoch.

## Failure and lifecycle behavior

- Frame size, canonical reserved fields, request ID, Worker owner, Worker count, and routing epoch
  are validated on every response.
- A changed routing epoch/count or inconsistent owner makes the client unhealthy; constructing a
  new client is required because protocol v2 defines no online rebalance.
- A transient socket failure closes only the affected Worker connection. Its next safe operation
  attempts a lazy reconnect.
- `close()` is thread-safe and idempotent. Calls begun concurrently with close may finish first;
  later calls fail with `unavailable`.
- Moving a client transfers ownership. A moved-from client is closed for API purposes.
- Hostname resolution uses the platform resolver. The connect timeout applies to socket connect,
  not to time spent inside a blocking system resolver.

## Cross-language SDK contract

The C++ client and canonical fixtures define behavior to reproduce, not a C ABI to wrap. Python,
Perl, and a future Erlang client should implement the same wire contract natively so each runtime
retains its normal cancellation, scheduling, packaging, and binary-data conventions.

| Runtime | Intended public shape | Concurrency model |
|---|---|---|
| C++ | Implemented reference client | synchronous, thread-safe, one connection per Worker |
| Python | Native synchronous package implemented under `sdk/python`; `asyncio` next | one locked connection per Worker |
| Perl | Native module using byte strings | synchronous handles, optional event-loop adapter later |
| Erlang | Native OTP application | supervised Worker-connection processes |

Every SDK must consume the canonical request/response corpus under `tests/fixtures/`, use unsigned
little-endian fields exactly, distinguish indeterminate mutations, and pass the same malformed-frame
and routing vectors. A thin FFI wrapper around the embedded C++ client is not the default design:
it would complicate runtime integration and distribution while hiding semantics that the 40-byte
protocol makes straightforward to implement safely.

## Performance interpretation

The public-client benchmark with `--client-api` measures alternating
`PUT`/`GET` calls through one shared `Client`; `INIT`, binding, allocation of client threads, and
server startup remain outside the timed region. `--latency` records each synchronous API call.

```bash
./scripts/dev.sh benchmark-server --client-api --ops 100000 \
  --workers 4 --clients 4 --latency --warmup 1 --repeats 5
```

Use `--client-pipeline N` to benchmark batches of `N` ordered `PUT`/`GET` pairs through the public
pipeline API. `N=32` therefore submits 64 request frames and remains within the default 256-frame
limit.

```bash
./scripts/dev.sh benchmark-server --client-pipeline 32 --ops 100000 \
  --workers 4 --clients 4 --warmup 1 --repeats 5
```

Raw wire `--pipeline N` pre-encodes its frames before timing, while the public client includes API
validation, request encoding, owned GET results, and per-request outcome construction. Compare them
to quantify end-to-end SDK cost, not transport alone. Pipeline depth changes the throughput/latency
operating point, so only compare equal depths and workloads.
