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
Worker has one in-flight request through one client instance.

The API is synchronous. It deliberately does not promise pipelining, asynchronous completion,
connection pooling beyond one connection per Worker, TLS, authentication, or automatic topology
rebalance.

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
| Python | Native package with sync API first, then `asyncio` | one ordered connection task per Worker |
| Perl | Native module using byte strings | synchronous handles, optional event-loop adapter later |
| Erlang | Native OTP application | supervised Worker-connection processes |

Every SDK must consume the canonical request/response corpus under `tests/fixtures/`, use unsigned
little-endian fields exactly, distinguish indeterminate mutations, and pass the same malformed-frame
and routing vectors. A thin FFI wrapper around the embedded C++ client is not the default design:
it would complicate runtime integration and distribution while hiding semantics that the 40-byte
protocol makes straightforward to implement safely.

## Performance interpretation

The public-client benchmark is the raw TCP benchmark with `--client-api`. It measures alternating
`PUT`/`GET` calls through one shared `Client`; `INIT`, binding, allocation of client threads, and
server startup remain outside the timed region. `--latency` records each synchronous API call.

```bash
./scripts/dev.sh benchmark-server --client-api --ops 100000 \
  --workers 4 --clients 4 --latency --warmup 1 --repeats 5
```

Raw wire `--pipeline 1` sends one `PUT`/`GET` pair together and therefore still has two requests in
flight; the public synchronous API waits after each call. Use the comparison to expose round-trip
cost, not as a pure API-overhead measurement. Raw wire `--pipeline 32` quantifies the throughput
headroom intentionally left for a future pipelined/async client. Do not present these workloads as
equivalent: pipeline depth changes the number of requests in flight and therefore the
latency/throughput operating point.
