# Server model

`glyphastored` is the network process around the embedded Store. The initial transport is
non-blocking IPv4 TCP. Unix-domain sockets are a later transport using the same protocol and
connection state.

## Native reactor

The server selects its readiness backend at compile time:

- Linux: edge-triggered `epoll`;
- macOS and FreeBSD: `kqueue` with `EV_CLEAR`.

All accepted sockets are non-blocking and close-on-exec. Read and write handlers drain the socket
until `EAGAIN`. Each executor combines one Reactor and one Store Worker. That Reactor is the sole
owner of its connection sockets and input/output buffers, and that Worker is the only server data
path operating its Index and segments.

Linux creates one `SO_REUSEPORT` listener per executor and lets the kernel distribute connections.
On macOS and FreeBSD, one public acceptor distributes accepted sockets round-robin through bounded
handoff queues; each destination Reactor then registers its socket in its own `kqueue`. This avoids
depending on BSD `SO_REUSEPORT` behavior for listener load balancing.

`GET`, `PUT`, and `ERASE` compute the owner from the key hash. A same-owner request executes
directly on the local Worker. A remote-owner request crosses the target executor's bounded MPSC
inbox. Its completion returns through the origin executor's bounded completion ring and wakes that
Reactor through `eventfd` on Linux or a non-blocking pipe on macOS/FreeBSD. `HELLO` and `PING`
remain Reactor-local. Server executors use a private owner-checked Store path, so local work and
inbox consumption do not acquire the public Store API's per-Worker mutex.

## Protocol

Frames use explicit little-endian fields and never decode persisted bytes through C++ object
layout. A request header is 32 bytes:

```text
u32 frame_size
u16 version
u8  opcode
u8  flags
u64 request_id
u32 key_size
u32 value_size
u64 expire_at_ns
key bytes
value bytes
```

`expire_at_ns` is an absolute Unix-epoch nanosecond timestamp for `PUT` and is ignored by other
opcodes. The daemon supplies its own current wall-clock timestamp to Store reads.

A response header is 24 bytes:

```text
u32 frame_size
u16 version
u16 status
u64 request_id
u32 value_size
u32 reserved
value bytes
```

`request_id` permits pipelined requests and out-of-order completion when requests target different
Workers. Frames are length-checked before spans are formed. The current maximum frame is 2 MiB.

## Connection identity

Poller events carry a packed `(slot, generation)` token rather than a bare file descriptor. Closing
a connection increments its generation. A delayed event or future Worker completion therefore
cannot target a new connection that reused the same descriptor or slot.

## Backpressure

Input and output buffering is bounded per connection. Socket handoff queues, every Worker inbox,
completion rings, and the number of in-flight remote Store requests per connection are also
bounded. A full Worker inbox or exhausted per-connection allowance produces an `overloaded`
response. A full socket-handoff queue rejects the newly accepted connection. Exceeding an
input/output byte watermark closes the offending connection. Queue capacity is rounded up to a
power of two at startup; overload never becomes unbounded memory growth.

## Current scope

The daemon is still volatile and is not a durable network database. Executor threads are not yet
guaranteed to run on performance cores, and connection-to-key locality depends on the client
workload: arbitrary keys on one connection can still require remote Worker hops. Optional executor
affinity is strict over the process-allowed CPU set on Linux. macOS exposes only Mach affinity tags,
so its mode is advisory rather than a hard performance-core pin. The executable validates TCP
lifecycle, multi-Reactor connection distribution, native readiness, partial frames, pipelining,
out-of-order responses, large partial output, bounded MPSC contention, local and remote Store
operations, half-close with in-flight work, connection generations, and graceful stop.

`glyphastore_server_benchmarks` measures the real loopback TCP protocol. `uniform` routing uses the
platform's normal connection distribution and arbitrary keys. `affine` routing forces explicit
round-robin socket assignment and generates each client's keys for its matching Worker, isolating
the local fast path. Server startup, connection establishment, request encoding, and client thread
creation are outside the timed region; every response is decoded and validated.
