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

Linux may create one `SO_REUSEPORT` listener per executor; macOS and FreeBSD use one public
acceptor. Initial placement is temporary. After `INIT`, the client sends `BIND_WORKER`. If the
selected Worker differs from the temporary Reactor, the source removes the descriptor from its
poller and moves the `SocketHandle` plus buffered connection state through a bounded handoff queue.
The destination registers the same descriptor in its poller and becomes its sole owner. The kernel
TCP connection is not recreated or copied.

After binding, `GET`, `PUT`, and `ERASE` compute `hash(key) % worker_count`. The request executes
only when that owner is the bound Worker. A mismatched key receives `wrong_owner` with the correct
Worker and routing epoch; it is never forwarded through another executor. Server executors use a
private owner-checked Store path and do not acquire the public Store API's per-Worker mutex.

## Protocol

Frames use explicit little-endian fields and never decode persisted bytes through C++ object
layout. Protocol version 2 uses a 40-byte request header:

```text
u32 frame_size
u16 version
u8  opcode
u8  flags
u64 request_id
u32 key_size
u32 value_size
u64 expire_at_ns
u32 target_worker
u32 reserved
key bytes
value bytes
```

`expire_at_ns` is an absolute Unix-epoch nanosecond timestamp for `PUT` and is ignored by other
opcodes. `target_worker` is used only by `BIND_WORKER`. The daemon supplies its own current
wall-clock timestamp to Store reads.

A response header is also 40 bytes:

```text
u32 frame_size
u16 version
u16 status
u64 request_id
u32 value_size
u32 owner_worker
u32 worker_count
u32 reserved
u64 routing_epoch
value bytes
```

The required session sequence is `INIT`, then `BIND_WORKER`, then pipelined Store operations. `INIT`
returns protocol and routing metadata. Store commands before binding receive `not_bound`.
Misdirected keys receive `wrong_owner`; the client retries on the correct bound connection.
`request_id` correlates pipelined responses. Frames are length-checked before spans are formed. The
current maximum frame is 2 MiB.

## Connection identity

Poller events carry a packed `(slot, generation)` token rather than a bare file descriptor. Closing
a connection increments its generation. A delayed event or future Worker completion therefore
cannot target a new connection that reused the same descriptor or slot.

## Backpressure

Input and output buffering is bounded per connection. The one-time connection handoff queue for
each executor is bounded as well. A full handoff queue closes the connection being rebound.
Exceeding an input/output byte watermark closes the offending connection. Queue capacity is rounded
up to a power of two at startup; overload never becomes unbounded memory growth.

## Current scope

The daemon is still volatile and is not a durable network database. Executor threads are not yet
guaranteed to run on performance cores. Optional executor affinity is strict over the
process-allowed CPU set on Linux. macOS exposes only Mach affinity tags, so its mode is advisory
rather than a hard performance-core pin. The executable validates `INIT`, one-time connection
rebinding, wrong-owner rejection, TCP lifecycle, native readiness, partial frames, pipelining,
large partial output, half-close, connection generations, and graceful stop.

`glyphastore_server_benchmarks` measures the real loopback TCP protocol using one owner-bound
connection per client. Server startup, `INIT`, `BIND_WORKER`, connection establishment, request
encoding, and client thread creation are outside the timed region; every response is decoded and
validated. Traffic counters report the exact timed protocol-frame bytes sent into and returned by
the server, together with ingress, egress, and aggregate duplex bytes per second. Memory counters
report current RSS after each sample, its change from the post-setup baseline, and process peak RSS.
Because the benchmark server and loopback clients run in one process, RSS describes the complete
benchmark process rather than an isolated daemon.
