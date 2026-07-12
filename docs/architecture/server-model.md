# Server model

`glyphastored` is the network process around the embedded Store. The initial transport is
non-blocking IPv4 TCP. Unix-domain sockets are a later transport using the same protocol and
connection state.

## Native reactor

The server selects its readiness backend at compile time:

- Linux: edge-triggered `epoll`;
- macOS and FreeBSD: `kqueue` with `EV_CLEAR`.

All accepted sockets are non-blocking and close-on-exec. Read and write handlers drain the socket
until `EAGAIN`. The Reactor is the sole owner of every connection socket and its input/output
buffers. Storage Workers return completions to the owning Reactor rather than writing to a socket
directly.

The current runtime has one network Reactor and one serial executor per Store Worker. `GET`, `PUT`,
and `ERASE` route by the already-computed key hash into that Worker's bounded MPSC inbox. The
Worker executor is the only server thread operating that partition, while `HELLO` and `PING` remain
reactor-local. Completed operations cross a bounded MPSC completion ring and wake the native
poller through `eventfd` on Linux or a non-blocking pipe on macOS/FreeBSD.

This removes Store work from the network loop and permits different Worker partitions to execute
concurrently. A later multi-reactor stage will pair a Reactor with each Worker executor, making
same-owner requests direct and reserving inbox hops for remote owners.

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

Input and output buffering is bounded per connection. Every Worker inbox, the completion ring, and
the number of in-flight Store requests per connection are also bounded. A full Worker inbox or an
exhausted per-connection allowance produces an `overloaded` response. Exceeding an input/output
byte watermark closes the offending connection. Queue capacity is rounded up to a power of two at
startup; overload never becomes unbounded memory growth.

## Current scope

The daemon is still volatile and is not a durable network database. Network accept/read/write is
currently handled by a single Reactor, so aggregate connection processing does not yet scale over
multiple pollers. The executable validates TCP lifecycle, native readiness, partial frames,
pipelining, out-of-order responses, large partial output, bounded MPSC contention, asynchronous
Store operations, half-close with in-flight work, connection generations, and graceful stop.
