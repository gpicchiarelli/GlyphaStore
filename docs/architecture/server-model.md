# Server model

`glyphastored` is the network process around the embedded Store. The initial transport is
non-blocking IPv4 TCP. Optional TLS 1.3 wraps the same protocol when `--tls-cert`/`--tls-key` are
set (ADR 0020; LibreSSL on OpenBSD). With `--tls-port`, the process may expose cleartext and TLS on
distinct ports in one process; without it, TLS configuration makes `--port` TLS-only. Unix-domain
sockets are a later transport using the same protocol and connection state.

## Native reactor

The server selects its readiness backend at compile time:

- Linux: edge-triggered `epoll`;
- macOS, FreeBSD, and OpenBSD: `kqueue` with `EV_CLEAR`.

All accepted sockets are non-blocking and close-on-exec. Read and write handlers drain the socket
until `EAGAIN`. Each executor combines one Reactor and one Store Worker. That Reactor is the sole
owner of its connection sockets and input/output buffers, and that Worker is the only server data
path operating its Index and segments.

Linux may create one `SO_REUSEPORT` listener per executor; macOS, FreeBSD, and OpenBSD use one public
acceptor. Initial placement is temporary. After `INIT`, the client sends `BIND_WORKER`. If the
selected Worker differs from the temporary Reactor, the source removes the descriptor from its
poller and moves the `SocketHandle` plus buffered connection state through a bounded handoff queue.
The destination registers the same descriptor in its poller and becomes its sole owner. The kernel
TCP connection is not recreated or copied.

After binding, `GET`, `PUT`, and `ERASE` compute `hash(key) % worker_count`. The request executes
only when that owner is the bound Worker. A mismatched key receives `wrong_owner` with the correct
Worker and routing epoch; it is never forwarded through another executor. Server executors use a
private owner-checked Store path and do not acquire the public Store API's per-Worker mutex.

Volatile GETs and durable hot-cache hits remain on the owner Reactor. A durable cold miss is split:
the Reactor briefly resolves and owns the exact `RecordRef` plus immutable generation pin, then
submits that prepared read to a process-wide bounded disk-read executor. File reads, CRC validation,
and value materialization happen there without a Worker or catalog lock. Completion returns through
a bounded queue owned by the original Reactor, where the runtime revalidates the index reference and
generation pin before the response is published. A concurrent replacement causes a bounded retry
against a newly prepared pin; a closed or reused connection is rejected by `(slot, generation)`.

Only one cold read may be outstanding per connection. The Reactor stops executing later frames from
that connection until completion, so protocol-v2 responses remain strictly in request order without
a reorder buffer. It may continue draining socket bytes into the configured input watermark so FIN,
RST, and overflow remain observable. Other connections on the same Reactor continue normally.

Durable `PUT` and `ERASE` use one bounded FIFO dispatch lane per Worker. The Reactor copies only the request's
key/value into an owning task; no `RecordRef`, Segment, file handle, or borrowed protocol span crosses
this boundary. Successful enqueue is the daemon admission/order point, not the storage visibility
point. The lane releases its queue mutex before entering Store, and the Store's persistence-v1 state
machine retains the actual visibility and durable commit points. A completion returns only after the
selected acknowledgement policy completes. One in-flight Store request per connection preserves
wire order, while independent Worker lanes avoid a shared request lock or queue. Sync and periodic
modes use one storage thread per lane. Strict-group uses a bounded producer set so multiple admitted
mutations can occupy one batch while earlier producers await acknowledgement; the configured width
is capped by both the batch record limit and a process-wide thread ceiling.

## Protocol

The normative, client-implementable contract is [Wire Protocol v2](../spec/wire-protocol-v2.md).
The summary below is intentionally non-exhaustive.

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

`expire_at_ns` is an absolute Unix-epoch nanosecond timestamp for `PUT` and must be zero for other
opcodes. `target_worker` is meaningful only for `BIND_WORKER` (a real Worker id); all other opcodes
must set it to `kNoWorker`. Encoders and decoders reject non-canonical opcode-specific fields.
The daemon supplies its own current
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

The recommended session sequence is `INIT`, then one `BIND_WORKER`, then pipelined Store operations.
Version 2 also permits `PING` before initialization and repeated `INIT`; binding itself requires
initialization and is one-time. `INIT` returns protocol and routing metadata. Store commands before binding receive `not_bound`.
Misdirected keys receive `wrong_owner`; the client retries on the correct bound connection.
`request_id` correlates pipelined responses. Frames are length-checked before spans are formed. The
current maximum frame is 2 MiB.

## Connection identity

Poller events carry a packed `(slot, generation)` token rather than a bare file descriptor. Closing
a connection increments its generation. A delayed event or future Worker completion therefore
cannot target a new connection that reused the same descriptor or slot.

## Backpressure

Input and output buffering is bounded per connection. The one-time connection handoff queue for
each executor, the shared disk-read request queue, every per-Worker durable mutation lane, and every
Reactor completion queue are bounded as well. Durable mutation admission has both a request-count
limit and an owned-byte limit, so maximum-size payloads cannot multiply up to the count limit. A full
handoff queue closes the connection being rebound. A cold-read or durable-mutation admission failure
returns `overloaded` and does not enqueue work.
Exceeding an input/output byte watermark closes the offending connection. Queue capacity is rounded
up to a power of two at startup; overload never becomes unbounded memory growth.

The durable lane queue also has a configurable wait deadline. A task that expires before Store entry
returns `overloaded` and is known not committed; a task that has entered Store is never cancelled.
Per-Worker snapshots report current/peak queue records and owned bytes, producer width,
admitted/rejected/expired/completed counts, queue-wait time, and total Store service time. Metrics use
only the lane's short queue mutex and cannot wait for its filesystem operation.

`Server::durable_batch_stats()` exposes the Store kernel's lock-free Worker-local batch snapshot:
pending and committed occupancy, adaptive target, close reasons, failures, and exact duration of the
Segment batch commit operation. These counters complement lane queue/service time instead of
conflating scheduling delay with persistence work.

## Current scope

The daemon supports the Store's volatile and durable modes. Executor threads are not yet guaranteed
to run on performance cores. Optional executor affinity is strict over the
process-allowed CPU set on Linux. macOS exposes only Mach affinity tags, so its mode is advisory
rather than a hard performance-core pin. The executable validates `INIT`, one-time connection
rebinding, wrong-owner rejection, TCP lifecycle, native readiness, partial frames, pipelining,
large partial output, half-close, connection generations, and graceful stop.

Graceful stop stops accepting new connections, drains existing connections within
`--shutdown-drain-ms`, drains durable mutations already admitted before closing Store, and fails
closed if the shared deadline expires. Wire-protocol `HEALTH` and `READY` report process liveness
and traffic readiness without requiring Worker binding. Client disconnect does not cancel admitted
storage work: a stale `(slot, generation)` completion is discarded, and the client must classify the
mutation as indeterminate.

`glyphastore_server_benchmarks` measures the real loopback TCP protocol using one owner-bound
connection per client. Server startup, `INIT`, `BIND_WORKER`, connection establishment, request
encoding, and client thread creation are outside the timed region; every response is decoded and
validated. Traffic counters report the exact timed protocol-frame bytes sent into and returned by
the server, together with ingress, egress, and aggregate duplex bytes per second. Memory counters
report current RSS after each sample, its change from the post-setup baseline, and process peak RSS.
Because the benchmark server and loopback clients run in one process, RSS describes the complete
benchmark process rather than an isolated daemon.

For durable-sync storage, a successful mutation response is encoded only after the Store crosses
the durable commit point and publishes coherent in-memory state. A disconnect
between commit and response delivery is an indeterminate client outcome as defined by the
[durability and recovery contract](durability-recovery.md).
