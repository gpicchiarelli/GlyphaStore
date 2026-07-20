# GlyphaStore Wire Protocol v2

Status: normative for the current TCP server
Applies to: protocol version 2
Owner: networking maintainers
Last reviewed: 2026-07-19

## 1. Transport and byte order

The protocol runs over a reliable TCP byte stream. Every integer is unsigned and encoded little-endian. Frames are length-prefixed; TCP packet boundaries have no semantic meaning.

The maximum protocol frame size is 2 MiB, including the header. The server's connection input and output buffering may impose an additional configured limit (4 MiB by default). A client must handle partial reads and writes.

There is currently no TLS, authentication, compression, multiplexed stream identifier, or protocol-level timeout. Deployments requiring confidentiality or access control must provide an external trusted boundary. Official clients impose local connect/request deadlines and classify failures as specified in [client semantics v1](client-semantics-v1.md). The planned secure profile wraps this protocol in TLS 1.3 with mTLS ([ADR 0020](../adr/0020-tls-outer-transport.md), [ADR 0021](../adr/0021-secure-profile-authentication.md)) without changing frame layout; OpenBSD uses LibreSSL.

## 2. Request frame

Every request has a 40-byte header followed by exactly `key_size` key bytes and `value_size` value bytes.

| Offset | Size | Type | Field |
|---:|---:|---|---|
| 0 | 4 | `u32` | `frame_size` |
| 4 | 2 | `u16` | `version`, exactly `2` |
| 6 | 1 | `u8` | `opcode` |
| 7 | 1 | `u8` | `flags` |
| 8 | 8 | `u64` | `request_id` |
| 16 | 4 | `u32` | `key_size` |
| 20 | 4 | `u32` | `value_size` |
| 24 | 8 | `u64` | `expire_at_ns` |
| 32 | 4 | `u32` | `target_worker` |
| 36 | 4 | `u32` | `reserved` |
| 40 | `key_size` | bytes | key |
| … | `value_size` | bytes | value |

`frame_size` must equal `40 + key_size + value_size` without overflow. Senders must set `flags` and
`reserved` to zero. Version 2 receivers reject nonzero values; no meaning may be inferred from them.

Keys and values are arbitrary bytes and need not be text or NUL-terminated.

## 3. Response frame

Every response has a 40-byte header followed by exactly `value_size` value bytes.

| Offset | Size | Type | Field |
|---:|---:|---|---|
| 0 | 4 | `u32` | `frame_size` |
| 4 | 2 | `u16` | `version`, exactly `2` |
| 6 | 2 | `u16` | `status` |
| 8 | 8 | `u64` | copied `request_id` |
| 16 | 4 | `u32` | `value_size` |
| 20 | 4 | `u32` | `owner_worker` |
| 24 | 4 | `u32` | `worker_count` |
| 28 | 4 | `u32` | `reserved`, zero |
| 32 | 8 | `u64` | `routing_epoch` |
| 40 | `value_size` | bytes | response value |

Unknown status codes must be treated as errors while preserving frame synchronization.

## 4. Opcodes

| Value | Name | Request fields | Successful response |
|---:|---|---|---|
| 1 | `INIT` | no key/value required | value is ASCII `GlyphaStore/2`; reports worker metadata |
| 2 | `PING` | value is opaque; key ignored | echoes value |
| 3 | `GET` | key required by Store semantics | stored value or `NOT_FOUND` |
| 4 | `PUT` | key, value; optional `expire_at_ns` | empty value |
| 5 | `ERASE` | key | empty value or `NOT_FOUND` according to Store result |
| 6 | `BIND_WORKER` | `target_worker` | confirms worker metadata; may transfer connection ownership |

Fields not listed for an opcode must be sent as zero/empty. The current decoder may accept ignored data, but clients must not rely on it.

## 5. Status codes

| Value | Name | Meaning |
|---:|---|---|
| 0 | `OK` | request completed |
| 1 | `INVALID_REQUEST` | invalid fields, sequence, or target |
| 2 | `UNSUPPORTED` | opcode or operation not supported |
| 3 | `INTERNAL_ERROR` | server could not complete request |
| 4 | `NOT_FOUND` | key is not currently visible |
| 5 | `OVERLOADED` | bounded resource or admission limit reached |
| 6 | `WRONG_OWNER` | bound Worker does not own the key; `owner_worker` is authoritative |
| 7 | `NOT_BOUND` | Store operation attempted before Worker binding |

The mapping from internal error categories to protocol status is many-to-one. Clients must not infer an internal `ErrorCode` that is not carried on the wire.

## 6. Session state

A conventional session is:

```mermaid
stateDiagram-v2
    [*] --> Connected
    Connected --> Initialized: INIT
    Connected --> Connected: PING
    Initialized --> Initialized: INIT or PING
    Initialized --> Bound: BIND_WORKER once
    Bound --> Bound: PING / GET / PUT / ERASE
    Bound --> [*]: peer close, protocol failure, or server close
```

Current version-2 behavior is precise:

- `PING` is accepted before initialization and binding;
- repeated `INIT` is accepted and returns the same identification value;
- `BIND_WORKER` requires successful initialization, a valid target, and no prior bind;
- `GET`, `PUT`, and `ERASE` before binding return `NOT_BOUND`;
- binding is permanent for the connection;
- after binding, a key owned by another Worker returns `WRONG_OWNER`; the server does not forward that request.

Clients should send `INIT`, read worker metadata, choose a Worker, and send exactly one `BIND_WORKER` before Store traffic.

## 7. Routing metadata

Responses carry `worker_count` and `routing_epoch`. Protocol v2 routing uses FNV-1a 64-bit over the
complete key, starting at offset basis `14695981039346656037` and, for each byte in order, XORing
the byte then multiplying modulo 2^64 by `1099511628211`. The owner is:

```text
owner_worker = fnv1a64(key) % worker_count
```

A client that receives `WRONG_OWNER` should trust the response's `owner_worker`, refresh the current
Worker count/epoch, and choose or open an appropriately bound connection.

A client should maintain at least one connection per actively used Worker when it needs Worker-affine throughput. Changing worker count or epoch invalidates cached routing decisions. Online epoch transition and rebalance are not yet defined.

## 8. Pipelining and ordering

Clients may place multiple complete requests on one connection without waiting for each response. The server processes and emits responses in request order for that connection. `request_id` is copied verbatim so the client can correlate responses; it does not change ordering or provide idempotency.

There is no cross-connection ordering guarantee and no transaction spanning requests.

## 9. Expiration

`expire_at_ns` is meaningful only for `PUT`. Zero means no expiration. A nonzero value is an absolute Unix timestamp in nanoseconds. Expired records are not visible to `GET`; exact reclamation timing is not promised.

Clock synchronization is an operational responsibility. Version 2 has no server-time query.

## 10. Malformed input and connection closure

The server closes the connection without a response when it cannot safely decode frame boundaries, including an invalid header size, unsupported version at framing time, size overflow, or frame beyond the maximum. Once framing is trustworthy, semantic request errors may receive `INVALID_REQUEST` or `UNSUPPORTED`.

Failure to enqueue a one-time connection handoff, input/output buffer exhaustion, socket error, or peer EOF also closes the connection. A client must treat disconnect as an indeterminate transport outcome: it cannot know solely from the disconnect whether a mutation linearized.

## 11. Backpressure

Input, output, and handoff queues are bounded. A client that does not read responses can eventually be disconnected. A client that sends faster than the bound executor can process may stop receiving read readiness or be disconnected at a configured hard bound. Unlimited buffering is never promised.

At most one durable cold `GET` is admitted per connection. Later frames on that connection remain
buffered and are not executed until the read completes, preserving the response order defined in
section 8. Disk-read executor or completion-capacity saturation returns `OVERLOADED` for the `GET`;
it never creates an unbounded queue. This does not prevent other connections bound to the same
Worker from making progress.

## 12. Compatibility rules

- Version 2 accepts only the exact version value `2`.
- Additive semantics for flags require a new written compatibility rule before use.
- Changing a field width, byte order, header length, opcode meaning, or response ordering requires a new protocol version.
- Encoders must emit canonical zero values for unused and reserved fields.
- Golden byte fixtures for every opcode and malformed-size boundary are required before declaring the protocol externally stable.

## 13. Minimal client algorithm

1. Connect over TCP.
2. Send `INIT`; verify version, status, and identification value.
3. Record `worker_count` and `routing_epoch`.
4. Send `BIND_WORKER` for a valid Worker.
5. Send framed requests and read framed responses in order.
6. On `WRONG_OWNER`, use a connection bound to `owner_worker`.
7. On disconnect after a mutation, apply application-specific retry/idempotency policy; protocol v2 has no deduplication token. Official clients classify outcomes and automatic retries per [client semantics v1](client-semantics-v1.md).
