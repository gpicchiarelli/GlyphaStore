Status: normative for official GlyphaStore TCP clients and embedded ErrorCode mapping
Applies to: wire protocol v2, C++ client, Python/Go/Perl/Ruby/Erlang SDKs, embedded Store errors that surface as wire statuses
Owner: networking maintainers
Last reviewed: 2026-08-01
Requirement: `GS-PROTO-ERROR-001`

# Error taxonomy v1 — wire, embedded, SDK

This specification is the **single normative authority** for the portable status → category →
retryability contract. It authorizes:

1. wire `ResponseStatus` integers in [wire protocol v2](wire-protocol-v2.md);
2. C++ `ErrorCode` values that map onto portable categories when exposed by the TCP client;
3. official SDK structured errors (exception class, `Error` type, or blessed map).

Behavioral narrative, automatic retry rules, deadlines, and pipeline positional outcomes remain in
[client semantics v1](client-semantics-v1.md) (ADR 0019). This document freezes the **cross-language
vector table** and the shared fixture contract. When texts diverge, prefer the stricter fail-closed
reading and open an ADR.

## 1. Portable categories (closed set)

| Category | Meaning |
| --- | --- |
| `invalid_argument` | Local validation failed, or wire `INVALID_REQUEST` / `UNSUPPORTED`. |
| `not_found` | Wire `NOT_FOUND`. |
| `overloaded` | Wire `OVERLOADED`, or local admission/resource limit before send. |
| `permission_denied` | Wire `PERMISSION_DENIED`. |
| `unavailable` | Client closed, routing metadata changed, or wire `NOT_BOUND` on a bound session. |
| `transport` | Socket I/O, connect failure after dial, peer EOF, or request deadline on the socket. |
| `protocol` | Framing/codec failure, mismatched `request_id`, `WRONG_OWNER`, non-empty mutation `OK`. |
| `internal` | Wire `INTERNAL_ERROR`, or unrecoverable client inconsistency. |

Language surfaces may use exceptions (Python), `Result`/`ErrorCode` (C++), or hashes/maps (Perl,
Erlang). The **category string** is the portable contract.

## 2. Wire status → category, mutation outcome, retryability

Golden vectors: [`tests/fixtures/error_taxonomy_v1.json`](../../tests/fixtures/error_taxonomy_v1.json).

| Wire status | Value | Category | Standalone `PUT`/`ERASE` outcome | Read retryability | Mutation retryability (bytes sent > 0) | Pipeline outcome | Unhealthy |
| --- | ---: | --- | --- | --- | --- | --- | --- |
| `OK` | 0 | (success) | `committed` only if value empty | — | — | `succeeded` | no |
| `INVALID_REQUEST` | 1 | `invalid_argument` | `rejected` | `never` | `reconcile_first` | `failed` | no |
| `UNSUPPORTED` | 2 | `invalid_argument` | `rejected` | `never` | `reconcile_first` | `failed` | no |
| `INTERNAL_ERROR` | 3 | `internal` | `indeterminate` | `new_attempt` | `reconcile_first` | `indeterminate` | no |
| `NOT_FOUND` | 4 | `not_found` | `rejected` | `new_attempt` | `new_attempt` | `failed` | no |
| `OVERLOADED` | 5 | `overloaded` | `rejected` | `never` | `never` | `failed` | no |
| `WRONG_OWNER` | 6 | `protocol` | `rejected` | `new_attempt` | `reconcile_first` | `failed` | **yes** |
| `NOT_BOUND` | 7 | `unavailable` | `rejected` | `never` | `never` | `failed` | **yes** |
| `PERMISSION_DENIED` | 8 | `permission_denied` | `rejected` | `never` | `never` | `failed` | no |

Notes:

- Mutation retryability in the table assumes a response was decoded after `bytes_sent > 0`.
- `OVERLOADED` collapses admission pressure and durable capacity exhaustion; clients must not infer
  which cause applied ([client semantics §3](client-semantics-v1.md)).
- Unknown numeric statuses (outside 0–8): response codecs reject after the full frame is buffered
  (stream stays synced). Defensive `from_status` / `error_from_wire_status` helpers map them to
  category `protocol`, read retryability `new_attempt`, mutation outcome `indeterminate`, and keep
  the original numeric `wire_status`. Shared fixture includes `unknown_wire_status` (99).

## 3. Embedded `ErrorCode` → portable category (C++ TCP client)

When the C++ TCP client maps a wire status or local failure into `glyphastore::Error`, the portable
`Error::category` must be one of the §1 names. Normative collapses used by the official client:

| `ErrorCode` | Portable category |
| --- | --- |
| `invalid_argument`, `record_too_large` | `invalid_argument` |
| `not_found` | `not_found` |
| `resource_exhausted` | `overloaded` |
| `unavailable` | `unavailable` |
| `io_error` | `transport` |
| `corrupted_data` | `protocol` |
| `internal_error` (and unlisted defaults) | `internal` |
| wire `PERMISSION_DENIED` | `permission_denied` (category set explicitly) |

Store-embedded paths may leave `category` / `retryability` empty; TCP clients must populate them
when returning errors to applications ([client semantics §2.1](client-semantics-v1.md)).

## 4. Conformance

Official SDKs and the C++ client must pass the shared fixture matrix:

| Runtime | Test |
| --- | --- |
| C++ | `tests/unit/client_error_taxonomy_tests.cpp` |
| Python | `sdk/python/tests/test_error_taxonomy.py` |
| Go | `sdk/go/client/error_taxonomy_test.go` |
| Perl | `sdk/perl/t/03-error-taxonomy.t` |
| Ruby | `sdk/ruby/test/test_error_taxonomy.rb` |
| Erlang | `sdk/erlang/test/glyphastore_error_taxonomy_SUITE.erl` |

Fixture copies under `sdk/*/…/fixtures/` (or Go `testdata/`) are synced from
`tests/fixtures/error_taxonomy_v1.json` via `scripts/sync-sdk-fixtures.sh`.

## 5. Residual gaps

- Transport / deadline / local-validation vectors beyond the shared fixture remain covered by
  client-semantics narrative tests, not this fixture.
