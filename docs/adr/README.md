# Architecture Decision Records

ADRs preserve why a durable architectural choice was made. They are historical records, not mutable
implementation guides.

## Lifecycle

Allowed statuses are:

- `proposed`: under review and not authoritative;
- `accepted`: authoritative unless superseded;
- `amended`: still authoritative together with named later ADRs;
- `superseded`: retained for history but no longer governs new work;
- `rejected`: considered and deliberately not selected;
- `deprecated`: still implemented temporarily but scheduled for removal.

An accepted ADR is never silently rewritten to pretend a later decision existed. Add `Amended by`,
`Superseded by`, or a new consequences section that clearly identifies the later decision.

## Required structure

Use [template.md](template.md). Every new ADR must describe context, decision drivers, considered
alternatives, decision, consequences, compatibility impact, verification, and relationships.

## Index

| ADR | Decision | Status | Relationships |
|---|---|---|---|
| [0001](0001-project-scope.md) | Narrow native exact-key project scope | accepted | — |
| [0002](0002-fixed-segment-size.md) | Fixed 64 MiB Segments | accepted | — |
| [0003](0003-log-indexed-storage.md) | Append-only Records plus positional Index | accepted | — |
| [0004](0004-index-as-derived-state.md) | Index is rebuildable acceleration state | accepted | — |
| [0005](0005-worker-auto-sizing.md) | Automatic creation-time Worker sizing | accepted | depends on 0006 for durable ownership |
| [0006](0006-key-routing-hash.md) | Persisted FNV-1a-64 routing v1 | accepted | security limitations tracked in threat model |
| [0007](0007-swiss-table-index.md) | SwissTable-style Worker Index | accepted | detailed by Index v1 specification |
| [0008](0008-alpha-persistence-contract.md) | Base volatile/sync/periodic persistence contract | amended | amended by 0010 and 0011 |
| [0009](0009-public-read-ownership.md) | Owning public reads; pinned reads reserved | accepted | pinned API remains unimplemented |
| [0010](0010-durable-periodic-policy.md) | Periodic durability policy | amended | amended by 0011 batching |
| [0011](0011-durable-group-commit.md) | Strict and periodic group batching | accepted | amends 0008 and 0010 |
| [0012](0012-worker-affine-reactors.md) | One Reactor/executor per Worker; one-time connection handoff | accepted | depends on 0006; exposed by 0013 |
| [0013](0013-native-wire-protocol-v2.md) | Explicit native binary protocol v2 | accepted | exposes 0006 and 0012 |
| [0014](0014-crc32c.md) | CRC32C Castagnoli parameters and role | accepted | persistent-format dependency |
| [0015](0015-whole-worker-compaction.md) | Whole-Worker sealed-history durable compaction | accepted | preserves 0003/0004/0008 |
| [0016](0016-bounded-cold-read-executor.md) | Bounded async durable cold reads outside Reactors | accepted | amends 0012; preserves 0013 ordering |
| [0017](0017-bounded-durable-hot-cache.md) | Bounded durable hot cache with pinned active fallback | accepted | preserves 0008; builds on 0016 |
| [0018](0018-bounded-durable-mutation-lanes.md) | Bounded Worker-affine durable mutation lanes | accepted | preserves 0008, 0011, 0012 |
| [0019](0019-client-error-retry-timeout.md) | Official client error/retry/timeout contract | accepted | detailed by client semantics v1; depends on 0013 |

Official TCP client error taxonomy, automatic retries, and deadline behavior are governed by
[ADR 0019](0019-client-error-retry-timeout.md) and [client semantics v1](../spec/client-semantics-v1.md).
Embedded Store `ErrorCode` values remain a separate surface.
