# ADR 0006: Key routing hash

- Status: accepted
- Date: 2026-07-11
- Amended by: [0024](0024-offline-worker-migration.md) (offline Worker-count migration)

Worker routing uses a deterministic 64-bit FNV-1a hash over the full logical key bytes. The selected
Worker is `hash(key) % worker_count`, where `worker_count` is fixed for the process lifetime.

FNV-1a is chosen for the bootstrap because it is simple, fast, stable across supported platforms,
and depends on every key byte without requiring an external dependency. It is not a security hash;
untrusted clients are out of scope for this routing step.

The hash is also stored in each Record as metadata for future diagnostics and alternative lookup
paths. Changing the routing hash function is an architectural decision and requires a new ADR plus
compatibility tests because it changes Worker ownership for existing keys.

Durable Store creation persists the routing algorithm identifier, Worker count, and routing epoch.
Reopen rejects incompatible requested values instead of silently repartitioning existing Records.
