# ADR 0011: Durable group commit

- Status: accepted
- Date: 2026-07-15

The alpha storage engine adds `durable_group` and shared Segment batching used by `durable_periodic`.
ADR 0008 remains authoritative for per-record strict semantics. ADR 0010 remains authoritative for
periodic visibility semantics.

## Motivation

`durable_periodic` removed per-write synchronization but still performs one commit-slot `pwrite`
per record. Group commit batches multiple records into one commit-slot publication and shares the
two ordered persistence barriers across the complete batch, reducing synchronization overhead on
both strict and periodic paths without weakening Record-before-slot ordering.

## Decision

### Shared Segment batching

`DurableSegmentFile` separates logical append state from the last persisted commit slot:

1. `append_record` writes only Record bytes and advances logical metadata in memory.
2. `flush_pending_commit` synchronizes accumulated Record data, publishes one commit slot, and
   optionally synchronizes that slot immediately.
3. `commit_generation` advances once per batch flush, not once per Record.

### `durable_group`

Add `StorageMode::durable_group` with configurable `DurableGroupConfig`:

- `max_records` (default 32)
- `max_bytes` (default 65536)
- `max_wait_ms` (default 10)

Acknowledgement occurs only after the batch containing the mutation completes the Record-data
synchronization, commit-slot write, and commit-slot synchronization. Clients may block until the
batch closes by size, byte threshold, or `max_wait_ms`. There is no durability loss window.

### `durable_periodic` batching

`DurablePeriodicConfig` uses a throughput-oriented default of 4096 records, 4 MiB, and 1000 ms.
Visibility remains immediate after in-memory publication; ordered Record and commit-slot
synchronization is deferred to batch thresholds, `sync_interval_ms`, `Store::flush()`, or orderly
shutdown.

### Lifecycle

Bootstrap, manifest publication, rotation, sealing, and `Store::flush()` force
`flush_pending_commit(immediate)` before proceeding.

## Configuration

```cpp
struct DurableGroupConfig {
    std::uint32_t max_records{32};
    std::uint32_t max_bytes{65536};
    std::uint32_t max_wait_ms{10};
};
```

All three parameters must be greater than zero when batching is enabled.

## Recovery

Recovery selects the newest valid commit slot present after restart. Record data is synchronized
before that slot is published, so a surviving slot cannot authorize unsynchronized Record bytes.
Only completion of the final slot synchronization is a client-visible durability guarantee.

## Consequences

- Strict throughput improves with larger batches at the cost of batch latency.
- Periodic throughput improves by eliminating per-record commit-slot writes.
- Benchmarks and crash evidence must distinguish `durable_sync`, `durable_group`, and
  `durable_periodic`.

Normative ordering rules are specified in
[`durability-recovery.md`](../architecture/durability-recovery.md).
