# MaintenanceController

Status: Phase 5 critical fail-closed + lifecycle; Phase 6 wire retry mapping (ADR 0023 / 0019)
Applies to: embedded Store and glyphastored
Owner: persistence maintainers
Last reviewed: 2026-07-23

# Overview

`MaintenanceController` optionally schedules Store physical maintenance. It does **not** replace
`Store::compact()`. The durable whole-Worker transaction and volatile selective vacuum remain the
sole compaction primitives (ADR 0015).

## Modes

| Mode | Embedded default | Daemon default | Thread |
| --- | --- | --- | --- |
| `cooperative` | yes | no | none; caller invokes `compact()` |
| `background` | no | yes | one Store-owned `std::jthread` |
| `disabled` | no | no | none; `compact()` still available |

## Phase 5 behavior (current)

In `background`, the controller:

1. Observes catalog-level stats (`MaintenanceObservation`) without scanning Worker Indexes or
   reading Segment Records. Recovery and already-serialized Index publication, lazy expiry,
   rotation, and compaction maintain exact active/sealed Index-referenced byte counters. The
   observation selects the next round-robin Worker with sealed history and reports its sealed,
   live, dead, and dead-ratio counters. Observation still runs when auto-compact is disabled so
   emergency admission stays fail-closed.
2. Classifies pressure via `classify_maintenance_pressure` (highest severity wins):
   - **emergency** when `segment_count >= max_segment_count`, or when available free bytes cannot
     cover `reserved_free_bytes + rotate_additional_bytes` (create/rotate at risk; catalog sets
     `rotate_additional_bytes` to Segment + next manifest size);
   - **segment pressure** when `segment_count >= ceil(max_segment_count * segment_count_pressure_pct / 100)`;
   - **free-space pressure** when `available_free_bytes <= reserved_free_bytes + free_bytes_pressure_margin`
     and emergency does not already apply. With default margin `0`, free-space pressure is only
     reachable when margin is raised above rotate headroom.
3. **Normal** policy: skip `no_candidate`; skip a candidate whose dead ratio is below the inclusive
   `dead_byte_ratio_bp_normal` threshold; backoff after `max_no_gain_attempts`; preflight the
   candidate's exact Index-referenced live Record bytes against `max_copy_bytes_per_cycle`; mid eval
   interval. The selected Worker identity is passed to the automatic compact call so policy and
   execution address the same round-robin candidate. The default limit is 128 MiB per evaluation
   (at most one compaction); equality is allowed and zero explicitly disables the limit. A rejected
   candidate reports `copy_budget`, distinct from no-gain `budget_backoff`. The durable runtime
   rechecks the same inclusive limit while holding the Worker snapshot lock; growth between
   observation and snapshot returns a finite `sequence_conflict` before scan or copy.
4. **Pressure / emergency** policy: use `min_eval_interval_ms`; continue compact attempts despite
   no-gain / copy budgets; record activation reason (`segment_pressure` / `free_space_pressure` /
   `emergency_capacity`).
5. **Emergency mutation gate**: publish `mutations_rejected`; `Store::put` / `Store::erase` (and
   exclusive `StoreAccess` paths) return `ErrorCode::storage_exhausted` with a stable message.
   `get`, `flush`, `compact`, and `close` continue. No mutation queue.
6. **Fail-closed on reclaim fault**: compact/observe faults **keep** an already-published emergency
   gate. While the gate is armed, auto-compact is **not** latched off — reclaim keeps retrying under
   budget. Non-emergency faults still disable auto-compact. `faulted` remains sticky for telemetry
   until a later evaluation recovers or `request_stop` runs. The gate clears only on a later
   non-emergency observation, `unavailable`/`store_closed`, or `request_stop`/`join`.
7. Still at most one `Store::compact()` in flight (shared try-lock).
8. Background start requests an immediate first evaluation (no mid-interval blind window).
9. Free-space emergency uses `rotate_additional_bytes` from catalog observation (Segment + next
   manifest), matching `rotate_active` headroom.

Telemetry in `MaintenanceSnapshot` includes pressure level, `mutations_rejected`, activation reason,
eval/compact durations, bytes/records copied, `expired_records_dropped` (last and total), suspend
count, time since last useful compaction, candidate Worker, candidate sealed/live/dead Record bytes,
and dead ratio in basis points. Exact no-gain planning scans also expose last/total
`source_records_verified`, `source_bytes_verified`, and `expired_records_dropped` examined before
the layout rejected a rewrite; cheap policy skips (`reclaim_threshold`, `copy_budget`,
`no_candidate`) do not update those fields. Durable snapshots also include rotation attempts, commits,
compaction waits, final-Record attempts/commits, and last/total/maximum durations for publication
wait, Segment seal, replacement Segment creation, Manifest publication, aggregate execution,
complete rotation, and the post-rotation final Record commit. Daemon `STATS` exports candidate,
no-gain planning, skip-reason, and rotation counters.

The live-byte counter means “currently Index-referenced,” not “guaranteed unexpired at observation
time.” Expiry discovered by validated GET immediately updates it; cold, unread TTL entries remain
conservatively live until GET, recovery, or compaction visits the Record. Under pressure or
emergency, an optional bounded probe (`unread_ttl_pressure_probe`, default on) reads sealed source
Records for the round-robin candidate only and exports
`candidate_unread_expired_sealed_record_{count,bytes}` plus `unread_ttl_probe_performed` in
`MaintenanceObservation` / daemon `STATS`. Normal scheduling stays conservative by default
(`unread_ttl_normal_scheduling`, default off): unread expired sealed puts remain Index-live for the
inclusive dead-byte threshold until GET, recovery, pressure, or an explicit `Store::compact()` visit.
When normal scheduling is enabled, normal evaluations also probe and add unread expired bytes to
`candidate_scheduling_dead_byte_ratio_bp` for threshold decisions only; copy budget still uses exact
Index-referenced live bytes and compaction still uses the sole `Store::compact()` path.

Wire note: the Reactor maps `storage_exhausted` to `ResponseStatus::overloaded` (existing
many-to-one collapse with admission limits). Official clients advertise `retryability=never` for
`overloaded` because the wire cannot distinguish capacity exhaustion from queue pressure. Applications
that still want backoff-retry for true admission overload must opt in explicitly.

## Shutdown

`Store::close()`:

1. stops admission of new Store operations;
2. `MaintenanceController::request_stop()` (clears callbacks, clears the mutation gate, wakes the
   thread);
3. requests durable flush and waits until no admitted operations remain (including an in-flight
   `compact()`);
4. `join()`s the maintenance thread;
5. closes the durable runtime and releases Store resources.

## Concurrency

Manual and automatic compact share `compaction_mutex` with `try_to_lock`. A busy gate returns
`sequence_conflict`. There is no compaction queue.

The automatic candidate cursor advances when a Worker is observed, not only when compaction starts.
Threshold, copy-budget, or other policy skips therefore cannot pin evaluation to one Worker and
starve reclaimable peers. `MaintenanceSnapshot::sequence_conflicts` and daemon
`maintenance_sequence_conflicts` count compact attempts rejected by concurrent state change.

## Explicitly deferred

- Controlled-hardware baselines and native power-loss certification (owned by ADR 0015 compaction
  transaction, not this scheduler). Related matrices:
  [durable compaction](../benchmarks/durable-compaction-2026-07-23.md),
  [concurrent maintenance](../benchmarks/concurrent-maintenance-2026-07-23.md),
  [rotation/idle/churn](../benchmarks/maintenance-rotation-idle-churn-2026-07-23.md),
  [macOS phase attribution](../benchmarks/maintenance-rotation-phases-macos-2026-07-23.md),
  [deep-phase macOS](../benchmarks/maintenance-rotation-deep-phases-macos-2026-07-23.md).
- Shorter compaction publication leases. Measure deep rotation phases first; only then decide
  whether replacement Segment construction should move before publication authority.

## Rate and CPU budgets

Under normal pressure, `max_copy_bytes_per_sec` and `max_cpu_ms_per_window` share a one-second
`steady_clock` window. Zero disables each limit. When exhausted, evaluation skips with
`rate_budget` and suspends until the next evaluation after the window refreshes. Pressure and
emergency bypass both budgets (same as the per-cycle copy limit). Window consumption is exported
through `MaintenanceSnapshot` and daemon `STATS`
(`maintenance_rate_window_bytes_copied` / `maintenance_rate_window_cpu_ns`). Daemon flags:
`--maintenance-max-copy-bytes-per-sec` and `--maintenance-max-cpu-ms-per-window`.
