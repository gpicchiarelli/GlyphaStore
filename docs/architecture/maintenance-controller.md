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
| `background` | no | yes | one Store-owned `std::thread` with explicit stop/join |
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
   observation and snapshot returns a finite `sequence_conflict` before scan or copy. When
   `suspend_on_p99_latency_ms` is nonzero, the daemon also consumes a fixed lock-free histogram of
   durable mutation admission-to-completion latency and defers an otherwise eligible compaction when
   the window p99 reaches the inclusive threshold. A decision requires
   `suspend_on_p99_min_samples` (default 32); a smaller window is treated as a low-traffic reclaim
   opportunity. Once armed, the guard resumes below 80% of the threshold, preventing boundary
   oscillation. `max_latency_deferral_ms` admits one normal reclaim attempt after continuous
   deferral (default 30 s; zero defers until pressure). The p99 threshold itself defaults to zero
   (disabled).
4. **Pressure / emergency** policy: use `min_eval_interval_ms`; continue compact attempts despite
   no-gain / copy budgets; record activation reason (`segment_pressure` / `free_space_pressure` /
   `emergency_capacity`).
5. **Emergency mutation gate**: publish `mutations_rejected`; `Store::put` / `Store::erase` (and
   exclusive `StoreAccess` paths, including paired sync `put/erase_volatile_published` with
   `caller_holds_guard`, each sibling in `mutate_durable_batch`, and each item in non-paired
   `Store::put_batch`) return `ErrorCode::storage_exhausted` with a stable message. `get`, `flush`,
   `compact`, and `close` continue. No mutation queue. The sync Writer fast path skips only the
   nested `OperationGuard` RMW — not this gate — so mid-batch / TOCTOU arming still rejects before
   Store append.
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
`MaintenanceObservation` / daemon `STATS`. On the mutex-elided exclusive Writer path the probe arms
`compaction_commit_active` and waits for `hot_path_depth == 0` before enumerating the Index (same
ownership protocol as compaction Phase A), after releasing the observation catalog lock so it does
not hold catalog across the depth wait. Normal scheduling stays conservative by default
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
starve reclaimable peers. Adversarial proofs: unit
`reclaim_threshold skip advances to a reclaimable peer Worker` and integration
`background reclaim_threshold skip advances past live-only Worker to reclaimable peer`
(HAZ-026). `MaintenanceSnapshot::sequence_conflicts` and daemon
`maintenance_sequence_conflicts` count compact attempts rejected by concurrent state change.

## Explicitly deferred

- Controlled-hardware baselines and native power-loss certification (owned by ADR 0015 compaction
  transaction, not this scheduler).
- Mid-transaction pause/resume. The p99 guard prevents a normal compaction from starting under an
  already-degraded foreground window; it does not make the persistence-v1 intent/replacement
  transaction preemptible. True copy quanta require a separately specified durable staging and
  recovery protocol so throttling cannot lengthen an ambiguous publication authority indefinitely.
- Shorter compaction publication leases. Measure deep rotation phases first; only then decide
  whether replacement Segment construction should move before publication authority.

## Rate and CPU budgets

Under normal pressure, `max_copy_bytes_per_sec` and `max_cpu_ms_per_window` share a one-second
`steady_clock` window. Zero disables each limit. When exhausted, evaluation skips with
`rate_budget` and suspends until the next evaluation after the window refreshes. Pressure and
emergency bypass both budgets (same as the per-cycle copy limit). Window consumption is exported
through `MaintenanceSnapshot` and daemon `STATS`
(`maintenance_rate_window_bytes_copied` / `maintenance_rate_window_cpu_ns`). Daemon flags:
`--maintenance-max-copy-bytes-per-sec`, `--maintenance-max-cpu-ms-per-window`, and
`--maintenance-suspend-on-p99-latency-ms`, `--maintenance-suspend-on-p99-min-samples`, and
`--maintenance-max-latency-deferral-ms`. Latency feedback is consumed before a normal compaction
starts; pressure and emergency bypass it so reclamation cannot starve when capacity is at risk.
STATS exports the consumed sample count, bucket-conservative p99, guard state/age, cumulative
suspension count, and debt overrides.
