# Changelog

- Started P0-08 with a deterministic whole-Worker durable compaction planner, generation-safe v1
  manifest replacement, physical temporary/peak/amplification gates, a checksummed dual-manifest
  intent codec, descriptor-relative crash-classified intent publication/removal, and restart
  resolution against exactly the old or next authority with validated, idempotent Segment
  retirement. Add an exact Record-boundary layout and a durable builder that prebuilds the new
  Index, preserves visible v1 Record bytes and sequences, reclaims expired/superseded/tombstoned
  history, validates sealed replacements, and classifies post-intent failures for rollback. Runtime
  installation now atomically publishes the prepared manifest, commit catalog, and Worker Index,
  retires old sources, serializes competing manifest authorities, keeps other-Worker descriptor
  caches stable by immutable identity, and fails closed when restart recovery is required. Public
  `Store::compact()` maintenance now selects Workers round-robin without a background thread or
  queued concurrent requests, skips exact no-gain layouts, executes at most one transaction per
  call, and returns copy statistics. Production reclaim tuning and native power-loss certification
  remain open.

All notable changes will be documented here. GlyphaStore follows Semantic Versioning once a stable
public API exists.

## [Unreleased]

- Replace fail-fast unrelated-Worker rotation during a durable compaction lease with condition-based
  serialization: the rotation now waits, rebuilds from the newly published Manifest, commits, and
  survives reopen without changing persistence v1. Advance the automatic round-robin cursor for
  every observed candidate so a below-threshold Worker cannot starve reclaimable peers, and expose
  a cumulative maintenance sequence-conflict counter through snapshots and daemon `STATS`. Extend
  the maintenance benchmark with forced-rotation, idle, and sustained-churn scenarios. On the clean
  seven-repeat macOS/APFS follow-up, all forced rotations commit without foreground errors; the
  serialized boundary costs about 2.5x median latency, product-default idle duty is about 0.0018%,
  and seven 1 GiB churn samples finish with four Segments instead of 22 at a 2.9% median throughput
  cost. Add lock-free runtime rotation telemetry for attempts, commits, compaction waits, and
  last/total/maximum publication-wait, execution, and total durations; surface it through
  `MaintenanceSnapshot`, daemon `STATS`, and phase-aware maintenance CSV.
- Add a reproducible concurrent-maintenance benchmark comparing disabled, cooperative, and
  Store-owned background policy under a synchronized mixed GET/PUT workload. Record a clean
  seven-repeat macOS/APFS matrix with raw CSV: both maintenance modes complete the same 31.01 MiB
  useful compaction without conflict, while median foreground throughput falls about 18% and p99
  rises 54--57% versus disabled. Cooperative and background medians are effectively equal. A
  rotation-forcing calibration identified the unrelated-Worker fail-fast publication conflict
  subsequently closed and measured by the follow-up above.
- Enforce `dead_byte_ratio_bp_normal` for normal durable maintenance using exact per-Worker
  Index-referenced active/sealed Record-byte counters maintained across recovery, mutation, lazy
  expiry, rotation, compaction, and reopen. Observe the next round-robin candidate without scanning
  its Index, pass that exact Worker to automatic compaction, retain pressure/emergency threshold
  bypass, and export candidate sealed/live/dead bytes plus basis-point ratio through
  `MaintenanceSnapshot` and daemon `STATS`.
- Make `max_copy_bytes_per_cycle` a preventive per-candidate limit for normal durable background
  maintenance instead of an after-the-fact accumulated counter. Set a finite 128 MiB default
  derived from the first compaction matrix, allow equality, define zero as explicitly unlimited,
  retain pressure/emergency bypass, report a distinct `copy_budget` policy reason, and expose the
  setting through daemon CLI/config/environment plus `--dump-config`.
- Add a dedicated public `Store::compact()` benchmark with high/medium/low reclaim, copy-heavy,
  50% TTL, and no-gain workloads. Each fresh-Store sample closes, reopens, verifies the Index, and
  checks its complete key model; CSV output includes segment/byte reclamation, copied and expired
  records, elapsed time, and effective scan/copy rates. Record the first seven-repeat exploratory
  macOS/APFS result and use it to drive the per-Worker dead-byte enforcement and finite normal copy
  limit above. No-gain work lacks public counters, unread TTL remains conservative under normal
  policy, and concurrent foreground cost is measured by the dedicated follow-up matrix.
- Add the platform durability evidence matrix with cumulative E0–E4 claim levels, an honest
  APFS/Linux/BSD row inventory, artifact/promotion requirements, and a controlled power-loss
  campaign protocol. Add a portable collector that records source, OS, hardware class,
  filesystem/mount, toolchain, commands, results, and SHA-256 provenance while limiting itself
  explicitly to metadata or process-kill evidence.
- Extend compaction recovery beyond the single-output fixture: interrupt rollback between two
  replacement unlinks and roll-forward between three source unlinks, then prove that an ordinary
  reopen preserves the selected manifest authority and completes every remaining cleanup. Add the
  same two-output cleanup transitions to the SIGKILL harness. Exercise the online multi-output path
  end to end with 64
  maximum-size live Records compacted from three sources into two replacements and reopened. Add
  four reproducible model histories covering 608 total PUT/ERASE/TTL operations; each reports its
  seed and must agree before compaction, after installation, and after reopen. Add five faulted
  seeds covering another 760 operations across intent, Record-copy, Manifest-sync, source-retirement,
  and intent-removal failures, with exact outcome/health/authority and reopen-model checks. Replace
  the minimal online crash seed with a deterministic 30-operation PUT/ERASE/TTL history and verify
  its complete eight-key model after every one of the 25 compaction SIGKILL checkpoints. Add 15
  differential online 3-to-2 SIGKILL checkpoints for second-replacement creation, its final Record
  and data/seal commits, shifted Manifest/retirement directory syncs, and the third source unlink,
  bringing the sync matrix to 91 occurrence-specific checkpoints. Make rollback remove both the
  canonical and partial temporary name for every obsolete replacement identity so a crash during
  second-output creation reopens with the old authority and a clean namespace. Batch the immutable
  source seed at Segment seal and add an opt-in `copy-matrix` covering the remaining 63
  `write_record` occurrences; together the standard and exhaustive profiles kill after all 154
  distinct checkpoints, including every one of the 64 maximum-size Record copies. Add an opt-in
  `random-matrix` with four reproducible 96-operation PUT/overwrite/ERASE/TTL histories. Across 36
  process kills it checks nine old/next-authority checkpoint classes per seed and verifies the
  complete 64-value maximum-size model after recovery.
- Add the normative persistence-v1 recovery state-transition matrix covering bootstrap,
  mutation/flush, rotation, compaction, ordinary rejection rules, and the exact automated evidence
  attached to each restart outcome. Make repeated bootstrap/rotation directory and commit
  checkpoints occurrence-specific, and verify the post-rotation mutation's absent/optional/present
  recovery boundary instead of only preserving the seed key.
- Add the independently generated Compaction Intent v1 golden fixture, bind the production encoder
  and decoder to its canonical dual-Manifest transition, and close the final persistent-codec
  fixture gap in the compatibility and readiness matrices.
- Fix daemon CLI option dispatch so `--workers` and `--max-connections` retain their distinct
  values, and make the SDK interoperability matrix verify the server's effective configuration.
  Expand that matrix through 8 Workers with deterministic owner coverage, structured `NOT_FOUND`,
  and uniform local 2 MiB frame-limit rejection across C++, Python, Perl, Go, and Ruby. Stabilize
  background-maintenance tests under sanitizers, restore the strict Perl quality gate, and document
  the completed cross-SDK wire-v2 golden-fixture coverage.
- Add an installable synchronous C++ wire-v2 client with per-Worker bound connections, explicit
  committed/rejected/indeterminate mutation outcomes, canonical independent wire fixtures, and a
  public-client TCP benchmark mode. Split the public filesystem fault-hook types from persistence
  implementation headers so installed Store and client consumers compile against a closed header set.
- Fix hardening feature checks to evaluate their result variables, enforce extension-free ISO C++23
  while permitting newer standards, apply supported stack-protector and PIE flags to every binary,
  and enable optimized Linux `_FORTIFY_SOURCE=3`, RELRO, and immediate binding. A strict Release CI
  job verifies both emitted compile/link commands and the resulting ELF security properties.
- Track SwissTable deleted controls and effective occupancy, reuse tombstones exactly, and perform
  bounded same-capacity cleanup before probe stability degrades. Rehash now builds an independent
  table and key arena before atomic in-memory installation, preserving the old Index on allocation
  failure; expose tombstone, effective-load, probe, rehash, and cleanup statistics plus a churn-miss
  benchmark.
- Bound the durable active-Record hot cache with deterministic global/per-Worker byte partitions,
  entry and staging limits, conservative observable accounting, and cold fallback on exhaustion.
  Hot GET now snapshots shared immutable value ownership and copies outside the Worker mutex; active
  misses use an exact generation pin plus post-I/O Index/pin revalidation. Add coverage for value
  sizes through near 1 MiB, zero/minimum budgets, overwrite, erase, TTL, rotation, and strict group
  publication.
- Move durable cold GET file I/O and CRC off Worker-affine network Reactors through a bounded shared
  executor. Prepared jobs own the exact `RecordRef` and immutable generation pin, completions return
  through bounded Reactor queues with `(slot, generation)` rejection and relinearization, saturation
  returns `overloaded`, per-connection pipeline order is preserved, and shutdown cancels queued work
  before draining in-flight reads. Add deterministic TCP coverage for blocked same-Worker progress,
  saturation, close/slot reuse, stale completion, and shutdown drain.
- Make volatile long-key erase reclaim geometrically instead of recopying the live key arena at
  every fixed 64 KiB of churn. Add strict-group record-target adaptation bounded by explicit
  `min_records`/`max_records`, contracting on deadline occupancy and growing with admitted producer
  pressure without changing acknowledgement semantics.
- Add per-directory deterministic file-I/O fault injection for short positional transfers,
  `EINTR`, writeback `EIO`, disk/quota exhaustion, and read-only filesystems; extend pre/post
  publication and mutation recovery-oracle matrices without global test state.
- Add validated durable resource limits for Store bytes, reserved free space, Segment and manifest
  counts/bytes, open descriptors, recovery memory, live keys, temporary compaction space, and write
  amplification; preflight bootstrap and rotation before publication/sealing and expose stable
  storage, file-size, and descriptor exhaustion errors.
- Add idempotent public `Store::close()` with atomic admission quiescing, in-flight operation
  draining, forced partial group closure, observable sticky final-flush errors, concurrent
  flush/close safety, cache-line-isolated per-Worker accounting, executor shutdown, and immediate
  resource/data-directory lock release.
- Make ordinary Store reads and durable recovery share a checked Unix-epoch clock, add thread-safe
  `StoreClock` injection, clamp backward movement per Store instance, and remove public per-read
  timestamp overrides that silently disabled TTL expiration by default.
- Preallocate hot-cache publication nodes before persistent writes, translate public/background
  exceptions into stable failures with fail-closed waiter release, and reject overlapping or
  reversed per-Worker sequence ranges across persistence v1 Segments.
- Add an isolated deterministic allocation-fault executable that exhaustively fails each observed
  durable put, update, erase, read, group-commit, and rotation allocation; verify recovery outcomes,
  prohibit steady-state allocation after the Record write boundary, and exercise background waiter
  release on `bad_alloc`.
- Add strict `durable_group` batching with whole-batch publication, absolute batch deadlines, a
  dedicated one-Worker v1 commit executor, bounded threshold admission, latency benchmarks,
  crash/rotation coverage, and macOS `F_BARRIERFSYNC` ordering before the final full durable flush.
- Enable public `Store::open(durable_sync)` with explicit create/open policies, persisted Worker-count
  validation, entropy-backed Store IDs, crash-recoverable bootstrap intents, and the durable
  mutation/rotation runtime behind the PImpl.
- Define the target alpha durability, recovery, routing persistence, and public read ownership
  contracts in ADRs 0008 and 0009.
- Separate the installed C++ API from engine internals with a PImpl Store, owning reads, byte-key
  overloads, and a build-tree-only server/test access bridge.
- Rename Store read benchmark result identifiers with a `_copy` suffix so owning-read measurements
  cannot be compared accidentally with the former non-owning prototype baseline.
- Add the exact little-endian Segment header v1 codec, alternating CRC32C commit-slot selection,
  fail-closed corruption/version handling, and a canonical golden fixture.
- Add the canonical manifest v1 codec with full-file CRC32C, bounded decoding, deterministic Segment
  catalog invariants, publication-generation selection, and a golden fixture.
- Add a canonical binary-key Record v1 fixture, require minimal aligned extents and zero padding on
  decode, centralize hexadecimal fixture loading, and document the format compatibility matrix.
- Add platform-aware POSIX persistence primitives with descriptor-anchored private paths, complete
  positional I/O, exclusive directory locking, strict synchronization, atomic manifest replacement,
  explicit indeterminate outcomes, and filesystem fault-injection tests.
- Add exact-size durable Segment files with platform-specific physical allocation, verified identity
  reopening, Record-before-slot synchronization, alternating commit/seal slots, bounded committed
  recovery scans, and explicit not-committed versus indeterminate fault outcomes.
- Add manifest-driven durable recovery with constant Segment-descriptor usage, single-pass committed
  Record visitation, per-Worker bounded latest-key rebuild, persisted hash/routing validation,
  sequence restoration, and interrupted-rotation detection.
- Add bounded descriptor-relative namespace audit with strict canonical Segment parsing,
  deterministic anomaly reports, crash-temporary tolerance, no-follow object checks, and fail-closed
  rejection of unlisted, malformed, unknown, unsafe, or missing entries before recovery scans.
- Add read-only durable runtime materialization with moved recovered Indexes, one cached Segment
  descriptor per Worker, read-only/no-follow reopening, per-read CRC/key/reference verification,
  sticky fail-closed corruption handling, and interrupted-rotation service refusal.
- Add the internal durable mutation state machine with preflighted allocation-free Index publication,
  ordered Record and commit-slot synchronization, explicit mutation outcomes, shared catalog
  concurrency, and exact-intent crash-safe Segment rotation completion.
- Add OpenBSD as a `kqueue` architectural target from the `0.1.0` prototype line.
- Bootstrap the C++23 architecture prototype.
- Add fixed 64 MiB append-only segments, explicit record codec, derived Index, rebuild, vacuum,
  worker sizing, tests, fuzz targets, benchmarks, and macOS/Xcode tooling.
