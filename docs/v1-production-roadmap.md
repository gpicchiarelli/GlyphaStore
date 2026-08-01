# Persistence v1 production roadmap

Ordered backlog and acceptance criteria for persistence v1, the durable runtime, the TCP daemon,
tests, operations, security, and release evidence. Last reviewed 2026-08-01.

Persistence stays on the existing **v1** Manifest, Segment header, alternating commit slots, and
Record format. Storage modes differ only in acknowledgement and batching policy. The native wire
protocol has its own independent version.

## Current status

### Paired runtime honesty (0.1.0)

`glyphastored` has a **single** daemon runtime: Reader–Writer shard pairs (ADR 0031/0032). There is
no dual-select switch and no “legacy until migration” daemon path.

| Surface | Path | Notes |
|---|---|---|
| Public embedded `Store::get` | Owning pin (`OwnedValue`) | ADR 0009; unchanged |
| Daemon GET | Borrowed Reader-local `ReadGeneration` | Adopted once per Reactor turn |
| Mutations | Bounded SPSC → serial Writer per shard pair | `Server::pair_writer_stats()` |
| CLI | `--shard-pairs` canonical; `--workers` alias | Same count as Manifest/wire `worker_count` |
| Lab | `src/experimental/paired_*` | Not installed; not reachable from `glyphastored` |

Residual P1 performance/evidence (not a second runtime): Delta mixed magnitude on Linux
hard-pinned A/B, get-into/multi-extent **rejected** pending proof, Linux 1/2/4/8 harness waiting
on `glyphastore-linux-perf`, optional Linux I/O backend **deferred** without ordering change. See
[paired-shards-plan](benchmarks/paired-shards-plan.md).

Implemented foundation: little-endian codecs, checksums, immutable Records, exact-key indexing,
descriptor-relative storage access, exclusive Store locking, preallocated fixed-size Segments,
atomic manifest replacement, bounded namespace auditing, fail-closed read validation, platform
pollers, sanitizer jobs, golden disk fixtures, and process-termination tests for v1 write
boundaries.

Open release gates (summary):

- Daemon durable mutations leave the reactor through bounded lanes; STATS latency histograms and
  maintenance rate-window needles are exported; further operability surfaces remain open.
- Store-owned TTL time is implemented; long-running native-platform clock evidence is a release gate.
- Exception translation, prepared hot-cache publication, and deterministic allocation-failure
  enumeration are implemented; native-platform evidence is a release gate.
- Cross-Segment sequence validation is implemented; released-artifact and native-platform evidence
  remain to accumulate.
- Durable compaction has crash-safe publication/retirement and `Store::compact()` scheduling;
  controlled native baselines and power-loss certification remain open (software policy closed).
- Embedded durable resource policy is implemented; per-candidate normal copy limits and
  per-second/CPU maintenance rate budgets are daemon-configurable (zero disables; pressure/emergency
  bypass). The daemon p99 guard has minimum-sample admission, 80% resume hysteresis, and a bounded
  reclaim-debt override. Phase 5 connection/handshake/principal rate limits and idle/request deadlines are
  implemented (`--secure-profile` applies defaults).
- Process-kill coverage is E2 evidence, not sudden power loss or filesystem certification (E3/E4 open).
- Offline inspect/verify/backup/repair/migrate tools exist; offline Index rebuild is refused; online
  fenced `Store::backup_to`, live-daemon wire `BACKUP` (opcode 10, admin-gated), typed C++
  `Client::backup`, and official SDK `backup` helpers are available (admission pause during flush +
  structural source check + catalog copy; destination CRC verify after resume; bounded parallel
  Segment copy; not fully hot concurrent I/O); CI greps the typed surface via
  `scripts/assert-sdk-backup-helpers.sh` and runs runtime smoke via
  `scripts/test-sdk-backup-interop.sh`. Hot
  zero-fence backup remains open. SDK tag provenance (Cosign + GitHub attestations, public /
  `ENABLE_ARTIFACT_ATTESTATIONS`) is gated; full SLSA L3 / project GPG and physical E3 remain
  open. Secure-profile
  authn/authz + Phase 5 abuse controls are
  implemented; Phase 6 auth audit + local CRL fail-closed landed — configure `--tls-crl` before
  hostile public Internet; multi-tenant Phase 8 and physical E3 remain residual (live OCSP HTTP
  unsupported).

Local throughput gains do not close these gates.

## Non-negotiable v1 invariants

Every implementation block below must preserve these rules:

1. A strict mutation is acknowledged only after its Record bytes and selected v1 commit slot meet
   the documented platform persistence ordering.
2. Recovery uses only committed v1 extents named by a valid v1 Manifest. Uncommitted tails never
   become visible.
3. Per-Worker sequence numbers are globally strict and monotonic across Segment rotation. Equal,
   overlapping, or reversed ranges are corruption.
4. Runtime state is never published before commit. A failure after commit is either represented as
   an explicit indeterminate outcome or makes that Store instance fail closed.
5. Every externally controlled length, count, offset, time, and resource total is checked before
   conversion, addition, allocation, or I/O.
6. Durable recovery never silently edits suspicious state. Verification is read-only; repair is a
   separate, explicit, auditable command.
7. Algorithms must have a documented worst-case resource bound. An optimization lands only with
   correctness tests, representative p50/p95/p99/p99.9 evidence, and no regression in crash tests.
8. Linux, macOS, FreeBSD, and OpenBSD use one logical contract but may use separately proven
   allocation, synchronization, polling, and cache strategies.

## P0 — correctness and durability blockers

### P0-01 — Make the daemon capable of v1 durability

**Status:** software-complete for embedded and TCP daemon paths; native power-loss certification
(E3/E4) remains open. `Server::create` accepts a complete `StoreConfig`, requires
Worker count to match executor count, and closes the Store from `join()`. The daemon exposes
`volatile`, `durable-sync`, `durable-periodic`, and `durable-group` selection plus durable
data-directory and open-policy controls. Durable `PUT`/`ERASE` leave the Reactor through bounded
per-Worker FIFO lanes with count and byte admission, generation-safe completion, overload responses,
queue-wait expiry before Store entry, per-lane metrics (including `queue_wait_ns` / `service_ns`
histograms), and drain-before-Store-close shutdown
(`--shutdown-drain-ms`, default 30s). Strict-group mode retains bounded concurrent producers. Wire
`HEALTH`/`READY`/`STATS` expose liveness, readiness (including maintenance emergency), and a bounded
ASCII admin report with maintenance rate-window needles. Daemon CLI exposes durable batch, resource,
and maintenance caps (including per-second/CPU rate budgets) with `--dump-config` covering effective
durable settings; file/environment config precedence and deployment profiles (`dev`, `embedded`,
`production`) validate fail-closed before listen. Secure-profile authn/authz (`--authz-map`,
`--secure-profile`) is wired. Real-daemon wire-protocol SIGKILL coverage exists for post-ack PUT,
pre-commit PUT, and post-ack ERASE (`glyphastore_crash_daemon`). Integration tests cover
emergency-gate wire `OVERLOADED` rejection and durable wire ERASE through reopen. Operator guide:
[durable TCP daemon](operations/durable-tcp-daemon.md).

**Required change:** validated `StoreConfig` into `Server`; CLI/configuration for data directory,
open policy, durability policy, batch limits, and recovery policy. Durable writes must leave the
reactor thread and complete through a bounded asynchronous path; an `fsync` must not block the event
loop. A success response may be queued only after the selected durability policy permits
acknowledgement.

**Acceptance:** restart and SIGKILL tests use the real daemon and wire protocol; strict/group
success responses always recover; pre-commit kills do not expose the mutation; disconnect after
commit is documented as indeterminate; slow storage cannot stall unrelated reactor I/O; invalid or
conflicting configuration fails before listening.

### P0-02 — Implement the documented Store clock and TTL semantics

**Status:** implemented; native-platform and long-running clock evidence remain a release gate.

**Change:** ordinary reads and durable recovery share a Store-owned clock. Production defaults to
checked Unix-epoch nanoseconds; deterministic tests inject a thread-safe `StoreClock`. Per-call
public timestamps were removed; nonzero `recovery_now_ns` is rejected. Each Store clamps backward
movement with an atomic high-water mark. Monotonic time remains reserved for batching deadlines,
never persisted absolute expiry.

**Acceptance:** an ordinary `get(key)` expires data without caller assistance; restart prunes the
same logical expirations; tests cover equality, zero/no-expiry, maximum timestamp, backward and
forward clock jumps, and conversion overflow.

### P0-03 — Close every exception and allocation boundary around commit

**Status:** implemented.

**Change:** public operations translate allocation and unexpected exceptions; background callback
exceptions stop the coordinator, fail-close the runtime, and release batch waiters. Hot-cache
key/value/node/capacity preparation occurs before persistent writes; post-commit publication uses
prepared node insertion. An isolated test executable interposes every throwing `new` form and fails
each allocation observed by the native STL build in put, update, erase, owning read, strict group
commit, and Segment rotation paths.

**Required change:** inventory every allocation and throwing operation before and after commit.
Preconstruct all fallible publication state before commit, publish prepared/no-throw nodes, or
remove the duplicating hot cache. Catch allocation and unexpected exceptions at the public API and
thread entry points. Record whether commit was reached and return a stable resource/internal/
indeterminate category or atomically fail close as appropriate. No queued pointer may outlive its
caller during cancellation, shutdown, or exception unwinding.

**Acceptance:** deterministic allocation-failure injection at every site proves: pre-commit failure
does not recover, post-commit failure never permits continued ambiguous use, all waiters are
released, and no exception terminates a worker or crosses the supported API.

**Evidence:** `glyphastore_allocation_fault_tests` creates a fresh v1 Store for every Nth-allocation
failure, checks the persistent write boundary through filesystem hooks, reopens pre-write and
interrupted-rotation states, verifies sticky fail-close after uncertain outcomes, and rejects any
steady-state allocation after `write_record`. A background `bad_alloc` test joins every strict-group
producer and proves no caller-stack pending pointer survives the failed batch.

### P0-04 — Enforce cross-Segment sequence ranges

**Status:** implemented. Recovery rejects equal, overlapping, or reversed non-empty ranges in later
manifest-ordered Segments of the same Worker before scanning their Records.

**Required change:** validate the first and last committed sequences of every non-empty Segment
against the preceding Segment in manifest order. Define empty active Segment behavior explicitly.

**Acceptance:** fixtures and corruption tests reject duplicate, overlapping, reversed, exhausted,
and inconsistent ranges, including equal sequences attached to different keys. Valid rotations and
an empty new active Segment continue to recover.

### P0-05 — Make background flush failure and shutdown observable

**Status:** implemented. Coordinator callback exceptions map to sticky `resource_exhausted`/
`internal_error` failures, stop the coordinator, fail-close the durable runtime, and release queued
batch waiters. Public `Store::close()` atomically stops admission, forces pending strict groups,
waits for already-admitted calls, performs the final flush, stops the executor, releases Store
resources and the directory lock, and returns a sticky idempotent status.

**Required change:** catch callback failures, persist a sticky fail-closed error, complete every
generation/waiter, and prevent new mutations. Add explicit idempotent `close()`/shutdown returning a
status; document the destructor as a non-throwing fallback, not the only durability barrier.

**Acceptance:** injected callback exceptions, I/O errors, stop races, maximum generation, and
concurrent `flush()`/close tests cannot deadlock or acknowledge unflushed state.

**Evidence:** tests cover repeated and concurrent close, concurrent flush/close, partial strict
groups with a 60-second normal deadline, immediate reopen while the closed Store object remains
alive, sticky final-sync failure, background callback exceptions, coordinator stop races, and
exhausted flush generations. The destructor invokes the same path but discards its status as a
non-throwing fallback.

### P0-06 — Add storage and recovery resource budgets

**Status:** implemented for the embedded v1 Store. `DurableResourceLimits` bounds peak Store bytes,
available-space reserve, Segment count, manifest bytes, Store descriptors, estimated recovery
memory, live keys, temporary compaction space, and write amplification. These are runtime policy and
do not change the v1 disk format.

**Required change:** introduce validated limits for usable disk reservation, total Store bytes,
Segment count, manifest bytes, open descriptors, recovery memory, live-key count, temporary
compaction bytes, and write amplification. Preflight bootstrap and rotation before partial state is
published. Map `ENOSPC`, `EDQUOT`, `EFBIG`, `EMFILE`, and memory exhaustion to specific stable errors.

**Acceptance:** boundary tests for every quota fail deterministically and leave a recoverable Store;
opening 256 Workers cannot reserve space without an explicit sufficient budget; recovery reports
resource exhaustion rather than terminating.

**Evidence:** bootstrap accounts for the simultaneous intent, manifest, and active Segments before
publishing the intent. Rotation accounts for the replacement Segment and both manifest generations
before sealing the old active Segment. Reopen bounds manifest allocation before decode, validates
steady Store bytes and descriptor/RLIMIT requirements, and applies a conservative key-aware recovery
memory estimator. Live-key capacity is divided into deterministic Worker-owned partitions whose sum
is the public limit. Tests cover invalid fields, configured boundaries, injected available space,
256-Worker rejection with no bootstrap intent, reusable live-key capacity, constrained recovery, and
rotation rejection while the persisted active Segment remains unsealed. Native `ENOSPC`/`EDQUOT`,
`EFBIG`, and `EMFILE`/`ENFILE` map to `storage_exhausted`, `file_too_large`, and
`descriptor_exhausted`.

### P0-07 — Complete the failure matrix beyond process termination

**Status:** deterministic in-process coverage and the evidence-collection foundation are
implemented; hardware/filesystem certification is open. v1 process-kill tests cover bootstrap, put,
periodic/group put, and rotation checkpoints. Instance-local raw I/O seams force short
`pread`/`pwrite`, `EINTR`, synchronization `EIO`, `ENOSPC`/`EDQUOT`, and `EROFS`. The
[platform durability evidence matrix](architecture/platform-durability-evidence.md) defines
cumulative E0–E4 claims, provenance, promotion rules, a collector for native E2 artifacts, and an
in-repo E3 block-reset harness for linux-ext4 loopback / macOS APFS diskimage rehearsal,
campaign orchestrator (`scripts/run-e3-campaign.sh`), honesty assert
(`scripts/assert-e3-rehearsal-honesty.sh`), and weekly CI campaign-profile / orchestrator rehearsal
(still `e3_certified=no`).
Controller caches, torn sectors, sudden physical power loss, and reviewed pinned native filesystem
rows still require dedicated lab campaigns beyond hosted CI.

**Required change:** add deterministic short-write/read, `EINTR`, delayed writeback `EIO`, `ENOSPC`,
`EDQUOT`, `EROFS`, missing file, corrupt directory entry, rename, file-sync, and directory-sync
faults. Add VM/block-device power-cut tests and a documented filesystem/mount matrix.

**Acceptance:** every persistent transition has a pre/post failure oracle; two crash matrices can
run concurrently; supported filesystem rows pass repeated power-cut recovery; unsupported remote
or user-space filesystems are rejected or prominently documented.

**Evidence:** exact-I/O tests prove retry after `EINTR`, completion after repeated short transfers,
and stable native error categories. Manifest, bootstrap intent, and Segment creation matrices verify
every pre-rename boundary. Directory-sync failure after rename is indeterminate and fail-closed.
Durable mutation tests cross write, Record sync, commit-slot write, and commit-slot sync with
`io_error`, `storage_exhausted`, and `read_only_filesystem`, then reopen and verify the recovery
oracle. Namespace recovery rejects missing catalog files, malformed names, symlinks, hard links,
and unlisted entries without adopting them. NFS, SMB, FUSE, overlay, and other remote/user-space
storage are unsupported. In-repo VM/block-device reset automation exists as a rehearsal harness
(`scripts/run-e3-block-reset.sh`, `scripts/run-e3-campaign.sh`, weekly
`durability-evidence.yml` campaign-profile / orchestrator jobs); reviewed E3 campaigns on pinned
lab disks and E4 release artifacts remain open. No row is E3/E4 certified.

### P0-08 — Implement crash-safe durable compaction in v1

**Status:** partial. Durable compaction is exposed as cooperative `Store::compact()` maintenance
with an optional Store-owned [MaintenanceController](architecture/maintenance-controller.md)
(ADR 0023). Automatic reclaim policy through Phase 3 (budgets, pressure, emergency mutation reject)
is implemented, including Phase 4 lifecycle fail-closed. Exact per-Worker sealed/live/dead byte
counters enforce the inclusive normal threshold. A finite 128 MiB default preflights candidate live
bytes before normal automatic compaction. Rotation waits on the compaction publication lease instead
of fail-fast rejecting unrelated Workers. A deterministic v1 planner treats one Worker's complete
sealed history as the atomic unit. Descriptor-relative intent publication, restart resolution
against exactly old or next authority, and online single- and multi-output crash/I/O fault matrices
are implemented. Public `Store::compact()` uses a non-queuing Store-wide maintenance gate. Durable
mutation lanes reject before enqueue when the maintenance emergency gate is armed. Official clients
map wire `OVERLOADED` to `retryability=never`. Controlled native benchmark baselines and native
power-loss certification remain open.

**Required change:** copy only the latest live v1 Records into new v1 Segments, validate the copy,
atomically publish a new v1 Manifest, sync the directory, then retire old files with a second
directory sync. Define pin/reader ownership, tombstone and TTL retention, temporary-space budget,
recovery of recognizable compaction temporaries, and an online scheduling policy.

**Acceptance:** kills at every copy, validation, manifest, rename, unlink, and directory-sync
boundary recover exactly one valid catalog; readers never observe deleted backing files; space and
write amplification remain within configured limits; no format change is introduced.

**Evidence:** planner, intent codec, filesystem, integration recovery, builder, runtime, model-
history, SIGKILL, and public integration coverage are summarized in
[crash-safe durable compaction](architecture/durable-compaction.md) and the crash harness
profiles (`standard`, `copy-matrix`, `random-matrix`).

## P1 — complete the product contract

### API, error model, and lifecycle

- Add stable categories for resource exhaustion, incompatible format, permission denial, lock
  conflict, indeterminate mutation, cancellation, and internal fail-closed state instead of
  overloading `invalid_argument`, `unavailable`, and `io_error`.
- State the `Result::value()` precondition or provide non-throwing accessors. Keep diagnostic text
  explicitly non-stable and expose structured fields where automation needs them.
- Add cancellation/deadline semantics, immutable diagnostic snapshots, build/version information,
  and recovery statistics. Do not advertise `PinnedValue` until its reader accounting and
  reclamation design is implemented and tested.
- Extend the implemented concurrent close/flush/read/mutation linearization with model/property
  histories, diagnostic lifecycle rules, and multi-Worker group completion fairness.

### Recovery scalability and read-path bounds

- Replace the implemented conservative recovery-memory estimator with allocator-accounted arenas or
  bounded sorted spill runs if measurements show unacceptable rejection or scale. A checkpointed
  derived index remains gated on crash and compatibility proofs.
- Parallelize recovery by Worker only behind descriptor, memory, and I/O-token limits. Compare
  sequential and bounded parallel scans on SSD, HDD, and constrained containers.
- Replace temporary allocating string lookups with transparent `string_view` hashing/equality.
- Cold reads now acquire an immutable exact-generation handle pin with their `RecordRef`, perform
  all positional I/O and validation without the Worker or catalog lock, and linearize at a final
  locked `Index`/pin identity check. Tests suspend `pread` while a same-Worker mutation completes and
  while compaction publishes and retires the source. Descriptor use is bounded by catalog Segments
  plus mutable Worker handles. Next, measure whether `hot_records` should be removed or replaced by
  a byte-bounded admission/eviction policy; active-Segment cold reads need an explicit pinned commit
  boundary before cached values can be safely evicted.
- Validated GET expiry now lazily removes the Index entry (`erase_no_compact`) and matching hot-cache
  row on both hot and cold paths, freeing live-key budget without a durable tombstone. Repeated GETs
  of a reclaimed expired key are Index misses. Sealed durable compaction drops Index-resident expired
  puts (`expired_records_dropped` on `CompactionResult` / `MaintenanceSnapshot`); active-Segment
  expired Index entries remain until GET reclaim or recovery. Physical TTL cleanup of sealed history
  is therefore measured. The isolated
  Local benefit/cost and concurrent-maintenance diagnostic runs cover TTL and useful/no-gain
  shapes, overwrite-driven dead-byte selection, and foreground tail cost under publication conflict.
  Normal unread-TTL scheduling is opt-in and fail-closed by default
  (`unread_ttl_normal_scheduling`); pressure/emergency probe and telemetry are closed. Remaining
  open work is controlled native baselines and native power-loss evidence under P0-08.

### Offline verification, backup, restore, and repair

- `glyphastore_inspect_segment` is a v1-aware, bounded, read-only Segment validator (header/commit
  decode, optional committed CRC scan, text/JSON, fail-closed exit codes).
  `glyphastore_verify_store` validates Manifest + namespace + every catalog Segment under an
  exclusive data-directory lock (optional `--no-scan`). `glyphastore_backup_store` performs offline
  verified backup/restore copies (lock → verify → copy catalog files → verify).
  `glyphastore_repair_store` performs offline fail-closed repair into an explicit empty workspace
  (`store/` + `quarantine/` + audit): it never mutates the source, quarantines non-catalog anomalies,
  and refuses missing catalog or unsafe namespace entries. In-place destructive rewrite remains
  forbidden. Fully concurrent hot backup (zero admission fence) remains open; online fenced
  `Store::backup_to`, typed C++ `Client::backup`, and official SDK `backup` helpers are implemented.
  `glyphastore_rebuild_index`
  permanently refuses offline Index rewrite for durable v1 with an explicit recovery/repair operator
  path; durable Indexes are rebuilt by Store recovery.
- Live/hot backup with zero writer fencing remains open; online fenced backup (C++ and official
  SDK typed helpers) and the offline contract are in [backup-restore](architecture/backup-restore.md).
  Operator procedures:
  [backup-restore runbook](operations/backup-restore.md), [corruption-repair runbook](operations/corruption-repair.md).
- Publish upgrade/downgrade rules for persistence v1 and test artifacts created by every released
  reader/writer. Worker-count changes and resharding require an offline, resumable, verified v1
  migration tool rather than an implicit open-time rewrite.

### Daemon operability and protocol

- Add bounded durable completion queues, overload responses, per-request deadlines, and fairness so
  one Worker or slow client cannot monopolize an executor.
- Define graceful drain: stop accepting, bound the drain deadline, finish or classify committed
  writes, flush/close the Store, and only then exit. Test SIGINT/SIGTERM during every phase.
  Operator procedure: [graceful-drain-and-overload runbook](operations/graceful-drain-and-overload.md).
- Add liveness, readiness, structured logs, metrics, build/config dump, and an administrative
  diagnostic surface. Readiness must fail on sticky storage errors and during unsafe recovery.
  Wire `HEALTH`/`READY`/`STATS` and `glyphastored --dump-config` are implemented, including durable
  lane latency histograms (`queue_wait_ns` / `service_ns`), `maintenance_rate_window_*` needles, and
  a bounded foreground-p99 maintenance guard with sample/p99/suspension telemetry.
  Structured JSON-lines lifecycle logging (`--log-format json`) is implemented for
  start/listen, readiness transitions, shutdown drain, maintenance emergency/fault, and executor
  failure.
- Add wire golden fixtures, reserved-bit validation, opcode-specific key/value/expiry constraints,
  stable error mapping, and compatibility tests. Duplicate `request_id` guidance (correlation-only;
  no server deduplication) and reconnect semantics (re-`INIT`/`BIND_WORKER`; indeterminate
  mutations after transport loss) are specified in [wire protocol v2 §8.1 and §10.1](spec/wire-protocol-v2.md)
  and [client semantics v1 §5](spec/client-semantics-v1.md). Protocol versioning remains independent
  from persistence v1.
- Avoid repeated vector erasure/memmove on input/output queues; use ring/slab buffers with bounded
  high/low watermarks and prove behavior under partial frames, slow readers, pipelining, and
  malicious maximum-size frames.

### Security and storage namespace

- Write a threat model covering local users, malicious persisted bytes, untrusted clients,
  symlinks/mount replacement, resource exhaustion, supply chain, backups, and crash recovery.
- Decide whether parent path symlinks are supported. If not, open every component descriptor-
  relatively; on Linux evaluate `openat2` resolution restrictions with a proven `openat` fallback.
- Keep exclusive locking explicitly advisory and certify only filesystems whose locking and
  synchronization semantics were tested. Reject or document NFS, SMB, FUSE, overlay, and networked
  volumes until certified.
- Add authentication, authorization, TLS/mTLS, secret handling, rate limits, connection/request
  quotas, and audit logging before exposing the daemon beyond a trusted loopback/private boundary.
- Add dependency and secret scanning, vulnerability-response ownership, fuzz regression intake,
  and a supported security maintenance window.

## P2 — platform optimization and engineering evidence

### Platform-specific storage and event algorithms

The existing choices are sensible starting points, but “optimal” means measured under the same
correctness contract:

| Platform | Keep as baseline | Evaluate and gate with evidence |
| --- | --- | --- |
| Linux | `epoll`, `openat`/`O_NOFOLLOW`, `fallocate`, `fdatasync`, explicit directory `fsync` | `io_uring` only if it lowers tail latency without changing commit ordering; `openat2` path confinement; `preadv2`/batched reads; ext4, XFS, btrfs and container/cgroup matrices |
| macOS | `kqueue`, `F_PREALLOCATE`, ordered `F_BARRIERFSYNC` before the v1 commit slot and `F_FULLFSYNC` for the final persistence point | APFS power-cut evidence, QoS/affinity behavior, bounded mapping/read cache, Apple Silicon performance and Intel compatibility |
| FreeBSD | `kqueue`, descriptor-relative I/O, `posix_fallocate`, `fdatasync`/`fsync` | native UFS/ZFS tests, Capsicum confinement for tools/daemon, CPU affinity and descriptor-cache tuning |
| OpenBSD | `kqueue`, `openat`, eager bounded allocation fallback, conservative `fsync` | native FFS tests, `pledge`/`unveil`, the cost of `fdatasync` being equivalent to `fsync`, and safe lower batch defaults if tail latency requires them |

Linux documents that syncing a file does not necessarily sync its directory entry, and that
writeback can report `EIO`, `ENOSPC`, or `EDQUOT` at synchronization time. Apple recommends an
ordered barrier for ordering and reserves full synchronization for stronger persistence needs,
while still describing it as best effort under sudden power loss. OpenBSD documents persistent
`EIO` failure behavior until all references close and currently implements `fdatasync` through
`fsync`. These differences must remain visible in code, fault tests, and operational support rows.

Primary references:

- [Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html)
- [Apple: Reducing disk writes](https://developer.apple.com/documentation/xcode/reducing-disk-writes)
- [FreeBSD `posix_fallocate(2)`](https://man.freebsd.org/cgi/man.cgi?query=posix_fallocate&sektion=2&manpath=FreeBSD+15.0-RELEASE)
- [OpenBSD `fsync(2)`](https://man.openbsd.org/fsync.2)
- [OpenBSD `open(2)`](https://man.openbsd.org/open.2)

### Data structures and scheduling

- The Swiss slot is now 64 rather than 80 bytes on supported 64-bit targets. Inline bytes and heap
  offset share storage, mode and length are packed, a 32-bit hash tag filters candidates, and full
  key bytes remain the collision authority. A 1M-entry ARM64 run reduced RSS by about 31.5 MiB,
  matching 16 bytes across 2,097,152 allocated slots; controlled repetitions and x86-64 evidence
  remain required. Continue benchmarking load factor, control groups, probe lengths, tombstone
  cleanup, and transparent lookup. Add scalar/SSE2/AVX2/NEON implementations only with runtime or
  compile-time dispatch and identical property/fuzz results.
- Make group commit deadline-driven and fairness-aware. Record batch occupancy, queue delay, sync
  duration, leader work, and per-Worker starvation. Adaptation may change batch size within explicit
  min/max and latency SLOs, never acknowledgement semantics.
- Evaluate bounded read-ahead and mapping by access pattern. Never map unvalidated offsets and never
  let an unbounded mapping/descriptor cache become the memory policy.
- Partition compaction by live ratio and write cost; use a priority queue or tiered policy with
  explicit maximum space amplification. Prevent a hot key range from starving cold reclamation.
- Evaluate NUMA-local Workers, allocator arenas, CPU affinity, and cache-line layout on multi-socket
  Linux. Defaults must remain portable; topology tuning is opt-in until automatically reliable.

### Benchmark contract

- Define workloads for point read/write/erase, mixed Zipf, TTL, rotation, recovery, compaction,
  cold and warm cache, sealed-Segment random reads, disk-full approach, network pipelining, slow
  clients, reconnect, and long-running memory stability.
- Report throughput with p50/p95/p99/p99.9, queue and sync time, RSS/peak RSS, disk bytes, write
  amplification, batch occupancy, recovery time, and confidence/variance. Fix hardware, governor,
  filesystem, mount options, compiler, commit, temperature/warmup, and concurrency.
- Establish regression gates separately for throughput and tail latency. GitHub-hosted measurements
  are smoke/regression signals, not publishable cross-machine claims.
- Decide whether machine-local benchmark results belong in Git. If retained, require schema,
  hardware/compiler/filesystem metadata, source commit, reproducible command, and retention policy;
  otherwise publish them as CI/release artifacts.

### Test and CI matrix

- Run fuzzers, do not only compile them. Retain seed/regression corpora for Record, protocol,
  Manifest, Segment header/commit, namespace, and recovery state machines.
- Add coverage generation and a risk-based gate; add nightly stress/soak/chaos and rotation/
  compaction workloads. Keep unit, integration, compatibility, crash, filesystem, network, and
  performance labels independently runnable.
- Add native FreeBSD and OpenBSD jobs, Linux ARM64, macOS Intel while available, and explicit Release
  tests. Use emulation only for codec portability; it is not storage certification. Decide and test
  the support policy for big-endian and 32-bit readers.
- Partition sanitizer work so ASan/UBSan, TSan, and crash matrices are independently attributable.
  Add MSan only with a fully instrumented supported toolchain.
- Add filesystem jobs for ext4/XFS/btrfs, APFS, UFS/ZFS/FFS where available, with mount metadata in
  artifacts. Use `scripts/collect-durability-evidence.sh` for attributable E2 process-kill artifacts
  and `scripts/run-e3-block-reset.sh` for disposable linux-ext4 / macOS APFS block-reset rehearsal
  (always `e3_certified=no` until a reviewed pinned campaign). Add disk quota and tiny block-device
  jobs.

### Build, hardening, and supply chain

- Apply PIE to executable link steps, not only compilation, and verify PIE, RELRO, immediate binding,
  stack protection, and fortification in CI. Make `_FORTIFY_SOURCE=3` configuration-aware and prove
  the optimized build actually enables it.
- Pin third-party Actions to verified full commit SHAs; full SHA is GitHub's immutable-action
  recommendation. Pin Python build dependencies and runner/compiler images, then use an automated,
  reviewed update process.
- Upload CodeQL/SARIF to code scanning when repository settings permit it instead of preserving only
  an artifact. Fail or warn on unexpectedly low analyzed-source coverage.
- Checksums + SPDX SBOM for packaged SDK artifacts are gated in CI
  (`.github/workflows/supply-chain.yml`, `SYFT_REQUIRED=1`). Tag pushes keyless-sign blobs with
  Cosign/Sigstore (`.cosign.bundle`). Tag/dispatch/weekly runs verify two-pass digests and a second
  Linux builder (`ubuntu-22.04`) against the primary sums. Residual: non-Linux release hosts if
  added later.
- Test installation/uninstallation, static/shared policy, symbol visibility, consumer exceptions/RTTI
  compatibility, and the declared compiler/standard-library matrix. Continue to avoid an ABI promise
  before 1.0.

Primary supply-chain references:

- [GitHub secure use of Actions](https://docs.github.com/en/actions/reference/security/secure-use)
- [GitHub artifact attestations and SBOM](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
- [GitHub CodeQL result upload](https://docs.github.com/en/code-security/tutorials/customize-code-scanning/upload-results)

## P3 — release lifecycle and later capabilities

- Define alpha, beta, release-candidate, and stable support periods with upgrade, downgrade,
  deprecation, end-of-life, and incident-response policies.
- Produce signed source and supported-platform binary releases with reproducible provenance and a
  compatibility suite that consumes earlier released v1 artifacts.
- Add online resharding, replication, high availability, or distributed consensus only as separate
  projects with their own failure models. None is required to finish single-node persistence v1.
- Consider zero-copy pinned reads only after durable compaction has correct reader-generation
  accounting. Consider a persistent derived-index checkpoint only after plain v1 log recovery is
  fully bounded and serves as the independent truth.

## Ordered execution blocks

1. **Correctness closure:** P0-02 through P0-07, stable error outcomes, explicit close, and complete
   fault injection. This freezes what a v1 acknowledgement and recovery mean.
2. **Durable lifecycle:** P0-08, TTL reclamation, verification, backup/restore, and large-store
   resource bounds.
3. **Durable daemon:** P0-01, asynchronous completion, protocol outcomes, graceful drain, readiness,
   and overload behavior.
4. **Security and operations:** namespace threat model, access control/TLS, logs, metrics, runbooks,
   and supported filesystem policy.
5. **Platform optimization:** architecture-specific Index/poller/storage experiments under the full
   correctness suite and controlled tail-latency benchmarks.
6. **Release evidence:** native platform matrix, running fuzz/soak/power-cut suites, compatibility
   artifacts, hardened reproducible builds, SBOM, signatures, and provenance.

No later block may weaken an earlier gate. In particular, daemon work does not bypass embedded
correctness, and performance work does not alter persistence ordering without a reviewed ADR and
new crash/power-loss evidence.

## Definition of production-ready v1

Persistence v1 is production ready when all P0 items are complete; every supported platform and
filesystem has automated recovery evidence; all resources have tested limits; backup/restore and
verification are usable; the daemon, if shipped as durable, acknowledges exactly the documented
commit state; security and operational controls exist; and released v1 artifacts pass forward and
backward compatibility tests within the published policy.
