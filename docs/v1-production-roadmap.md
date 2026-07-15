# Persistence v1 production roadmap

This document is the repository-wide engineering audit and execution plan as of 2026-07-15. It
covers the public API, volatile and durable runtimes, persistent files, recovery, the TCP daemon,
tests, build and release automation, operations, security, and performance.

The persistence direction is deliberately singular: GlyphaStore keeps and completes the existing
persistent **v1** Manifest, Segment header, alternating commit slots, and Record format. Storage
modes differ only in acknowledgement and batching policy. This plan does not introduce a second
persistent format. The native wire protocol has its own independent version.

## Audit conclusion

The tree has a strong storage-engine foundation: checked little-endian codecs, checksums, immutable
Records, exact-key indexing, descriptor-relative storage access, private regular-file validation,
exclusive Store locking, preallocated fixed-size Segments, atomic manifest replacement, bounded
namespace auditing, fail-closed read validation, platform pollers, sanitizer jobs, golden disk
fixtures, and process-termination tests for the v1 write boundaries.

It is not production ready yet. The most important gaps are behavioral rather than cosmetic:

- `glyphastored` always opens a volatile Store, so durable embedded operation does not yet imply a
  durable network service;
- Store-owned TTL time is now implemented; long-running native-platform clock evidence remains a
  release gate;
- exception translation, prepared hot-cache publication, and deterministic allocation-failure
  enumeration are implemented; native-platform evidence remains a release gate;
- recovery now validates monotonic sequence ranges both inside and across all Segments owned by one
  Worker; released-artifact and native-platform evidence remains to be accumulated;
- durable Segments and manifest entries have no crash-safe compaction and retirement lifecycle;
- disk, memory, descriptor, recovery, and write-amplification budgets are not configurable;
- process-kill coverage is useful but is not evidence for sudden power loss, every supported
  filesystem, disk-full behavior, or remote/user-space filesystems;
- the offline inspection and rebuild commands are placeholders, while backup, restore, metrics,
  readiness, authentication, transport security, and release provenance are absent.

These items are release gates. Local throughput gains do not reduce their priority.

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

**Current evidence:** `Server::create` constructs `Store::open` with only a Worker count, and the
daemon CLI has no storage mode, data directory, open policy, or durability controls.

**Required change:** pass a validated `StoreConfig` into `Server`; add CLI/configuration fields for
data directory, `create_new`/`open_existing`/`open_or_create`, strict/group/periodic policy, batch
limits, and recovery policy. Durable writes must leave the reactor thread and complete through a
bounded asynchronous completion path; an `fsync` must not block the event loop. A success response
may be queued only after the selected durability policy permits acknowledgement.

**Acceptance:** restart and SIGKILL tests use the real daemon and wire protocol; strict/group
success responses always recover; pre-commit kills do not expose the mutation; disconnect after
commit is documented as indeterminate; slow storage cannot stall unrelated reactor I/O; invalid or
conflicting configuration fails before listening.

### P0-02 — Implement the documented Store clock and TTL semantics

**Status:** implemented in the current tree; native-platform and long-running clock evidence remains
part of the release gate.

**Root cause:** public `get` and `recovery_now_ns` defaulted to zero, which disabled expiry. The
server supplied wall-clock time, but the embedded API had no Store-owned injectable clock.

**Implemented change:** ordinary reads and durable recovery now share a Store-owned clock.
Production defaults to checked Unix-epoch nanoseconds; deterministic tests inject a thread-safe
`StoreClock`. Per-call public timestamps were removed and nonzero `recovery_now_ns` is rejected.
Each Store clamps backward movement with an atomic high-water mark. Monotonic time remains reserved
for batching deadlines, never persisted absolute expiry.

**Acceptance:** an ordinary `get(key)` expires data without caller assistance; restart prunes the
same logical expirations; tests cover equality, zero/no-expiry, maximum timestamp, backward and
forward clock jumps, and conversion overflow.

### P0-03 — Close every exception and allocation boundary around commit

**Status:** implemented in the current tree. Public operations translate allocation and unexpected
exceptions; background callback exceptions stop the coordinator, fail-close the runtime, and
release batch waiters. Hot-cache key/value/node/capacity preparation occurs before persistent
writes and post-commit publication uses prepared node insertion. An isolated test executable
interposes every throwing `new` form and fails each allocation observed by the native STL build in
put, update, erase, owning read, strict group commit, and Segment rotation paths.

**Root cause:** public operations did not provide a complete exception barrier. Durable publication
inserted strings and values into `hot_records` after commit, and coordinator callbacks could throw
from a background thread.

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

**Status:** implemented in the current tree. Recovery rejects equal, overlapping, or reversed
non-empty ranges in later manifest-ordered Segments of the same Worker before scanning their
Records.

**Root cause:** each Segment scan checked its own sequence order; recovery tracked a maximum but did
not reject an overlapping or reversed range in a later Segment belonging to the same Worker.

**Required change:** validate the first and last committed sequences of every non-empty Segment
against the preceding Segment in manifest order. Define empty active Segment behavior explicitly.

**Acceptance:** fixtures and corruption tests reject duplicate, overlapping, reversed, exhausted,
and inconsistent ranges, including equal sequences attached to different keys. Valid rotations and
an empty new active Segment continue to recover.

### P0-05 — Make background flush failure and shutdown observable

**Status:** in progress. Coordinator callback exceptions are translated to sticky
`resource_exhausted`/`internal_error` failures, stop the coordinator, fail-close the durable runtime,
and release queued batch waiters. An explicit public `close()` result is still missing.

**Root cause:** `DurableFlushCoordinator::run` invoked its callback without an exception barrier,
and destruction suppressed final flush errors.

**Required change:** catch callback failures, persist a sticky fail-closed error, complete every
generation/waiter, and prevent new mutations. Add explicit idempotent `close()`/shutdown returning a
status; document the destructor as a non-throwing fallback, not the only durability barrier.

**Acceptance:** injected callback exceptions, I/O errors, stop races, maximum generation, and
concurrent `flush()`/close tests cannot deadlock or acknowledge unflushed state.

### P0-06 — Add storage and recovery resource budgets

**Current evidence:** one 64 MiB active Segment is created per Worker (up to 16 GiB at 256 Workers),
and no maximum exists for disk bytes, Segment count, manifest size, recovery memory, or temporary
compaction space.

**Required change:** introduce validated limits for usable disk reservation, total Store bytes,
Segment count, manifest bytes, open descriptors, recovery memory, live-key count, temporary
compaction bytes, and write amplification. Preflight bootstrap and rotation before partial state is
published. Map `ENOSPC`, `EDQUOT`, `EFBIG`, `EMFILE`, and memory exhaustion to specific stable errors.

**Acceptance:** boundary tests for every quota fail deterministically and leave a recoverable Store;
opening 256 Workers cannot reserve space without an explicit sufficient budget; recovery reports
resource exhaustion rather than terminating.

### P0-07 — Complete the failure matrix beyond process termination

**Current evidence:** v1 process-kill tests cover bootstrap, put, periodic/group put, and rotation
checkpoints. They do not simulate controller caches, torn sectors, sudden power loss, quota, or all
filesystem implementations. The crash harness previously used shared temporary names; each run is
now isolated by process and start-time suffix.

**Required change:** add deterministic short-write/read, `EINTR`, delayed writeback `EIO`, `ENOSPC`,
`EDQUOT`, `EROFS`, missing file, corrupt directory entry, rename, file-sync, and directory-sync
faults. Add VM/block-device power-cut tests and a documented filesystem/mount matrix.

**Acceptance:** every persistent transition has a pre/post failure oracle; two crash matrices can
run concurrently; supported filesystem rows pass repeated power-cut recovery; unsupported remote
or user-space filesystems are rejected or prominently documented.

### P0-08 — Implement crash-safe durable compaction in v1

**Current evidence:** the in-memory vacuum builder exists, but durable sealed Segments and manifest
entries accumulate indefinitely and there is no retirement/deletion protocol.

**Required change:** copy only the latest live v1 Records into new v1 Segments, validate the copy,
atomically publish a new v1 Manifest, sync the directory, then retire old files with a second
directory sync. Define pin/reader ownership, tombstone and TTL retention, temporary-space budget,
recovery of recognizable compaction temporaries, and an online scheduling policy.

**Acceptance:** kills at every copy, validation, manifest, rename, unlink, and directory-sync
boundary recover exactly one valid catalog; readers never observe deleted backing files; space and
write amplification remain within configured limits; no format change is introduced.

## P1 — complete the product contract

### API, error model, and lifecycle

- Add stable categories for resource exhaustion, incompatible format, permission denial, lock
  conflict, indeterminate mutation, cancellation, and internal fail-closed state instead of
  overloading `invalid_argument`, `unavailable`, and `io_error`.
- State the `Result::value()` precondition or provide non-throwing accessors. Keep diagnostic text
  explicitly non-stable and expose structured fields where automation needs them.
- Add explicit Store `close()`, cancellation/deadline semantics, immutable diagnostic snapshots,
  build/version information, and recovery statistics. Do not advertise `PinnedValue` until its
  reader accounting and reclamation design is implemented and tested.
- Define concurrent close, flush, read, mutation, and diagnostics linearization. Add model/property
  tests for these histories and for multi-Worker group completion fairness.

### Recovery scalability and read-path bounds

- Enforce a recovery memory budget. The current full-key `unordered_map` and per-Segment read buffer
  can exhaust memory on large stores; consider sorted spill runs or a checkpointed derived index
  only after crash and compatibility proofs.
- Parallelize recovery by Worker only behind descriptor, memory, and I/O-token limits. Compare
  sequential and bounded parallel scans on SSD, HDD, and constrained containers.
- Replace temporary allocating string lookups with transparent `string_view` hashing/equality.
- Measure whether `hot_records` should be removed or replaced by a bounded prepared-node cache. Add
  a bounded per-Worker LRU of validated Segment descriptors or mappings so random reads across
  sealed Segments do not reopen and revalidate a file for every access.
- Lazily remove expired Index entries after a validated read and make durable TTL reclamation part
  of compaction. Repeated reads of an expired key must not repeatedly perform avoidable disk I/O.

### Offline verification, backup, restore, and repair

- Replace `glyphastore_inspect_segment` and `glyphastore_rebuild_index` placeholders with v1-aware,
  bounded, read-only tools. Provide stable text and JSON output, CRC/identity/commit validation,
  useful exit codes, and fixture tests.
- Add `verify-store` across Manifest and all Segments. Keep destructive repair a distinct command
  requiring an explicit output directory; never rewrite the only copy in place.
- Specify a consistent backup/snapshot procedure, including active Segment and manifest ordering,
  then test backup and restore during concurrent writes and after kills.
- Publish upgrade/downgrade rules for persistence v1 and test artifacts created by every released
  reader/writer. Worker-count changes and resharding require an offline, resumable, verified v1
  migration tool rather than an implicit open-time rewrite.

### Daemon operability and protocol

- Add bounded durable completion queues, overload responses, per-request deadlines, and fairness so
  one Worker or slow client cannot monopolize an executor.
- Define graceful drain: stop accepting, bound the drain deadline, finish or classify committed
  writes, flush/close the Store, and only then exit. Test SIGINT/SIGTERM during every phase.
- Add liveness, readiness, structured logs, metrics, build/config dump, and an administrative
  diagnostic surface. Readiness must fail on sticky storage errors and during unsafe recovery.
- Add wire golden fixtures, reserved-bit validation, opcode-specific key/value/expiry constraints,
  stable error mapping, duplicate request-id guidance, reconnect semantics, and compatibility
  tests. Protocol versioning remains independent from persistence v1.
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

- Benchmark SwissTable load factor, control-byte groups, probe lengths, tombstone cleanup, and
  transparent lookup on x86-64 and ARM64. Add scalar/SSE2/AVX2/NEON implementations only with
  runtime or compile-time dispatch and identical property/fuzz results.
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
  artifacts. Add disk quota and tiny block-device jobs.

### Build, hardening, and supply chain

- Apply PIE to executable link steps, not only compilation, and verify PIE, RELRO, immediate binding,
  stack protection, and fortification in CI. Make `_FORTIFY_SOURCE=3` configuration-aware and prove
  the optimized build actually enables it.
- Pin third-party Actions to verified full commit SHAs; full SHA is GitHub's immutable-action
  recommendation. Pin Python build dependencies and runner/compiler images, then use an automated,
  reviewed update process.
- Upload CodeQL/SARIF to code scanning when repository settings permit it instead of preserving only
  an artifact. Fail or warn on unexpectedly low analyzed-source coverage.
- Generate checksums, signed artifacts, SBOM, and build-provenance attestations. Add reproducible
  build settings (`SOURCE_DATE_EPOCH`, normalized archives, path remapping) and compare independent
  builds.
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

Persistence v1 is production ready only when all P0 items are complete; every supported platform
and filesystem has automated recovery evidence; all resources have tested limits; backup/restore
and verification are usable; the daemon, if shipped as durable, acknowledges exactly the documented
commit state; security and operational controls exist; and released v1 artifacts pass forward and
backward compatibility tests within the published policy.

Until then, the accurate status remains architectural prototype with implemented embedded v1
durability—not a production-certified database.
