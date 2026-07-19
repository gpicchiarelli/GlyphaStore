# Persistence v1 production roadmap

This document is the repository-wide engineering audit and execution plan as of 2026-07-16. It
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

- `glyphastored` can now open the v1 durable Store through explicit storage, data-directory, and
  open-policy options, but durable mutations still execute synchronously on the reactor thread;
- Store-owned TTL time is now implemented; long-running native-platform clock evidence remains a
  release gate;
- exception translation, prepared hot-cache publication, and deterministic allocation-failure
  enumeration are implemented; native-platform evidence remains a release gate;
- recovery now validates monotonic sequence ranges both inside and across all Segments owned by one
  Worker; released-artifact and native-platform evidence remains to be accumulated;
- durable compaction has an internal crash-safe publication and retirement transaction plus
  explicit `Store::compact()` scheduling, but no complete kill/fault matrix or automatic policy yet;
- embedded durable operation now has explicit disk, descriptor, recovery, live-key, temporary-space,
  and write-amplification policy; daemon configuration and compaction-time enforcement remain;
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

**Status:** in progress. `Server::create` accepts a complete `StoreConfig`, requires its Worker count
to match the executor count, and closes the Store observably from `join()`. The daemon exposes
explicit `volatile`, `durable-sync`, `durable-periodic`, and `durable-group` selection plus durable
data-directory and open-policy controls. Durable `PUT`/`ERASE` now leave the Reactor through bounded
per-Worker FIFO lanes with count and byte admission, generation-safe completion, overload responses,
queue-wait expiry before Store entry, per-lane queue/service metrics, and drain-before-Store-close
shutdown. Strict-group mode retains bounded concurrent producers so daemon batching does not collapse
to occupancy one. Tests suspend real sync calls and prove Reactor responsiveness, independent queue
admission, bounded overload, non-commit of expired queued work, multi-record group sync, and recovery
of a mutation admitted during shutdown. Lock-free Worker-local kernel counters now expose exact batch
occupancy, close reasons, failures, and commit duration; histogram export remains an observability
surface task. Remaining resource CLI controls, real-daemon process-kill coverage, and bounded
shutdown deadlines remain open.

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

**Status:** implemented in the current tree. Coordinator callback exceptions are translated to
sticky `resource_exhausted`/`internal_error` failures, stop the coordinator, fail-close the durable
runtime, and release queued batch waiters. Public `Store::close()` atomically stops admission,
forces pending strict groups, waits for already-admitted calls, performs the final flush, stops the
executor, releases Store resources and the directory lock, and returns a sticky idempotent status.

**Root cause:** `DurableFlushCoordinator::run` invoked its callback without an exception barrier,
and destruction suppressed final flush errors.

**Required change:** catch callback failures, persist a sticky fail-closed error, complete every
generation/waiter, and prevent new mutations. Add explicit idempotent `close()`/shutdown returning a
status; document the destructor as a non-throwing fallback, not the only durability barrier.

**Acceptance:** injected callback exceptions, I/O errors, stop races, maximum generation, and
concurrent `flush()`/close tests cannot deadlock or acknowledge unflushed state.

**Evidence:** tests cover repeated and concurrent close, concurrent flush/close, partial strict
groups with a 60-second normal deadline, immediate reopen while the closed Store object remains
alive, sticky final-sync failure, background callback exceptions, coordinator stop races, and
exhausted flush generations. The destructor invokes the same path but intentionally discards its
status as a non-throwing fallback. Admission counters are cache-line-isolated by Worker; an A/B
Release benchmark against the pre-change commit used two interleaved, order-reversed runs of nine
samples each; aggregate worker-affine parallel get and put medians remained within 3% on arm64 macOS.

### P0-06 — Add storage and recovery resource budgets

**Status:** implemented for the embedded v1 Store. `DurableResourceLimits` bounds peak Store bytes,
available-space reserve, Segment count, manifest bytes, Store descriptors, estimated recovery
memory, live keys, temporary compaction space, and write amplification. These are runtime policy and
do not change the v1 disk format.

**Root cause:** one 64 MiB active Segment was created per Worker (up to 16 GiB at 256 Workers), while
catalog growth, recovery keys, descriptors, and rotation had only format or address-space ceilings.

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
is the public limit, avoiding global mutation-path contention. Tests cover all invalid fields,
configured byte/count/descriptor boundaries, injected available space, a 256-Worker rejection with
no bootstrap intent, reusable live-key capacity, constrained recovery, and rotation rejection while
the persisted active Segment remains unsealed. Native `ENOSPC`/`EDQUOT`, `EFBIG`, and
`EMFILE`/`ENFILE` map to `storage_exhausted`, `file_too_large`, and `descriptor_exhausted`.
Two order-reversed Release runs of nine samples for 4,096 one-Worker periodic durable puts measured
baseline medians of 42.52/45.75k put/s and limited-build medians of 44.01/45.28k put/s, showing no
regression from Worker-local live-key admission.

### P0-07 — Complete the failure matrix beyond process termination

**Status:** deterministic in-process coverage is implemented; hardware/filesystem certification is
still open. v1 process-kill tests cover bootstrap, put, periodic/group put, and rotation checkpoints,
and each run is isolated by process and start-time suffix. Instance-local raw I/O seams now force
short `pread`/`pwrite`, `EINTR`, synchronization `EIO`, `ENOSPC`/`EDQUOT`, and `EROFS` without global
test state. Controller caches, torn sectors, sudden power loss, and pinned native filesystem rows
cannot be established by an in-process seam and remain release blockers.

**Required change:** add deterministic short-write/read, `EINTR`, delayed writeback `EIO`, `ENOSPC`,
`EDQUOT`, `EROFS`, missing file, corrupt directory entry, rename, file-sync, and directory-sync
faults. Add VM/block-device power-cut tests and a documented filesystem/mount matrix.

**Acceptance:** every persistent transition has a pre/post failure oracle; two crash matrices can
run concurrently; supported filesystem rows pass repeated power-cut recovery; unsupported remote
or user-space filesystems are rejected or prominently documented.

**Evidence:** exact-I/O tests prove retry after `EINTR`, completion after repeated short transfers,
and stable native error categories. Manifest, bootstrap intent, and Segment creation matrices verify
every pre-rename boundary leaves the old authority or a pristine namespace; directory-sync failure
after rename is indeterminate and fail-closed. Durable mutation tests cross write, Record sync,
commit-slot write, and commit-slot sync with `io_error`, `storage_exhausted`, and
`read_only_filesystem`, then reopen and verify the absent/optional commit oracle and rebuilt Index.
Existing namespace recovery cases also reject missing catalog files, malformed names, symlinks, hard
links, and unlisted entries without adopting or repairing them.
The SIGKILL bootstrap matrix now begins at data-directory creation and parent-directory sync.
Periodic and group crash matrices also completed concurrently with isolated namespaces and markers.
The filesystem contract now prominently marks NFS, SMB, FUSE, overlay, and other remote/user-space
storage unsupported and records explicit APFS/Linux/BSD certification rows. VM/block-device
power-cut automation and pinned native mount rows remain before this P0 item can be complete.

### P0-08 — Implement crash-safe durable compaction in v1

**Status:** in progress. Durable compaction is now exposed as explicit cooperative Store maintenance;
automatic operational policy and the complete online crash matrix remain open. A deterministic v1
planner treats one Worker's complete sealed history as the atomic unit,
reuses the earliest source IDs with incremented generations, preserves the active Segment, and
rejects generation exhaustion, no-gain rewrites, and temporary/peak/amplification budget overruns.
A checksummed intent codec embeds and validates both complete manifest authorities and their exact
canonical transition. Descriptor-relative intent publication and removal now implement private
temporary creation, exact write, file sync, rename, `unlinkat`, bounded read, and mandatory directory
sync with explicit pre-operation/indeterminate outcomes. Runtime reopen now resolves an interrupted
intent against exactly the old or next authority, fully recovers that catalog before deletion,
validates obsolete identities, performs idempotent rollback/source retirement, synchronizes the
directory, removes the intent, and re-audits the namespace. This prevents a per-Segment tombstone
drop from resurrecting older values. A durable builder now consumes an owning manifest/Index
snapshot and exact source-generation pins, verifies routed source Records, drops expired puts,
computes exact non-spanning Segment
layout, prebuilds the replacement Index, publishes the intent, copies original v1 bytes with a
reused buffer, seals and reopens every output, and validates checksums and commit metadata. The
internal durable runtime now holds no Worker/catalog lock during scan, CRC, replacement writes,
seal, or reopen validation. A brief final try-lock publishes only if sequence, batch, manifest,
source identity, and generation pins still match; concurrent mutation instead triggers finite
old-authority rollback and preserves the mutation. It installs the prepared manifest, commit
catalog, and Index atomically, releases data locks before retiring sources, removes the intent, and
fails closed only when recovery is genuinely required. A logical manifest-publication lease makes
rotation or another compaction fail fast instead of waiting behind the build, while immutable
generation pins let readers continue across catalog entry removal. Final manifest write/sync also
runs without Worker, catalog, or publication mutexes under a Worker-local commit gate. Public
`Store::compact()` now uses a non-queuing
Store-wide maintenance gate, selects Workers round-robin, skips exact no-gain layouts, executes at
most one transaction per call, and reports copy statistics. The online single-output crash and I/O
fault matrices now cover 25 distinct persistence boundaries, including occurrence-specific directory
syncs and unlinks; allocator interposition reopens after every observed allocation failure. Automatic
policy, multi-output randomized crash histories, and native power-loss certification remain open.

**Required change:** copy only the latest live v1 Records into new v1 Segments, validate the copy,
atomically publish a new v1 Manifest, sync the directory, then retire old files with a second
directory sync. Define pin/reader ownership, tombstone and TTL retention, temporary-space budget,
recovery of recognizable compaction temporaries, and an online scheduling policy.

**Acceptance:** kills at every copy, validation, manifest, rename, unlink, and directory-sync
boundary recover exactly one valid catalog; readers never observe deleted backing files; space and
write amplification remain within configured limits; no format change is introduced.

**Implemented evidence:** planner tests cover header-aware output sizing, complete sealed-set
replacement, zero-output retirement, stable Segment-ID ordering, incremented generations, unchanged
routing/active identity, encodable next manifests, generation exhaustion, no-gain rejection, and
temporary-space, peak-Store, and physical write-amplification limits. The protocol and reader
ownership rules are specified in
[crash-safe durable compaction](architecture/durable-compaction.md).
Intent codec tests additionally cover exact dual-manifest round trips, truncation, trailing bytes,
CRC corruption, header/payload disagreement, unknown versions, reserved bytes, and noncanonical
catalog transitions.
Filesystem tests cross intent write, sync, rename, post-rename directory sync, pre-unlink rejection,
post-unlink directory sync, duplicate intent, bounded read, reopen, and namespace classification.
Integration recovery tests cover old-authority rollback, next-authority roll-forward, rejection of
an unrelated manifest or Segment, failure before retirement, partial source unlink, indeterminate
retirement sync, both intent-removal boundaries, and successful idempotent completion on the next
reopen.
Builder tests cover exact sequence and value preservation, superseded/tombstoned Record omission,
TTL reclamation, active-reference preservation, zero-output retirement, non-spanning layout
fragmentation, intent-aware peak-space accounting, and rollback after an injected post-intent copy
failure. Runtime integration tests cover atomic in-memory installation and source retirement,
restart visibility, fail-closed rollback after an online post-intent failure, preservation of
another Worker's cached descriptor when catalog positions shift, and artificially blocked Phase B
where same-Worker GET, PUT, erase, and TTL update complete before the stale build rolls back with
`sequence_conflict`. A forced unrelated rotation returns conflict without waiting; a blocked final
manifest sync proves that reads and other-Worker writes continue without any Worker, catalog, or
publication mutex held. Close during Phase B rolls the old authority back to a clean reopen.
Public integration tests additionally cover no-op maintenance without sealed history, rejection on
volatile and closed Stores, restart visibility, one-Worker-per-call round-robin progress, and
post-compaction no-gain detection.

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
