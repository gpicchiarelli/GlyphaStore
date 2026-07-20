# Production readiness

GlyphaStore remains pre-alpha until every mandatory gate below is backed by automated evidence.
Passing a gate means that the contract is documented, tested, and exercised in release CI; an
implementation or design document alone is not sufficient.

## Release levels

- **Prototype:** architecture and performance exploration; no compatibility or durability promise.
- **Alpha:** public API and formats are versioned, but destructive changes remain possible.
- **Beta:** durability, recovery, upgrade, security, and operational contracts are feature-complete.
- **Release candidate:** only correctness, compatibility, security, and operability fixes are accepted.
- **Stable:** supported upgrade paths, published artifacts, and an explicit support lifetime exist.

## Mandatory gates

### Public contract

- [x] The supported API is separated from implementation headers and has an ownership/lifetime model.
  The installed API uses owning reads; internal access remains build-tree-only.
- [ ] API and ABI compatibility policies define what may change in patch, minor, and major releases.
  Target policy: [public C++ API](architecture/public-api-contract.md); release evidence pending.
- [ ] Disk and wire formats have independent versions, golden fixtures, and compatibility matrices.
  Manifest, Segment header, commit slot, and Record have exact v1 layouts and golden fixtures emitted
  independently of the production decoders. Decode-only compatibility tests and durable artifact
  round-trip evidence exist; wire golden fixtures and cross-release artifact evidence remain pending.
  Target disk contract: [durability and recovery](architecture/durability-recovery.md).
- [ ] Error behavior, limits, time semantics, and concurrency guarantees are normative specifications.
  Official TCP **client** error categories, mutation outcomes, automatic retries, and monotonic
  request deadlines are normative in [client semantics v1](spec/client-semantics-v1.md)
  ([ADR 0019](adr/0019-client-error-retry-timeout.md)). Ordinary reads and recovery now share a Store-owned clock with deterministic injection and
  per-instance backward clamping. Public allocation/unexpected exceptions and background flush
  callback exceptions are translated to stable categories. A dedicated allocator-interposition
  executable now fails every allocation observed in durable mutation, rotation, read, and group
  paths. Public `Store::close()` now prevents new admission, drains in-flight calls, makes final
  flush failure observable, and releases executors and the directory lock. Embedded durable config
  now validates storage, free-space, Segment/manifest, descriptor, recovery-memory, live-key,
  temporary-compaction, and write-amplification limits; daemon precedence and server-side
  cancellation/deadline semantics beyond the client contract remain pending.

### Durability and recovery

- [ ] Acknowledgement semantics state exactly when a mutation is durable. The target semantics are
  specified and implemented for `durable_sync`, `durable_group`, and `durable_periodic`; process-kill
  evidence covers their persistent write boundaries. Native filesystem and sudden-power-loss
  certification remain pending.
- [ ] Write ordering, synchronization, manifest publication, and directory synchronization are specified.
  The platform-aware descriptor, locking, full-I/O, atomic manifest publication, preallocated
  Segment creation, alternating commit-slot layer, and Store integration are implemented with fault
  and process-kill tests; filesystem/power-loss matrices remain pending.
- [ ] Recovery is deterministic after process termination at every persistent state transition.
  Manifest-driven committed scans now rebuild partitioned Indexes, next Worker sequences, and the
  sealed-active rotation marker deterministically. A bounded descriptor-relative namespace audit
  tolerates only recognizable crash temporaries and rejects unlisted/unknown/unsafe entries without
  mutation. Recovered Indexes now feed a bounded per-Worker runtime whose disk reads revalidate
  CRC/key/reference metadata and remain fail-closed after corruption. Existing stores support
  ordered durable puts/tombstones, exact-intent rotation completion, and crash-recoverable public
  Store creation. The `glyphastore_crash_persistence` harness SIGKILL-tests bootstrap, put, and
  rotation boundaries on Linux CI; native-platform exhaustive matrices and disk-full coverage remain
  pending.
- [ ] Truncation, corruption, missing files, disk-full conditions, and I/O failures fail safely.
  Segment unit recovery rejects committed corruption and ignores uncommitted tails; missing catalog
  files and process termination are covered. Deterministic per-directory seams now exercise short
  reads/writes, `EINTR`, delayed-sync `EIO`, `ENOSPC`/`EDQUOT`, `EROFS`, and every embedded mutation
  commit boundary with a recovery oracle. System-level disk-full/quota/writeback-error and
  power-loss matrices remain pending.
- [ ] Backup, restore, verification, and version migration are tested with released artifacts.

### Verification

- [ ] Unit, integration, property, concurrency, crash, recovery, and compatibility suites are distinct.
  Durable recovery now has a separate integration suite for catalog, lifecycle, routing, visibility,
  namespace policy, bounded runtime reads, sticky corruption failure, missing-file, conflict, and
  overflow behavior. Crash (`glyphastore_crash_persistence`), decode-only compatibility, and durable
  artifact suites are separate from integration recovery tests; released-artifact suites remain
  pending.
- [ ] Fault injection covers allocation and relevant filesystem, clock, socket, and thread failures.
  Allocation sites in durable put/update/erase/read/group/rotation paths are enumerated
  deterministically per native STL build, including pre-write recovery invariants, post-write
  fail-close behavior, allocation-free steady-state publication, and background waiter release.
  Filesystem publication and mutation boundaries now have deterministic pre/post failure matrices;
  exhaustive socket, thread-creation, platform clock, and hardware power-cut failures remain pending.
- [ ] Fuzz targets run continuously with retained seed and regression corpora; CI does more than compile them.
- [ ] Long-running stress and soak tests cover memory stability, rotation, vacuum, reconnect, and shutdown.
- [ ] Performance tests track tail latency, throughput, memory, and regressions without hiding variance.
  Benchmark CI now fails when matched median ops/s regresses more than 10% versus the previous
  baseline; weekly PGO smoke training includes durable open/put/reopen workloads. Local
  `store-durable-*` filters measure strict write-through persistence; `store-durable-periodic-*`
  filters measure the production deferred-flush path; `store-durable-group-*` filters measure
  strict batched group commit. On Apple Silicon (macos-release, 20k ops, key=16, value=64)
  the corrected two-barrier strict durable put path measures ~122 ops/s, while
  `store-durable-periodic-read-after-write` measures ~239k ops/s with the 4096-record/4 MiB/1000 ms
  default batch. After moving whole-batch Index publication into the batch closer, strict
  `durable_group` with 32 concurrent writers on one Worker measured ~3.6--3.9k put/s. Replacing the
  first macOS full flush with the platform's ordered storage barrier raised the directly comparable
  4,096-operation median from 3.80k to 6.21k put/s (+63%), while p50 fell from 8.53 to 5.02 ms and
  p99 from 12.11 to 6.96 ms. Holding concurrency at 32 across four Workers also measured 6.22k put/s
  and p99 6.05 ms, showing that partially filled independent Worker batches remain the limit. With
  128 clients, four full Worker batches reached 8.52k put/s at p99 29.03 ms; one Worker at the same
  concurrency reached 5.34k put/s at p99 75.20 ms. Concurrency can buy occupancy, but not an
  acceptable latency curve by itself. The dedicated one-Worker commit-executor path measured
  6.28k put/s over seven 4,096-operation samples, effectively neutral against its 6.21k baseline.
  The four-Worker path measured 6.14k put/s in the same follow-up. With one sparse client and an
  absolute 10 ms batch deadline, end-to-end p99 including both persistence phases measured 20.08 ms.
  Occupancy-adaptive record targets then raised one-Worker throughput at 4/8/16 producers from
  238/480/957 put/s to 898/1.74k/2.97k put/s and reduced p50 from about 16 ms to 4--5 ms. Four
  Workers at 8/16 producers improved from 315/636 put/s to 1.73k/3.03k put/s; saturated 32-producer
  samples remained within 3--7% of the prior local range. Hot-cache durable get remains around
  1.9M ops/s. The ARM64 Swiss-slot reduction from 80 to 64 bytes removed about 31.5 MiB RSS at the
  2,097,152-slot capacity used by a 1M-entry run; cold-read generation pins now allow a blocked
  `pread` to coexist with same-Worker mutation and source-retiring compaction without stale return.
  These local measurements are diagnostic baselines, not release claims;
  controlled-hardware CI evidence and an enforced tail-latency target remain pending.

### Operations and security

- [ ] Configuration has documented precedence, validation, safe defaults, and resource limits.
  Embedded `StoreConfig` has validated durable resource defaults and deterministic boundary tests.
  The daemon now has explicit storage-mode, data-directory, and durable open-policy flags that are
  validated before binding; batch/resource flags, CLI/file/environment precedence, and deployment
  profiles remain pending.
- [ ] Structured logs, metrics, health/readiness, build information, and administrative diagnostics exist.
- [ ] Graceful drain, overload behavior, backup, restore, and corruption runbooks are exercised.
- [x] Authentication, authorization, transport security, rate limits, and audit requirements are specified.
  Planning: [security/roadmap.md](security/roadmap.md). Decisions: ADRs
  [0020](adr/0020-tls-outer-transport.md)–[0022](adr/0022-authorization-capabilities.md)
  (OpenBSD/LibreSSL first-class). Daemon TLS scaffold landed (Phase 2 partial: cert/key/mTLS CA
  flags); SDK TLS train and remaining secure-profile work remain open.
- [ ] A threat model and security release process cover storage, protocol, build, and supply-chain boundaries.
  Threat model: [security/threat-model.md](security/threat-model.md). Reporting:
  [SECURITY.md](../SECURITY.md). Supply-chain scanning / SBOM remain open.

### Distribution and lifecycle

- [x] CMake installs versioned package metadata and the supported `GlyphaStore::core` target.
- [x] CI builds and runs an external consumer exclusively from the installed prefix.
- [ ] Release CI covers supported compilers, architectures, operating systems, and optimized builds.
- [ ] Artifacts are reproducible, signed, checksummed, and accompanied by provenance and an SBOM.
- [ ] Upgrade, downgrade, deprecation, support, and end-of-life policies are published.

## Change discipline

Any change to routing, hashing, persisted bytes, protocol framing, acknowledgement semantics, or reclamation
requires an ADR and new compatibility or recovery evidence. Performance changes must preserve all safety and
durability gates; benchmark improvement is never evidence of correctness.

The ordered implementation backlog and acceptance criteria are maintained in the
[persistence v1 production roadmap](v1-production-roadmap.md). Persistence work remains on the v1
format; storage modes are policies over that same format, not separate persistent versions.
