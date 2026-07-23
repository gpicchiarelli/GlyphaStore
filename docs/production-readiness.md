# Production readiness

GlyphaStore is an architectural prototype. A release level advances only when every mandatory gate
below has automated evidence. A design document or implementation alone does not close a gate.

## Release levels

- **Prototype:** architecture and performance exploration; no compatibility or durability promise.
- **Alpha:** public API and formats are versioned; destructive changes remain possible.
- **Beta:** durability, recovery, upgrade, security, and operational contracts are feature-complete.
- **Release candidate:** only correctness, compatibility, security, and operability fixes are accepted.
- **Stable:** supported upgrade paths, published artifacts, and an explicit support lifetime exist.

## Mandatory gates

### Public contract

- [x] The supported API is separated from implementation headers and has an ownership/lifetime model.
  The installed API uses owning reads; internal access remains build-tree-only.
- [ ] API and ABI compatibility policies define what may change in patch, minor, and major releases.
  Target policy: [public C++ API](architecture/public-api-contract.md).
- [ ] Disk and wire formats have independent versions, golden fixtures, and compatibility matrices.
  Manifest, Segment header, commit slot, Record, and dual-Manifest compaction intent have exact v1
  layouts and golden fixtures emitted independently of the production decoders. Decode-only
  compatibility tests, exact encoder checks, and durable artifact round-trip evidence exist. Wire
  v2 request/response golden fixtures are verified across the C++, Python, Perl, Go, and Ruby
  codecs. Cross-release artifact evidence is open.
  Target disk contract: [durability and recovery](architecture/durability-recovery.md).
- [ ] Error behavior, limits, time semantics, and concurrency guarantees are normative specifications.
  Official TCP client error categories, mutation outcomes, automatic retries, and monotonic request
  deadlines are normative in [client semantics v1](spec/client-semantics-v1.md)
  ([ADR 0019](adr/0019-client-error-retry-timeout.md)). Ordinary reads and recovery share a
  Store-owned clock with deterministic injection and per-instance backward clamping. Public
  allocation/unexpected exceptions and background flush callback exceptions map to stable
  categories. Allocator interposition covers durable mutation, rotation, read, and group paths.
  Public `Store::close()` stops admission, drains in-flight calls, makes final flush failure
  observable, and releases executors and the directory lock. Embedded durable config validates
  storage, free-space, Segment/manifest, descriptor, recovery-memory, live-key, temporary-compaction,
  and write-amplification limits. Daemon precedence and server-side cancellation/deadline semantics
  beyond the client contract are open. The real-daemon cross-SDK matrix covers C++, Python, Perl,
  Go, and Ruby at 1/2/4/8 Workers, every deterministic Worker owner, structured `NOT_FOUND`, and
  local oversized-frame rejection.

### Durability and recovery

- [ ] Acknowledgement semantics state exactly when a mutation is durable. Semantics are specified and
  implemented for `durable_sync`, `durable_group`, and `durable_periodic`; process-kill evidence
  covers their persistent write boundaries. The
  [platform durability evidence matrix](architecture/platform-durability-evidence.md) defines E0–E4
  claims; no native filesystem row has E3/E4 sudden-power-loss certification.
- [ ] Write ordering, synchronization, manifest publication, and directory synchronization are specified.
  Platform-aware descriptor, locking, full-I/O, atomic manifest publication, preallocated Segment
  creation, alternating commit-slot layer, and Store integration are implemented with fault and
  process-kill tests. Filesystem/power-loss matrices are open.
- [ ] Recovery is deterministic after process termination at every persistent state transition.
  Manifest-driven committed scans rebuild partitioned Indexes, next Worker sequences, and the
  sealed-active rotation marker. A bounded descriptor-relative namespace audit tolerates only
  recognizable crash temporaries and rejects unlisted/unknown/unsafe entries without mutation.
  Recovered Indexes feed a bounded per-Worker runtime whose disk reads revalidate CRC/key/reference
  metadata and remain fail-closed after corruption. Existing stores support ordered durable
  puts/tombstones, exact-intent rotation completion, and crash-recoverable public Store creation.
  CI runs `glyphastore_crash_persistence` (91 occurrence-specific SIGKILL checkpoints for bootstrap,
  put, rotation, single-output online compaction, and multi-output cleanup; optional `copy-matrix`
  and `random-matrix` profiles extend coverage). `glyphastore_crash_daemon` SIGKILL-tests the real
  `glyphastored` process after wire-protocol acknowledgements. These are E2 process-kill signals.
  Native-platform exhaustive matrices and disk-full coverage are open. The
  [recovery state-transition matrix v1](spec/recovery-state-matrix-v1.md) maps bootstrap, commit/flush,
  rotation, and compaction phases to authority, visibility, resumable action, or fail-closed result.
- [ ] Truncation, corruption, missing files, disk-full conditions, and I/O failures fail safely.
  Segment unit recovery rejects committed corruption and ignores uncommitted tails. Deterministic
  per-directory seams exercise short reads/writes, `EINTR`, delayed-sync `EIO`, `ENOSPC`/`EDQUOT`,
  `EROFS`, and every embedded mutation commit boundary with a recovery oracle. System-level
  disk-full/quota/writeback-error and power-loss matrices are open.
- [ ] Backup, restore, verification, and version migration are tested with released artifacts.

### Verification

- [ ] Unit, integration, property, concurrency, crash, recovery, and compatibility suites are distinct.
  Durable recovery has a separate integration suite. Crash, decode-only compatibility, and durable
  artifact suites are separate from integration recovery tests. Released-artifact suites are open.
- [ ] Fault injection covers allocation and relevant filesystem, clock, socket, and thread failures.
  Allocation sites in durable put/update/erase/read/group/rotation paths are enumerated
  deterministically. Filesystem publication and mutation boundaries have deterministic pre/post
  failure matrices. Exhaustive socket, thread-creation, platform clock, and hardware power-cut
  failures are open.
- [ ] Fuzz targets run continuously with retained seed and regression corpora; CI does more than compile them.
- [ ] Long-running stress and soak tests cover memory stability, rotation, vacuum, reconnect, and shutdown.
- [ ] Performance tests track tail latency, throughput, memory, and regressions without hiding variance.
  Benchmark CI fails when matched median ops/s regresses more than 10% versus the previous baseline.
  Weekly PGO smoke training includes durable open/put/reopen workloads. Local filters:
  `store-durable-*` (strict write-through), `store-durable-periodic-*` (deferred flush),
  `store-durable-group-*` (batched group commit). Dedicated matrices record durable compaction
  benefit/cost, concurrent maintenance cost, and forced-rotation/idle/churn latency on macOS; see
  [benchmarks/](benchmarks/). These are diagnostic local baselines, not release claims.
  Controlled-hardware CI evidence and an enforced tail-latency target are open.

### Operations and security

- [ ] Configuration has documented precedence, validation, safe defaults, and resource limits.
  Embedded `StoreConfig` has validated durable resource defaults and deterministic boundary tests.
  The daemon has explicit storage-mode, data-directory, durable open-policy, batch, and resource
  flags plus documented file/environment precedence (`defaults < profile < file < env < CLI`).
  `--dump-config` prints the resolved effective settings and exits without listening. Normal
  background compaction has a documented, daemon-configurable 128 MiB per-candidate copy limit plus
  opt-in unread-TTL normal scheduling (default off). Per-second/CPU maintenance rate fields remain
  reserved placeholders. Deployment profiles (`dev`, `embedded`, `production`) validate fail-closed
  before listen.
- [ ] Structured logs, metrics, health/readiness, build information, and administrative diagnostics exist.
  Wire `HEALTH`/`READY`/`STATS` expose liveness, readiness, build version, connection counts, durable
  lane/batch counters, and maintenance snapshot fields. Histogram export remains open. Structured
  JSON-lines lifecycle logging (`--log-format json`) covers start/listen, readiness transitions,
  shutdown drain, maintenance emergency/fault, and executor failure.
- [ ] Graceful drain, overload behavior, backup, restore, and corruption runbooks are exercised.
  Operator procedures: [operations runbooks](operations/README.md) including the end-to-end
  [durable TCP daemon guide](operations/durable-tcp-daemon.md) (graceful drain/overload,
  [backup-restore](operations/backup-restore.md), [corruption-repair](operations/corruption-repair.md)).
  Formal staging/CI exercise remains open.
- [x] Authentication, authorization, transport security, rate limits, and audit requirements are specified.
  Planning: [security/roadmap.md](security/roadmap.md). Decisions: ADRs
  [0020](adr/0020-tls-outer-transport.md)–[0022](adr/0022-authorization-capabilities.md).
  Daemon TLS scaffold landed (cert/key/mTLS CA flags, dual `--tls-port`, SDK TLS train partial);
  authn/authz remain open.
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

Any change to routing, hashing, persisted bytes, protocol framing, acknowledgement semantics, or
reclamation requires an ADR and new compatibility or recovery evidence. Performance changes must
preserve all safety and durability gates; benchmark improvement is never evidence of correctness.

The ordered implementation backlog is in the
[persistence v1 production roadmap](v1-production-roadmap.md). Persistence work remains on the v1
format; storage modes are policies over that same format, not separate persistent versions.
