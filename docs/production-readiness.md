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
  Manifest, Segment header, commit slot, and Record have exact v1 layouts and golden fixtures. The
  [development compatibility matrix](architecture/format-compatibility.md) is explicit; wire golden
  fixtures and cross-release artifact evidence remain pending. Target disk contract: [durability and
  recovery](architecture/durability-recovery.md).
- [ ] Error behavior, limits, time semantics, and concurrency guarantees are normative specifications.

### Durability and recovery

- [ ] Acknowledgement semantics state exactly when a mutation is durable. The target semantics are
  specified; implementation and crash evidence remain pending.
- [ ] Write ordering, synchronization, manifest publication, and directory synchronization are specified.
  The platform-aware descriptor, locking, full-I/O, atomic manifest publication, preallocated
  Segment creation, and alternating commit-slot layer are implemented with fault tests; Store
  integration and filesystem crash evidence remain pending.
- [ ] Recovery is deterministic after process termination at every persistent state transition.
  Manifest-driven committed scans now rebuild partitioned Indexes, next Worker sequences, and the
  sealed-active rotation marker deterministically. A bounded descriptor-relative namespace audit
  tolerates only recognizable crash temporaries and rejects unlisted/unknown/unsafe entries without
  mutation. Runtime materialization, explicit orphan quarantine/reservation, transition completion,
  and process-kill coverage remain pending.
- [ ] Truncation, corruption, missing files, disk-full conditions, and I/O failures fail safely.
  Segment unit recovery rejects committed corruption and ignores uncommitted tails; system-level
  disk-full, missing-catalog-file, and process-kill matrices remain pending.
- [ ] Backup, restore, verification, and version migration are tested with released artifacts.

### Verification

- [ ] Unit, integration, property, concurrency, crash, recovery, and compatibility suites are distinct.
  Durable recovery now has a separate integration suite for catalog, lifecycle, routing, visibility,
  namespace policy, missing-file, conflict, and overflow behavior; crash and released-artifact suites
  remain pending.
- [ ] Fault injection covers allocation and relevant filesystem, clock, socket, and thread failures.
- [ ] Fuzz targets run continuously with retained seed and regression corpora; CI does more than compile them.
- [ ] Long-running stress and soak tests cover memory stability, rotation, vacuum, reconnect, and shutdown.
- [ ] Performance tests track tail latency, throughput, memory, and regressions without hiding variance.

### Operations and security

- [ ] Configuration has documented precedence, validation, safe defaults, and resource limits.
- [ ] Structured logs, metrics, health/readiness, build information, and administrative diagnostics exist.
- [ ] Graceful drain, overload behavior, backup, restore, and corruption runbooks are exercised.
- [ ] Authentication, authorization, transport security, rate limits, and audit requirements are specified.
- [ ] A threat model and security release process cover storage, protocol, build, and supply-chain boundaries.

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
