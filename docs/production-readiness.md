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

- [ ] The supported API is separated from implementation headers and has an ownership/lifetime model.
- [ ] API and ABI compatibility policies define what may change in patch, minor, and major releases.
- [ ] Disk and wire formats have independent versions, golden fixtures, and compatibility matrices.
- [ ] Error behavior, limits, time semantics, and concurrency guarantees are normative specifications.

### Durability and recovery

- [ ] Acknowledgement semantics state exactly when a mutation is durable.
- [ ] Write ordering, synchronization, manifest publication, and directory synchronization are specified.
- [ ] Recovery is deterministic after process termination at every persistent state transition.
- [ ] Truncation, corruption, missing files, disk-full conditions, and I/O failures fail safely.
- [ ] Backup, restore, verification, and version migration are tested with released artifacts.

### Verification

- [ ] Unit, integration, property, concurrency, crash, recovery, and compatibility suites are distinct.
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

- [x] CMake installs versioned package metadata and exported `GlyphaStore::core`/`GlyphaStore::server` targets.
- [x] CI builds and runs an external consumer exclusively from the installed prefix.
- [ ] Release CI covers supported compilers, architectures, operating systems, and optimized builds.
- [ ] Artifacts are reproducible, signed, checksummed, and accompanied by provenance and an SBOM.
- [ ] Upgrade, downgrade, deprecation, support, and end-of-life policies are published.

## Change discipline

Any change to routing, hashing, persisted bytes, protocol framing, acknowledgement semantics, or reclamation
requires an ADR and new compatibility or recovery evidence. Performance changes must preserve all safety and
durability gates; benchmark improvement is never evidence of correctness.
