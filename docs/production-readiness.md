<!-- GENERATED FILE. Do not edit by hand.
     Authority: engineering/gates/*.yaml
     Regenerate: python3 engineering/tools/validate_assurance.py --write-generated
-->

# Production readiness

> **Derived view.** Machine-readable authority lives under
> [`engineering/gates/`](../engineering/gates/). GlyphaStore remains an
> **architectural prototype**. A release level advances only when every
> mandatory gate below has automated evidence. A design document or
> implementation alone does not close a gate.

## Daemon runtime boundary (0.1.0)

`glyphastored` runs only the paired Reader–Writer model
([ADR paired shards](adr/paired-reader-writer-shards.md),
[server model](architecture/server-model.md)): one ShardPair (Reader + serial
Writer + SPSC lanes) per owner id. There is no dual-select daemon runtime.
The volatile engine under `src/experimental/` is lab-only.

## Release levels

- **Prototype:** architecture and performance exploration; no compatibility or durability promise.
- **Alpha:** public API and formats are versioned; destructive changes remain possible.
- **Beta:** durability, recovery, upgrade, security, and operational contracts are feature-complete.
- **Release candidate:** only correctness, compatibility, security, and operability fixes are accepted.
- **Stable:** supported upgrade paths, published artifacts, and an explicit support lifetime exist.

## Mandatory gates

| State | Meaning |
| --- | --- |
| `NON_INIZIATA` … `IMPLEMENTATA` | Work incomplete or not yet proven |
| `PROVATA_LOCALMENTE` | Proven outside mandatory CI evidence |
| `PROVATA_IN_CI` | CI evidence path exists and is linked |
| `PROVATA_SU_HARDWARE` / `VERIFICATA_INDIPENDENTEMENTE` / `ACCETTATA_PER_RILASCIO` | Higher claim levels |

### Public contract

- [ ] **GATE-API-ABI-POLICY** — API and ABI compatibility policies for patch/minor/major  
  State: `SPECIFICATA` · Release target: `alpha`  
  Requirements: `GS-COMPAT-FIXTURE-001`  
  Residual risk: Tagged cross-release binary matrices remain open
  Target policy documented; tagged matrices open.

- [ ] **GATE-CONCURRENCY-SPEC** — Error behavior, limits, time, and concurrency guarantees specified  
  State: `IMPLEMENTATA` · Release target: `alpha`  
  Requirements: `GS-CONCUR-PAIR-001`, `GS-CONCUR-LIN-001`, `GS-CONCUR-FAULT-001`, `GS-CONCUR-MEM-001`, `GS-CONCUR-TLA-001`, `GS-CONCUR-LIVE-001`, `GS-PROTO-WIRE-001`, `GS-CORE-CLOSE-001`  
  Residual risk: Daemon cancellation/deadline beyond client contract open; TLC job best-effort; checker history size bounded
  Client semantics and concurrency model normative; B1 checker/hooks/TLA+ present; some daemon edges open.

- [x] **GATE-DISK-WIRE-VERSIONS** — Disk and wire formats versioned with fixtures and matrices  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-COMPAT-FIXTURE-001`, `GS-PROTO-WIRE-001`  
  Residual risk: Publishing trees into permanent fixture drop remains a release-process step
  Golden fixtures and released-artifact harness exist; permanent drop process open.

- [x] **GATE-PUBLIC-API-OWNERSHIP** — Supported API separated with ownership/lifetime model  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-CORE-API-001`  
  Residual risk: No ABI stability before 1.0
  The installed API uses owning reads; internal access remains build-tree-only.

### Durability and recovery

- [ ] **GATE-BACKUP-RESTORE** — Backup restore verification and version migration  
  State: `IMPLEMENTATA` · Release target: `beta`  
  Requirements: `GS-OPS-BACKUP-001`  
  Residual risk: Released-tag artifact consumption remains open
  Offline tools and online fenced backup implemented; tag consumption open.

- [x] **GATE-DURABLE-ACK** — Acknowledgement semantics for durable mutations  
  State: `PROVATA_IN_CI` · Release target: `beta`  
  Requirements: `GS-PERSIST-ACK-001`  
  Residual risk: No native filesystem row has E3/E4 certification
  E2 evidence present; physical E3 honesty enforced by rehearsal scripts.

- [ ] **GATE-FAIL-CLOSED-IO** — Fail-closed on truncation corruption and I/O failures  
  State: `PROVATA_LOCALMENTE` · Release target: `beta`  
  Requirements: `GS-RECOVERY-FAILCLOSED-001`  
  Residual risk: System-level disk-full/quota/writeback matrices open
  Deterministic fault seams exist; system-level matrices open.

- [x] **GATE-RECOVERY-DETERMINISTIC** — Deterministic recovery after process termination  
  State: `PROVATA_IN_CI` · Release target: `beta`  
  Requirements: `GS-RECOVERY-DET-001`  
  Residual risk: Native exhaustive matrices open
  Crash suites provide E2 signals; not physical power-loss proof.

- [x] **GATE-WRITE-ORDER** — Write ordering synchronization and manifest publication  
  State: `PROVATA_IN_CI` · Release target: `beta`  
  Requirements: `GS-PERSIST-ORDER-001`  
  Residual risk: Filesystem/power-loss certification matrices open
  Platform-aware publication implemented with fault and process-kill tests.

### Verification

- [ ] **GATE-FAULT-INJECTION** — Fault injection for allocation filesystem clock socket thread failures  
  State: `IMPLEMENTATA` · Release target: `beta`  
  Requirements: `GS-RECOVERY-FAILCLOSED-001`  
  Residual risk: Exhaustive socket/thread/clock/hardware power-cut open
  Allocation and FS publication seams exist; broader matrices open.

- [x] **GATE-FUZZ-CI** — Fuzz targets run in CI with retained corpora  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-RECOVERY-FAILCLOSED-001`  
  Residual risk: Bounded smoke only; multi-hour continuous fuzz open
  Bounded libFuzzer smoke in sanitizers workflow.

- [x] **GATE-PERFORMANCE** — Performance tests track latency throughput memory regressions  
  State: `PROVATA_IN_CI` · Release target: `beta`  
  Requirements: `GS-PERF-REGRESSION-001`  
  Residual risk: Hosted CI is regression signal only; hardware budgets Phase E
  Median ops/s regression gate exists; absolute claims forbidden.

- [x] **GATE-SOAK** — Long-running stress and soak coverage  
  State: `PROVATA_IN_CI` · Release target: `beta`  
  Requirements: `GS-OPS-CONFIG-001`  
  Residual risk: Controlled multi-hour hardware soak with mandatory rotation evidence open
  Smoke/long soaks exist; hardware soak remains release gate.

- [x] **GATE-TEST-SUITES** — Distinct unit integration property concurrency crash recovery suites  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-RECOVERY-DET-001`, `GS-COMPAT-FIXTURE-001`  
  Residual risk: Regular tagged artifact drops remain open
  Suites are distinct in tree; permanent tagged drops open.

### Operations and security

- [x] **GATE-AUTH-TRANSPORT** — Authentication authorization transport security rate limits audit  
  State: `PROVATA_IN_CI` · Release target: `beta`  
  Requirements: `GS-SEC-PROFILE-001`  
  Residual risk: Multi-tenant Phase 8 and hostile-public CRL ops residual
  Secure profile Phases 2–6 landed; Phase 8 deferred.

- [x] **GATE-CONFIG** — Configuration precedence validation safe defaults limits  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-OPS-CONFIG-001`  
  Residual risk: Operator misuse of unsupported filesystems
  Validated durable defaults and daemon profiles implemented.

- [x] **GATE-OPS-RUNBOOKS** — Graceful drain overload backup restore corruption runbooks  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-OPS-BACKUP-001`, `GS-OPS-CONFIG-001`  
  Residual risk: Staging/production rehearsal still operator-owned
  Operator procedures exercised by ops-runbooks CI.

- [x] **GATE-TELEMETRY** — Structured logs metrics health readiness diagnostics  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-OPS-CONFIG-001`  
  Residual risk: Histogram approximations are not SLOs
  Wire HEALTH/READY/STATS and JSON lifecycle logging exist.

- [ ] **GATE-THREAT-SUPPLY** — Threat model and security release process including supply chain  
  State: `IMPLEMENTATA` · Release target: `beta`  
  Requirements: `GS-SEC-PROFILE-001`  
  Residual risk: Project GPG / full SLSA L3 optional; Actions SHA pin Phase D
  Threat model + SBOM/checksum/Cosign/SLSA path present; residuals documented.

### Distribution and lifecycle

- [x] **GATE-CMAKE-INSTALL** — CMake installs versioned package metadata and GlyphaStore::core  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-CORE-API-001`  
  Residual risk: Monolithic CMake (~989 lines) pending Phase C split
  Installed targets exist; structure debt remains.

- [x] **GATE-INSTALL-CONSUMER** — CI builds external consumer from installed prefix  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-CORE-API-001`  
  Residual risk: None beyond prototype claim ceiling
  Install-consumer gate present in CI.

- [ ] **GATE-RELEASE-MATRIX** — Release CI covers supported compilers OS arch optimized builds  
  State: `IMPLEMENTATA` · Release target: `rc`  
  Requirements: `GS-COMPAT-FIXTURE-001`  
  Residual risk: Tagged signed release pipeline deferred
  Build/test gates exist; full signed release pipeline deferred.

- [ ] **GATE-REPRO-SBOM** — Artifacts reproducible signed checksummed with provenance and SBOM  
  State: `IMPLEMENTATA` · Release target: `rc`  
  Requirements: `GS-COMPAT-FIXTURE-001`  
  Residual risk: macOS/Windows builders if those become release hosts
  Checksums SPDX Cosign SLSA path landed for Linux tags/dispatch.

- [x] **GATE-VERSION-LIFECYCLE** — Upgrade downgrade deprecation support EOL policies published  
  State: `PROVATA_IN_CI` · Release target: `alpha`  
  Requirements: `GS-COMPAT-FIXTURE-001`  
  Residual risk: Formal support windows for beta/RC/stable remain P3
  0.x / persistence v1 policies published; formal windows later.

## Change discipline

Any change to routing, hashing, persisted bytes, protocol framing, acknowledgement
semantics, or reclamation requires an ADR and new compatibility or recovery
evidence. Performance changes must preserve all safety and durability gates;
benchmark improvement is never evidence of correctness.

Assurance catalog: [`engineering/`](../engineering/) · Baseline:
[`docs/assurance/engineering-baseline.md`](assurance/engineering-baseline.md) ·
Agent rules: [`AGENTS.md`](../AGENTS.md).
