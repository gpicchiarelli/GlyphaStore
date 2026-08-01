## [Unreleased]

- Paired adoption naming: prefer shard-pair / Reader / Writer language in operator telemetry docs
  (`observability` lane/batch surfaces, durable-tcp hot-cache wording) while keeping Manifest/wire
  `worker_count` and CLI `--workers` as 0.1.x aliases.
- Paired adoption P1 residual honesty: optional Linux I/O backend (`io_uring` / batched
  completion) **deferred** for 0.1.0 without a measured queue/syscall win that preserves write
  ordering; refresh Delta COW / get-into reject / Linux harness residual status in
  `paired-shards-plan`, lab prototype “Prossimo gate”, and durable cold-read follow-ups. No
  fabricated `glyphastore-linux-perf` evidence; no `e3_certified=yes`.
- Paired adoption Fase 0 honesty: dual-path inventory on the production roadmap
  (`docs/v1-production-roadmap.md` — public owning `Store::get` vs daemon `ReadGeneration`,
  `pair_writer_stats`, `--shard-pairs` / `--workers` alias, lab-only `experimental/`). Complements
  the earlier CHANGELOG inventory under the sole-runtime docs cutover.
- Daemon request/idle timeout vs client contract: prove `--request-timeout-ms` closes
  stalled partial frames and in-flight cold-read waits (`abuse_request_timeout_closed`,
  cancel-on-close) without cancelling admitted durable Store mutations
  (`server_reactor_security_tests` / `server_reactor_durable_tests`); document in
  client-semantics §6.2 and durable-tcp-daemon. Closes GATE-CONCURRENCY-SPEC /
  `GS-PROTO-WIRE-001` residual on daemon cancellation/deadline beyond client contract.
  TLC/checker history residuals unchanged; not N−1 fixtures; not E3.
- HAZ-021: real `glyphastored` exec mid-BACKUP kill matrix via env-gated crash hooks
  (`GLYPHASTORE_CRASH_TEST` / `GLYPHASTORE_CRASH_KILL_AT` / `GLYPHASTORE_CRASH_CHECKPOINT_DIR`)
  and `glyphastore_crash_backup_daemon` (`copy_backup_segment` / `copy_backup_manifest` /
  `sync_backup_destination`). Incomplete dest fails verify; source reopens healthy. Lab-only;
  production leaves hooks unset. Still not zero-fence (ADR 0034); durable remains sync write-through.
- Runtime SDK online `BACKUP` interop smoke (`scripts/test-sdk-backup-interop.sh`): durable
  `glyphastored` + typed `backup()` for Python/Go/Perl/Ruby/Erlang; wired into CI `sdk-clients`
  (`BACKUP_INTEROP_REQUIRE_ALL=1`). Closes the symbol-only residual from
  `assert-sdk-backup-helpers.sh` (still fenced, not zero-fence).
- C++ `Error` carries portable `mutation_outcome` (`rejected` / `indeterminate`) on failed
  TCP-client mutations and pipeline mutation positions; `portable_mutation_outcome(wire_status)`
  matches the taxonomy fixture. Closes the last `GS-PROTO-ERROR-001` residual on C++ Error.
- HAZ-022: synthetic incompatible restore/open matrices — checksum-valid future Manifest
  format and future pinned Record version refuse verify/restore/`Store::open`; truncated
  Manifest and Worker-count mismatch after restore also fail closed
  (`tests/unit/store_backup_tests.cpp`). `STORE-FUTURE-REQUIRED` evidence updated; tagged
  N−1 permanent fixture drops remain a release residual.
- `GS-PROTO-ERROR-001`: unknown wire status maps uniformly to category `protocol` (preserve numeric
  `wire_status`; mutation `indeterminate`) across C++ and official SDKs; fixture case
  `unknown_wire_status` (99). Codecs still reject unknown statuses after buffering the frame.
- Offline migrate resume hardening (ADR 0024 / `GS-OPS-MIGRATE-001`): real mid-copy interrupt via
  destination `FilesystemHooks` then resume; fail-closed corrupt/mismatched/orphan checkpoints;
  N↔N-1 `STORE-WORKER-RESHARD` evidence points at unit tests.
- HAZ-021: wire/reactor BACKUP process-kill via in-process Server + `Client::backup`
  (`glyphastore_crash_backup_wire`). Incomplete dest fails verify; source reopen + wire GET after
  Server restart.
- HAZ-021: Store process-kill mid-backup via `FilesystemHooks`
  (`copy_backup_segment` / `copy_backup_manifest` / `sync_backup_destination`) and
  `glyphastore_crash_backup`. Incomplete dest fails verify; source reopens healthy.
- Surface `source_crc_scanned` / `destination_crc_scanned` on wire `BACKUP` ASCII and
  `glyphastore_backup_store` text/JSON reports. Clarify ops docs: online fenced backup is supported;
  zero-fence hot backup is not.
- HAZ-021: incomplete backup destinations fail verify/restore; failed online backup leaves the live
  Store usable (`tests/unit/store_backup_tests.cpp`).
- Online fenced backup: keep only **structural** source verify under the admission fence; run
  committed CRC scan on the destination after admissions resume (promotion gate). Offline backup
  still CRC-scans the source once before copy. Report `source_crc_scanned` /
  `destination_crc_scanned`.
- Parallelize catalog Segment file copy during offline/online backup (bounded workers, Manifest
  last). Report `segment_copy_workers`. Completes ADR 0034 fenced-path incremental (shorter fence +
  copy parallelism); still not zero-fence.
- Shorten online `Store::backup_to` admission fence: resume writers after catalog copy; run
  destination verify outside the fence. Report `admission_fence_ns` / `catalog_copy_ns` /
  `destination_verify_ns` on backup reports and wire `BACKUP` ASCII output (still not zero-fence;
  ADR 0034).
- ADR [0034](docs/adr/0034-zero-fence-hot-backup-deferred.md): freeze design requirements for
  zero-fence hot backup; 0.1.x keeps offline + online fenced paths only.
- Docs P1: [operations handbook](docs/operations/handbook.md) and
  [ADR 0033](docs/adr/0033-online-rebalance-deferred.md) (online rebalance design frozen,
  implementation deferred past 0.1.x).
- Docs P1: normative [backup-restore-v1](docs/spec/backup-restore-v1.md) and
  [tcp-client-conformance-v1](docs/spec/tcp-client-conformance-v1.md).
- Docs P1: [observability reference](docs/operations/observability.md) for HEALTH/READY/STATS
  needles, JSON lifecycle logs, and `--dump-config` (no metrics exporter claimed).
- Docs P1: operator [compatibility-and-migration](docs/operations/compatibility-and-migration.md)
  manual and [release-checklist](docs/assurance/release-checklist.md); WAV-001 size waiver revoked
  after production and test suite splits under the structure line budget.
- Close WAV-001: split remaining oversized test suites
  (`maintenance_controller_*`, `server_reactor_*`, `persistence_recovery_*`) under the
  structure line budget; revoke the size waiver.
- Reduce WAV-001 surface: split `src/persistence/runtime_catalog.cpp` into
  `runtime_catalog_detail.hpp`/`.cpp`, `runtime_catalog_ops.cpp`, and
  `runtime_catalog_maintenance.cpp` (each under the structure line budget).
- Reduce WAV-001 surface: split `src/server/reactor.cpp` into `reactor_detail.hpp` +
  `reactor_dispatch.cpp` (I/O/lifecycle vs frame/dispatch path).
- Reduce WAV-001 surface: split `src/store/store.cpp` into `store_impl.hpp`,
  `store_access.cpp`, and `prepared_cold_read.cpp` (each under the structure line budget).
- Assurance Phase E: performance/soak/overload budgets
  (`engineering/performance/budgets.yaml`, `validate_perf_budgets.py`) linked to
  `GATE-PERFORMANCE` / `GATE-SOAK` / `GATE-OPS-RUNBOOKS` (`GS-PERF-BUDGET-001`,
  `GS-OPS-SOAK-001`). Absolute hardware thresholds remain `specified_waiting_for_runner`.
  Final honest summary: `docs/assurance/final-engineering-report.md` (claim ceiling stays
  architectural prototype).
- Assurance Phase D: N↔N-1 compatibility matrix (`engineering/compatibility/n-n1-matrix.yaml`),
  SHA-pinned GitHub Actions (`validate_actions_pins.py`), release claim schema/packaging
  (`engineering/claims/`, `scripts/package-release-claim.sh`), and requirements
  `GS-COMPAT-NN1-001` / `GS-SUPPLY-ACTIONS-001`. Residual: permanent tagged N−1 fixture drops.
- Assurance Phase C: split root `CMakeLists.txt` via `add_subdirectory` for
  `src`/`tools`/`tests`/`benchmarks`/`fuzz` (installed `GlyphaStore::*` aliases unchanged);
  add `engineering/build/dependency-matrix.yaml`, structure debt thresholds, waivers (`WAV-001`),
  and CI validators `validate_cmake_deps.py` / `validate_structure_debt.py`.
- Paired embedded Store gate snapshot (ADR 0032 T5): macOS-release Zipf durable parallel GET
  p50/p99 recorded in `docs/benchmarks/paired-embedded-store-gates-2026-08-01.md`; full ctest
  37/37 green. Fix `ShardPairRuntime::Lane` member init order warning.
- Paired exclusive Writer mutex-elision (ADR 0032 T2): durable `mutate` / `capture_published_read`
  skip the Worker mutex when `exclusive_writer` and no background flusher (`durable_sync`);
  compaction waits on `hot_path_depth`. Volatile paired Writers keep generation-only publication
  without `mutex_` on the hot path (debug assert). Compaction/verify/backup/catalog-refresh
  snapshots retain locks. Catalog shared lock on mutate/capture remains (pin lookup).
  Crash recovery harnesses open with deprecated `legacy_mutex` so TSan crash matrices are not
  dominated by paired Writer startup. `legacy_mutex` ctest path documented in
  `docs/development/test-strategy.md`.
- ADR 0032: paired Reader/Writer concurrency is the product default for embedded `Store::open` as
  well as `glyphastored` (amends ADR 0031/0005/0009 concurrency notes). Persistence v1 and wire v2
  unchanged; public owning `Store::get` unchanged. Docs aligned
  (`concurrency-memory-model`, `worker-model`, `public-api-contract`, glossary). Deprecated
  `legacy_mutex` escape hatch documented for 0.1.x removal in 0.2; mixing legacy mutators with a
  paired Writer on one Store is refused / UB.
- Embed `ShardPairRuntime` in `glyphastore_core`: `Store::open` defaults to paired (Writer thread +
  published `ReadGeneration` per shard). Public `get` adopts the generation (durable cold reads
  complete synchronously); `put`/`erase` hand off to the Writer. Durable hot-cache admission is
  disabled in paired mode (generation-only). `glyphastored` opens the same paired Store and uses a
  thin `PairWriterPool` adapter (no second publication spine). `src/experimental/paired_*` remains
  lab-only.
- Document paired Reader–Writer as the sole `glyphastored` 0.1.0 runtime (ADR 0031); the volatile
  engine under `src/experimental/` remains lab-only and is not a second selectable daemon. Inventory:
  public `Store::get` keeps owning pins; daemon GET borrows a Reader-local `ReadGeneration`;
  `Server::pair_writer_stats()` is the paired mutation-lane surface while Manifest/wire
  `worker_count` and CLI `--workers` stay 0.1.x aliases of shard-pair count; no dual-select runtime
  exists.
- P1 Delta mixed follow-up: hierarchical directory-chunk COW (plus O(1) arena key counters / in-place
  store) to cut per-publication spine traffic without changing version capacity or QSBR lifetime
  ([paired-delta-directory-chunks-2026-07-31](docs/benchmarks/paired-delta-directory-chunks-2026-07-31.md)).
  macOS advisory A/B is noisy; Linux hard-pinned confirmation still required for magnitude claims.
- P1 reject get-into / multi-extent scatter promotion without bounded+win proof
  ([paired-get-into-multi-extent-reject-2026-07-31](docs/benchmarks/paired-get-into-multi-extent-reject-2026-07-31.md)).
- P1 Linux hard-pinned 1/2/4/8 harness + runbook + `workflow_dispatch` self-hosted workflow
  ([paired-shards-linux-p1](docs/benchmarks/paired-shards-linux-p1.md)); macOS evidence stays advisory.
- Add CI assert for typed SDK backup helpers (`scripts/assert-sdk-backup-helpers.sh`): fail closed
  if any official SDK (C++/Python/Go/Perl/Ruby/Erlang) lacks a typed `backup`/`Backup` surface for
  wire `BACKUP`; wired into `sdk-clients`. Runtime interop smoke:
  `scripts/test-sdk-backup-interop.sh`.
- Add SDK artifact attestation verification gate
  (`scripts/verify-sdk-artifact-attestations.sh`): fail-closed `gh attestation verify` on
  tagged supply-chain runs when attestations are produced (public or
  `ENABLE_ARTIFACT_ATTESTATIONS`); soft-skip otherwise. Residuals: project GPG, full SLSA L3,
  non-GHEC private without the opt-in variable.
- Expose typed `backup(destination)` on official SDKs (Python, Go, Perl, Ruby, Erlang) mirroring
  C++ `Client::backup` for wire `BACKUP` (opcode 10): worker-0 routing, ASCII report on success,
  fenced (not hot zero-impact) semantics; admin under secure authz.
- Expose typed C++ `Client::backup(destination)` for wire `BACKUP` (opcode 10): worker-0 routing,
  fenced (not hot zero-impact) semantics.
- Expose online fenced backup on the live daemon: wire opcode `BACKUP` (10) and `Server::backup_to`,
  admin-gated under secure authz.
- Add online durable backup via `Store::backup_to`: fence admissions, flush, copy catalog under the
  open Store lock (writer fence, not fully hot concurrent I/O). Offline CLI still requires a stopped
  Store. Docs/CHANGELOG honesty updated; concurrent-writer unit coverage included.
- Harden E3 rehearsal (not certification): `scripts/assert-e3-rehearsal-honesty.sh` fails closed on
  accidental `e3_certified=yes` / release-eligible labels; campaign `--e3-profile`; weekly
  `durability-evidence.yml` runs campaign-profile loopback/APFS + hosted-ci E0→E3 orchestrator
  rehearsal while keeping `e3_certified=no`.
- Extend secure-profile interop with Phase 5 principal quota → wire `OVERLOADED` (single-connection
  burst) and refresh docs that still claimed the secure matrix was incomplete.
- Add cross-builder SDK archive digest compare (`scripts/compare-sdk-artifact-sums.sh`, job
  `sdk-repro-cross` on `ubuntu-22.04` vs primary supply-chain sums) for tags/dispatch/weekly.
- Normalize Python sdist / Perl tar.gz with `scripts/normalize-tar-gz.sh` (epoch mtimes +
  `gzip -n`) so two-pass reproducibility covers wheels, gems, and those archives.
- Pin `SOURCE_DATE_EPOCH` for SDK packaging (`scripts/export-reproducible-build-env.sh`) and gate
  two-pass wheel/gem digest equality (`scripts/verify-sdk-artifact-reproducibility.sh`) on tags /
  workflow_dispatch. Residual: normalize Python sdist / Perl tar.gz host metadata.
- Expand secure-profile interop to ruby/erlang when toolchains are present (CI already has
  Ruby 3.3 + Erlang/rebar3; local soft-skips). Full official-SDK secure matrix is now opt-in by
  availability rather than cpp/python/go-only.
- Add GitHub SLSA provenance attestations for tagged SDK packages (`actions/attest` in
  `supply-chain.yml`; public repos or private with `ENABLE_ARTIFACT_ATTESTATIONS=true` / GHEC).
- Include Perl in secure-profile interop when `IO::Socket::SSL` is present (CI installs
  `libio-socket-ssl-perl`). Residual: ruby/erlang in that matrix.
- Wire Cosign/Sigstore keyless `sign-blob` for packaged SDK artifacts on tag pushes
  (`.github/workflows/supply-chain.yml`); verify bundles in-job. Project GPG remains optional.
- Add gitleaks + Trivy filesystem scanning CI (`.github/workflows/supply-chain-scan.yml`) for
  Phase 7.1 secret/dependency gates on main and PRs.
- Expand secure-profile interop smoke (`scripts/test-secure-profile-interop.sh`): authz deny for
  unmapped mTLS principals, `prefix=` key-scope allow/deny, and `--tls-crl` rejection of revoked
  client certs (cpp/python/go happy path unchanged). Residual: perl/ruby/erlang in that matrix.
- Add first-slice secure-profile interop smoke (`scripts/test-secure-profile-interop.sh`): mTLS
  client/server PEMs, `--authz-map` write principal, pinned `--worker-hash-seed` under
  `--secure-profile`, cpp/python/go PUT→GET + keyed owner checks; wired into CI `sdk-clients`
  (`timeout-minutes: 5`, TLS build forced `GLYPHASTORE_ENABLE_TLS=ON`).
- Add supply-chain CI gate (`.github/workflows/supply-chain.yml`): package SDKs, require `syft`
  SPDX JSON (`SYFT_REQUIRED=1`), upload `SHA256SUMS` + `*.spdx.json`. Tag Cosign keyless signing
  and GitHub SLSA attestations land on tags (public / `ENABLE_ARTIFACT_ATTESTATIONS`); verify
  via `scripts/verify-sdk-artifact-attestations.sh`. Residual: project GPG / full SLSA L3.
- Complete the ADR 0030 keyed Worker routing SDK train: Python / Perl / Go / Erlang / Ruby decode
  plain and extended INIT identities, implement SipHash-2-4 bit-for-bit with C++, and route with
  the disclosed seed. Cleartext FNV default path unchanged. Interop harness runs FNV for the full
  worker list and, when `INTEROP_KEYED=1` (default), a SipHash cleartext matrix for workers 2/4.
- Add Phase 8 Unix-domain socket transport with optional peer-credential principals
  ([ADR 0029](docs/adr/0029-uds-peercred.md)): `--unix-socket PATH`, `--unix-peercred` →
  `unix:uid=N` for `--authz-map`; `--secure-profile` requires peercred when UDS is enabled.
  Linux `SO_PEERCRED`, macOS/FreeBSD/OpenBSD `getpeereid`. Not a TLS replacement.
- Add Phase 8 keyed Worker routing ([ADR 0030](docs/adr/0030-keyed-worker-routing.md)):
  `--worker-hash-seed`; Manifest-persisted seed; INIT identity extension; C++ client train;
  `--secure-profile` randomizes unless pinned. Default Stores stay FNV-1a. Not full multi-tenant.
- Add Phase 8 hash-flood + STATS isolation slices (not a full multi-tenant product):
  - Keyed Index mix seed ([ADR 0026](docs/adr/0026-keyed-index-hash-seed.md)):
    `--index-hash-seed`; `--secure-profile` randomizes unless pinned.
  - Prefix-scoped principals need `admin` for daemon-wide `STATS`
    ([ADR 0027](docs/adr/0027-stats-isolation-prefix-principals.md)).
  - Per-tenant data-dir deferred honestly
    ([ADR 0028](docs/adr/0028-per-tenant-data-dir-deferred.md)).
- Add Phase 8 first-slice key-prefix authz scope (`prefix=` in `--authz-map`, ADR 0025): deny
  cross-prefix `GET`/`PUT`/`ERASE` with wire `PERMISSION_DENIED`; omit prefix for whole-keyspace
  principals. Document residual risks (shared data dir; keyed routing now closed by ADR 0030 SDK
  train) — not a full multi-tenant product.
- Implement Phase 6 security audit + CRL fail-closed: JSON-lines `auth`/`authz`/`tls` events
  (`SecurityAudit`), `STATS` auth/tls/authz counters, `--tls-crl` / `--tls-ocsp-fail-closed`
  (CRL required; live AIA OCSP HTTP unsupported), OpenBSD unveil of CRL paths, and
  [secure-profile-certs.md](docs/operations/secure-profile-certs.md). Residual public-bind blockers:
  operator CRL config, multi-tenant Phase 8 remainder, physical E3.
- Add operator-ready E3 campaign package: `scripts/run-e3-campaign.sh` orchestrates E0→E1→E2→E3
  (many reps, evidence tarball + SHA-256 manifest) while keeping `e3_certified=no`; document pin,
  PASS/FAIL/INCONCLUSIVE, artifact layout, and human promotion gate in
  `docs/operations/e3-campaign.md` and `platform-durability-evidence.md`. No row is E3 certified.
- Add multi-hour soak profiles (`smoke`/`long`/`1h`/`4h`) to `scripts/soak_daemon.sh` with optional
  RSS and STATS sampling (rotation/compaction counters); document honesty in
  `docs/operations/soak.md` and production-readiness. Wire optional CI via
  `.github/workflows/soak-extended.yml` (`workflow_dispatch` + monthly 1h only — not every PR).
- Implement OpenBSD Phase 6.5 `pledge`/`unveil` confinement after `Server::create` (data dir +
  TLS/authz paths; fail closed; no-op on Linux/macOS/FreeBSD). Cover promise-set unit tests and
  OpenBSD CI grep for `openbsd-sandbox=pledge+unveil`.

- Implement Secure profile Phase 5 abuse / DoS controls: process-wide `--max-accepts-per-sec`,
  `--idle-timeout-ms` / `--request-timeout-ms`, per-connection and per-principal request/bandwidth
  quotas, shared `AbuseController`, `STATS` `abuse_*` counters, and `--secure-profile` defaults that
  refuse explicit `0`. Trusted cleartext keeps limits disabled unless set.
- Close Ruby SDK TLS honesty gap: opt-in TLS 1.3 (`ClientConfig#tls`, CA / mTLS / hostname verify,
  lab `insecure_skip_verify`) matching Go/Python/Perl; include Ruby in the TLS interop matrix;
  retire the “Ruby cleartext exception” from the security same-train docs.
- Wire tagged release-artifact compatibility into CI (`released-artifact-compat` job in
  `.github/workflows/release-compat.yml`): decode in-tree `tests/fixtures/released/` (including
  `self-v1`), package a per-SHA self artifact on push/PR, and on tags package + upload
  `released/<label>/`. Register `released_artifact_compat_tests` in the unit test binary.
- Advance E3/E4 durability certification scaffolding: `scripts/run-e3-block-reset.sh` provisions
  disposable linux-ext4 (loopback + optional dm-flakey) and macOS APFS (hdiutil) rows, arms abrupt
  block-device reset at crash-harness checkpoints, remounts with non-repairing fsck, and records
  honest `e3_certified=no` artifacts; expose `glyphastore_crash_persistence --mode seed`; add
  `.github/workflows/durability-evidence.yml` for E2 collector + E3 harness smoke; document PASS/FAIL
  and promotion rules in `platform-durability-evidence.md`. No filesystem row is E3/E4 certified.
- Advance durable hot-cache probing to Swiss-style H2 control bytes with SIMD/scalar 8-slot group
  matching (shared `swiss_control_group.hpp`), and keep full-key identity checks. Defer catalog
  shared-lock acquisition in `prepare_get` / cold revalidation off the ordinary hot path (Worker
  mutex only until a cold miss needs a generation pin). Add `hot_record_table` unit coverage and
  record comparative GET medians in `docs/benchmarks/get-path-hot-cache-simd-2026-07-25.md`.
- Finish durable GET path follow-up optimizations: slim `prepare_get` critical section (hot snapshot
  then unlock; pin only on cold miss; deferred TTL drain only when backlog non-empty), move hot-cache
  bookkeeping to cache-line-aligned relaxed atomics, gate fine-grained GET timing out of Release
  builds (`NDEBUG`, overridable with `GLYPHASTORE_GET_PATH_TIMING`), and replace the hot map with a
  flat open-addressed table (FNV hash, load 0.5, 48-byte inline values, in-place staging). Raise
  default GET-path bench ops. The prior −20% v32 regression is closed at credible op counts.
- Record durable GET path + hot-cache optimization notes (comparative microbenchmarks, sanitizer
  notes, preserved invariants, and discarded alternatives) without retaining run artifacts in-tree.
- Tighten durable hot-cache structure: max_load_factor 0.5, geometric reserve, 32-byte inline
  values to avoid heap allocations on small payloads, and documented per-entry accounting. Hash is
  never treated as identity; full key compare remains mandatory on collision.
- Add explicit durable hot-cache controls: `hot_cache_enabled`, `max_hot_cache_value_bytes`
  (default 64KiB), daemon `--disable-hot-cache` / `--max-hot-cache-value-bytes`, and stats for
  hit-rate, size-rejected, enabled, and max value. Oversized values never admit; disabling the
  cache leaves cold pinned reads correct.
- Defer durable Index TTL reclaim to a bounded per-Worker backlog drained by existing Worker paths
  (`prepare_get`, `mutate`). Expired GETs still return `not_found` immediately, drop hot rows, and
  never serve expired values; reclaim verifies exact `RecordRef` before erase so reinserts survive.
- Resolve durable GET generation pins in O(1) via a dense `SegmentId` → catalog-slot side table
  rebuilt on recovery, rotation, and compaction. Index↔catalog identity, generation, owner, and
  pin-object checks remain mandatory; `RecordRef` identity is unchanged.
- Add low-overhead durable GET path telemetry (`get_path_stats()`): Worker mutex wait, prepare/complete
  lock hold, Index/hot-cache/generation-pin lookup time, cold read and CRC/value-copy time,
  relinearization retries, hot-cache hit/miss/stale/eviction, expired-TTL GETs, and hot-cache resident
  bytes/entries. Extend `hot_cache_stats()` with stale, eviction, size-rejected, and expired counters.
  Behavioral GET/hot-cache optimizations follow in later commits; capture baselines first.
- Wire the consolidation slice so documented secure-profile / migrate / STATS surfaces match the
  binary: CMake builds `authz.cpp`, `store_migrate.cpp`, `glyphastore_migrate_store`, and the orphan
  unit tests; mTLS extracts principal (URI SAN → DNS SAN → CN); the reactor enforces
  `--authz-map` with wire `PERMISSION_DENIED` (8); `--secure-profile` fails closed (no dual
  `--tls-port`); durable lanes export `LatencyHistogram` needles plus
  `maintenance_rate_window_*` in `STATS`; daemon CLI exposes
  `--maintenance-max-copy-bytes-per-sec` / `--maintenance-max-cpu-ms-per-window` and extends
  `--dump-config`. Official SDKs (including Ruby) share the TLS “same train” policy for opt-in
  TLS 1.3 connect options.
- Complete durable TCP daemon software path (P0-01). Extend `--dump-config` with maintenance
  thresholds, durable resource defaults, disk-read settings, and group batch fields. Extend
  `glyphastore_crash_daemon` with pre-commit PUT and post-ack ERASE checkpoints. Mark P0-01
  software-complete; E3/E4 power-loss remain open (histogram export now wired into STATS).
- Add end-to-end operator guide for durable `glyphastored` deployments
  (`docs/operations/durable-tcp-daemon.md`): profile or explicit storage mode, data directory and
  open policy, resource/batch/maintenance flags, `HEALTH`/`READY`/`STATS` expectations, shutdown
  drain, offline backup/verify/repair pointers, and explicit unsupported claims (live backup,
  power-loss certification pending). Link from operations index, CLI reference, production readiness,
  and v1 production roadmap.
- Close durable unread-TTL normal-mode policy fail-closed. Default scheduling stays conservative
  (Index-referenced dead bytes only). Opt-in `unread_ttl_normal_scheduling` probes unread expired
  sealed puts during normal evaluations and adds them to `candidate_scheduling_dead_byte_ratio_bp`
  for the inclusive dead-byte threshold only; compaction still uses the sole `Store::compact()`
  path and copy budget still uses exact live bytes. Export scheduling ratio through
  `MaintenanceSnapshot` and daemon `STATS`. Add daemon CLI/config for unread-TTL probe and normal
  scheduling flags.
- Document permanent refusal of `glyphastore_rebuild_index` for durable v1 with explicit operator
  paths via Store recovery and `glyphastore_repair_store`. Sync persistence roadmap: software P0-08
  policy slices closed; controlled native baselines and E3/E4 power-loss certification remain open.
- Add fail-closed JSON-lines structured logging for `glyphastored` lifecycle events (`start`, `listen`,
  `ready`, `maintenance_emergency`, `maintenance_fault`, `shutdown_begin`, `shutdown_drain_begin`,
  `shutdown_drain_end`, `stopped`, `executor_failure`). Opt in with `--log-format json` (default
  `human` keeps legacy stdout/stderr). Structured fields are bounded, omit secrets, and `--quiet`
  suppresses only the normal startup/shutdown lifecycle lines.
- Add operator runbooks under `docs/operations/` for graceful drain and overload (`HEALTH`/`READY`/`STATS`,
  `--shutdown-drain-ms`), offline backup/restore (`glyphastore_backup_store`, `glyphastore_verify_store`),
  and corruption detection/repair (`glyphastore_verify_store`, `glyphastore_inspect_segment`,
  `glyphastore_repair_store` with quarantine outside the live store). Link from the documentation index,
  production readiness, persistence roadmap, and architecture backup-restore guide.
- Add fail-closed unread-TTL observability for pressure/emergency maintenance. When
  `unread_ttl_pressure_probe` is enabled (default), background evaluations under segment or
  free-space pressure, or emergency, perform a bounded sealed-Index probe of the round-robin
  candidate and export unread expired sealed Record counts/bytes through
  `MaintenanceObservation`, `MaintenanceSnapshot`, and daemon `STATS`. Normal policy remains
  Index-referenced dead bytes until pressure or an explicit `Store::compact()` visit. Fail-closed
  probe faults disable auto-compact unless an emergency gate is already armed.
- Add a deterministic whole-Worker durable compaction planner, generation-safe v1 manifest
  replacement, physical temporary/peak/amplification gates, a checksummed dual-manifest intent
  codec, descriptor-relative intent publication/removal, and restart resolution against exactly
  the old or next authority with validated, idempotent Segment retirement. Add a durable builder
  that prebuilds the new Index, preserves visible v1 Record bytes and sequences, reclaims
  expired/superseded/tombstoned history, and validates sealed replacements. Runtime installation
  publishes the prepared manifest, commit catalog, and Worker Index atomically and retires sources.
  Public `Store::compact()` selects Workers round-robin without a background thread, skips exact
  no-gain layouts, executes at most one transaction per call, and returns copy statistics.
- Add fail-closed daemon deployment profiles (`dev`, `embedded`, `production`) with precedence
  `defaults < profile < file < env < CLI`. Unknown profile names fail before listen; `--dump-config`
  prints the selected profile plus resolved settings.
- Normatively specify duplicate `request_id` and reconnect semantics for wire protocol v2. `request_id`
  is correlation-only with no server deduplication; transport loss requires re-`INIT`/`BIND_WORKER`;
  mutations with bytes sent remain indeterminate until application reconciliation. Update
  [wire protocol v2 §8.1 and §10.1](docs/spec/wire-protocol-v2.md), [client semantics v1 §5](docs/spec/client-semantics-v1.md),
  ADR 0019, and close the corresponding production-roadmap bullets.
- Expose durable no-gain planning work that previously looked identical to cheap scheduler skips.
  `Store::compact()` still returns `compacted == false` without publishing an intent, but now reports
  the Worker examined plus exact Index-referenced sealed Record/byte counts verified before the
  layout rejected the rewrite. `MaintenanceSnapshot` and daemon `STATS` accumulate last/total no-gain
  scan counters alongside `skips`, `consecutive_no_gain`, and `last_skip_reason`.
- Replace fail-fast unrelated-Worker rotation during a durable compaction lease with condition-based
  serialization: the rotation now waits, rebuilds from the newly published Manifest, commits, and
  survives reopen without changing persistence v1. Advance the automatic round-robin cursor for
  every observed candidate so a below-threshold Worker cannot starve reclaimable peers, and expose
  a cumulative maintenance sequence-conflict counter through snapshots and daemon `STATS`. Extend
  the maintenance benchmark with forced-rotation, idle, and sustained-churn scenarios. On the clean
  seven-repeat macOS/APFS follow-up, all forced rotations commit without foreground errors; the
  serialized boundary costs about 2.5x median latency, product-default idle duty is about 0.0018%,
  and seven 1 GiB churn samples finish with four Segments instead of 22 at a 2.9% median throughput
  cost. Add atomic runtime rotation telemetry for attempts, commits, compaction waits, and
  last/total/maximum publication-wait, execution, and total durations; surface it through
  `MaintenanceSnapshot`, daemon `STATS`, and phase-aware maintenance CSV. The clean macOS matrix
  attributes 71--75% of forced-overlap latency to publication wait; under 1 GiB churn only one of
  16 rotations waits, all commit, and background maintenance retains four versus 22 Segments at a
  2.5% median throughput cost. Split execution telemetry into Segment seal, replacement Segment
  creation, Manifest publication, residual in-memory execution, and the post-rotation final Record
  commit. Serialize only the short multi-writer atomic statistics update, without adding a storage
  lock or extending the compaction publication lease. The clean macOS deep-phase matrix attributes
  65--72% of forced rotation execution to replacement creation and about 0.3 ms to final Record
  commit. Require the churn harness to observe a complete quiescent Worker sweep before settling;
  all corrected samples compact to four instead of 22 Segments.
- Add a reproducible concurrent-maintenance benchmark comparing disabled, cooperative, and
  Store-owned background policy under a synchronized mixed GET/PUT workload. Record a clean
  seven-repeat macOS/APFS matrix with raw CSV: both maintenance modes complete the same 31.01 MiB
  useful compaction without conflict, while median foreground throughput falls about 18% and p99
  rises 54--57% versus disabled. Cooperative and background medians are effectively equal. A
  rotation-forcing calibration exposed unrelated-Worker fail-fast publication conflict; the follow-up
  above closes and measures that path.
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
  limit above. No-gain planning scans now expose public counters through `CompactionResult`,
  `MaintenanceSnapshot`, and daemon `STATS`. Unread TTL remains conservative under normal
  policy, and concurrent foreground cost is measured by the dedicated follow-up matrix.
- Add the platform durability evidence matrix with cumulative E0–E4 claim levels, APFS/Linux/BSD
  row inventory, artifact/promotion requirements, and a controlled power-loss campaign protocol.
  Add a portable collector that records source, OS, hardware class, filesystem/mount, toolchain,
  commands, results, and SHA-256 provenance while limiting itself to metadata or process-kill
  evidence.
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
