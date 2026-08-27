Status: living program map (not generated)
Applies to: multi-bot debt remediation (Waves 0–6, lanes L0–L7)
Owner: L0 orchestrator
Last reviewed: 2026-08-27

# Debt remediation — lane / wave map

Claim ceiling stays **architectural prototype**. Silent-change ban: wire v2, persistence v1,
routing, ACK/RAW, Manifest authority, borrow lifetimes, recovery polarity require ADR +
requirements + proofs + evidence. Evidence labels: [evidence-taxonomy.md](evidence-taxonomy.md).
`src/experimental/` stays out of daemon/install until ADR 0036 (or successor) accepts promotion.

## Lanes

| Lane | Focus | Primary surfaces | Merge note |
| --- | --- | --- | --- |
| **L0** | Hygiene, gate residual sync, evidence taxonomy, merge order | `engineering/`, `docs/assurance/` | First; unblocks parallel waves |
| **L1** | ADR 0036 slot-pool publish → official paired runtime | `shard_pair_runtime`, `read_generation`, experimental → `store/paired/` | Owns publish files exclusively |
| **L2** | TLA+/memory-order inventory, linearizability depth, fault hooks | `engineering/formal/`, concurrency tests | Required litmus before L1 publish merge |
| **L3** | Bench harness, latency sampling, Linux A/B prep | `benchmarks/`, `scripts/benchmark_paired_linux_ab.sh` | Non-regression row before L1 merge |
| **L4** | Crash/fault/ENOSPC/backup/compaction debt; E3 honesty | crash suites, platform durability matrix | Independent unless reclaim/publish semantics change |
| **L5** | Daemon wakeup/SPSC/handoff, GET barrier residual ops, telemetry | `src/server/`, pair writer | No dual TCP / io_uring without ADR+A/B |
| **L6** | Authz/quota/audit, HEALTH/READY, drain, soak adversarial | server security + ops gates | Phase 8: implement or document non-support |
| **L7** | C ABI fixture, BSD packages, signing/SLSA/SBOM, sealed release | `.github/workflows/`, install-consumer | Independent of L1 except publish ABI |

## Waves

| Wave | Lanes | Goal | Silent-change watch |
| --- | --- | --- | --- |
| **0** | L0 | Gate residual sync (0037 Phase C), evidence taxonomy, this map | Docs/assurance only; no wire/ACK |
| **1** | L1+L2+L3 | Integrate direct-object `{epoch,slot}` pool; close V5–V14; accept ADR 0036 | Publish/reclaim/borrow lifetimes |
| **2** | L5 (+L1 liaison) | Hot-path residual: padding, GET/PUT alloc, combining RAW, wakeup/SPSC | No ACK early; no dual ports |
| **3** | L4 | Compaction crash matrix, ENOSPC/EINTR, backup E2E, debt bounds | Manifest/commit-slot polarity |
| **4** | L5+L6 | Handoff/TLS/authz/telemetry/drain/soaks; Phase 8 honesty | QSBR under short-write; OVERLOADED polarity |
| **5** | L7 | C ABI cross-release scaffolding, FreeBSD+OpenBSD package producers, Cosign/SLSA/SBOM paths, sealed `release.yml` | Artifact sealing / N−1 fixtures; honest residuals in [wave5-l7-residuals.md](../distribution/wave5-l7-residuals.md) |
| **6** | L3+L4 (**blocked**) | Hard-pinned 1–8 scaling + absolute budgets + ACCETTATA when runners exist | macOS `local` ≠ scaling |

### Wave 6 blockers (scaffolding landed; proofs not)

Wave 6 docs/scripts may prepare matrices and `specified_waiting_for_runner` placeholders, but
**must not** invent runner results or promote gates:

1. Self-hosted runner label `glyphastore-linux-perf` with retained `pass-candidate` (evidence
   class `hardware`) — see [performance-budgets.md](performance-budgets.md),
   [paired-shards-linux-p1.md](../benchmarks/paired-shards-linux-p1.md),
   harness `scripts/benchmark_paired_linux_ab.sh`.
2. Physical E3 durability lab (E3/E4 remain open; rehearsal ≠ certification —
   [evidence-taxonomy.md](evidence-taxonomy.md)).
3. Absolute p99 GET/PUT thresholds stay **TBD** until hardware samples exist.
4. No `ACCETTATA_PER_RILASCIO` from scaffolding alone.

## Wave progress (living)

| Wave | Status | Notes |
| --- | --- | --- |
| **0** | Landed | Assurance hygiene; evidence taxonomy; this map |
| **3** | In progress (L4) | Compaction `storage_exhausted` + paced pre-intent faults; write-amp budget before intent (`GS-PERSIST-AMP-001`); backup ENOSPC/concurrent fence proofs; `GS-OPS-DEBT-001`; platform-durability evidence path placeholders. **E3/E4 remain open** (rehearsal ≠ certification). |

## Anti-duplication rules

1. **L1 merge lock** on publication/reclaim APIs; other lanes rebase or wait.
2. **L1 publish PRs** need L2 litmus + L3 non-regression before main.
3. Reject PRs that cite only macOS unpinned median for scaling, or that install experimental into `glyphastored` without ADR accept.
4. One purpose per PR; link requirement IDs + evidence dir; Conventional Commits on `debt/<lane>-…` branches.
5. Do not reopen closed 0037 Phase C window residual without an explicit remaining-windows gap requirement.

## Related

- Program plan (external Cursor plan; not committed): debt remediation multi-bot
- [ADR 0036](../adr/0036-generation-slot-pool-publish.md) (proposed), [ADR 0037](../adr/0037-shard-execution-token-flat-combining.md) (accepted)
- Authority: [`engineering/gates/`](../../engineering/gates/), [`engineering/requirements/`](../../engineering/requirements/)
