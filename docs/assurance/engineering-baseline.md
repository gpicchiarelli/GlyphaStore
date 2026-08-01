Status: descriptive
Applies to: assurance program Phase A baseline (0.1.x)
Owner: maintainers
Last reviewed: 2026-08-01

# Engineering baseline (Phase A)

GlyphaStore remains an **architectural prototype**. This document records the gap recon that
precedes the machine-readable assurance system under `engineering/`. It does not advance any
release level.

## Current state (what already exists)

| Area | Present | Notes |
| --- | --- | --- |
| Narrative readiness checklist | `docs/production-readiness.md` | Checkbox model; now **derived** from `engineering/gates/` |
| Normative specs | `docs/spec/*` | architecture, persistence v1, wire v2, client semantics, concurrency, recovery matrix, index, benchmarks |
| ADRs | `docs/adr/` | Including paired shards (0031), paired Store concurrency (0032), TLS/authz, keyed routing |
| CI | `.github/workflows/*` | build/test, sanitizers+fuzz smoke, durability E2/E3 rehearsal, release-compat, supply-chain, soak, BSD |
| Crash / recovery proofs | `tests/crash/`, integration recovery | E2 process-kill; not physical E3/E4 |
| SDKs | C++/Python/Perl/Go/Erlang/Ruby | Shared fixtures + client-semantics contract |
| Daemon runtime | paired Reader–Writer only | Volatile engine under `src/experimental/` is lab-only |

## Gaps (doc ↔ code ↔ evidence)

| Gap | Severity | Priority | Dependencies | Residual risk if deferred |
| --- | --- | --- | --- | --- |
| No machine-readable requirement IDs (`GS-*`) | High | P0 (Phase A) | schemas + validator | Silent drift; checklist theatre |
| No formal hazard register | High | P0 (Phase A) | requirements linkage | Untracked integrity failures |
| Gates not multi-state / not CI-enforced | High | P0 (Phase A) | evidence paths | Closed boxes without proofs |
| Monolithic oversized sources (Phase C + WAV-001 splits) | Medium | P2 (Phase C) | dependency matrix | Done; WAV-001 revoked |
| No formal linearizability checker | High | P1 (Phase B) | history recorder | Concurrency regressions undetected |
| Actions not pinned to commit SHA (Phase D: SHA pin + validator) | Medium | P2 (Phase D) | supply-chain policy | Done baseline; Dependabot must update SHAs |
| Physical E3/E4 durability unset | High (claim honesty) | Keep open | hardware campaign | False durability confirmation |
| Unified error taxonomy across wire/embedded/SDK | Medium | P1 (Phase B) | `docs/spec/error-taxonomy-v1.md` + fixture | Transport/deadline narrative vectors beyond fixture |
| `legacy_mutex` policy not yet formalized | Medium | P1 (Phase B) | `docs/architecture/legacy-mutex-policy.md` | Removal still pending 0.2 |
| Performance budgets not gate-linked (Phase E: budget catalog) | Medium | P2 (Phase E) | hardware CI | Done baseline; absolute claims wait for `glyphastore-linux-perf` |

## Strategy

1. **Phase A (this delivery):** schemas, requirement catalog bootstrap, hazard register (§7),
   multi-state gates, validator CI, generated readiness view. Claim ceiling stays *architectural
   prototype*.
2. **Phase B:** concurrency linearizability + fault hooks + memory-order inventory + reduced TLA+;
   fault matrix honesty; unified error taxonomy; `legacy_mutex` freeze/removal policy.
3. **Phase C:** CMake component split + dependency matrix + complexity/waiver CI.
4. **Phase D:** N↔N-1 compat matrix, SHA-pinned Actions, claims on tags.
5. **Phase E:** performance budgets with hardware-gated release thresholds; soak/overload linked to
   gate IDs.

## Authority rule

- Structured data under `engineering/` is the single authority for requirements, hazards, and gates.
- Markdown under `docs/assurance/` and `docs/production-readiness.md` is generated or descriptive.
- Specs/ADRs remain normative for on-disk and wire behavior; requirements **reference** them, they
  do not replace them.

## Immediate acceptance (Phase A)

- `engineering/tools/validate_assurance.py` exits 0 locally and in CI (`.github/workflows/assurance.yml`).
- ≥1 requirement file per category directory listed in the program plan.
- Hazard register covers every brief §7 listed event.
- Production-readiness view is generated from gates (or clearly non-authoritative).
- README / CONTRIBUTING / `AGENTS.md` point at the assurance system.
- No production-ready language beyond existing prototype disclaimers.
