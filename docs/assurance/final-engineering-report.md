Status: descriptive summary of the assurance engineering program
Applies to: release claim honesty for GlyphaStore 0.1.x
Owner: maintainers
Last reviewed: 2026-08-01

# Final engineering report (assurance program)

## Verdict

**Claim ceiling remains architectural prototype.** Phases A–E of the assurance catalog are
implemented in-tree with machine-readable requirements, hazards, multi-state gates, and CI
validators. That is necessary engineering structure; it is **not** alpha/beta/RC/stable
certification and must not be read as production readiness.

## What landed

| Phase | Outcome |
| --- | --- |
| A | `GS-*` requirements, hazard register, multi-state gates, `validate_assurance.py`, generated readiness view |
| B | Linearizability/fault/memory-order work, error taxonomy, `legacy_mutex` policy (merged earlier) |
| C | CMake `add_subdirectory` split, dependency matrix, structure-debt thresholds; **WAV-001 revoked** after source/test splits |
| D | N↔N-1 matrix, SHA-pinned Actions (74 uses), claim schema/packaging on tags |
| E | Performance/soak/overload budget catalog linked to `GATE-PERFORMANCE` / `GATE-SOAK` / `GATE-OPS-RUNBOOKS` |

Authority roots: `engineering/`, validators under `engineering/tools/`, workflow
`.github/workflows/assurance.yml`.

## Honest residuals (do not close by checklist)

1. **GitHub Actions billing** — hosted workflows may not run until billing is restored; local
   validator/build success ≠ green CI.
2. **Physical E3/E4 durability** — still open; rehearsals keep `e3_certified=no`.
3. **Tagged N−1 fixture drops** — policy matrix exists; permanent prior-release trees under
   `tests/fixtures/released/<label>/` remain a release-process step.
4. **Hardware performance** — `HARDWARE-PAIRED-LINUX-AB` is `specified_waiting_for_runner`
   until `glyphastore-linux-perf` publishes `pass-candidate`.
5. **Hardware soak / mandatory rotation** — software soak paths exist; controlled multi-hour
   hardware soak residual remains on `GATE-SOAK`.
6. **Project GPG / full SLSA L3** — optional residuals on supply-chain gates.
7. **Zero-fence hot backup** — online fenced backup exists (shorter fence + structural source check +
   bounded Segment copy parallelism; destination CRC promotion gate); fully concurrent hot copy
   remains deferred per [ADR 0034](../adr/0034-zero-fence-hot-backup-deferred.md). HAZ-021 incomplete
   destination / failed-online paths, Store process-kill mid-copy (`glyphastore_crash_backup`),
   in-process Server/wire BACKUP kill (`glyphastore_crash_backup_wire`), and real `glyphastored`
   exec mid-BACKUP kill (`glyphastore_crash_backup_daemon`, lab-only crash hooks) are covered.
   Daemon `--request-timeout-ms` / idle timeout vs client contract (no cancel of admitted Store
   mutations; partial-frame close) is covered by reactor integration tests.

## Gate posture (summary)

- Many alpha verification / public-contract gates are `PROVATA_IN_CI` or `IMPLEMENTATA` for
  **software-path** evidence.
- Beta/RC distribution and absolute performance claims still carry open residuals above.
- Generated view: [`docs/production-readiness.md`](../production-readiness.md) (non-authoritative
  narrative; `engineering/gates/quality-gates.yaml` is authority).

## How to re-verify locally

```bash
python3 engineering/tools/validate_assurance.py --write-generated
python3 engineering/tools/validate_cmake_deps.py
python3 engineering/tools/validate_structure_debt.py
python3 engineering/tools/validate_actions_pins.py
python3 engineering/tools/validate_compat_matrix.py
python3 engineering/tools/validate_claims.py
python3 engineering/tools/validate_perf_budgets.py
```

After billing is restored, re-run workflows on `main` before treating CI as green.

## Recommendation

Ship further product work under the **architectural prototype** ceiling. Promote claim level
only when residuals that match the target level (especially E3, hardware perf, tagged N−1
fixtures, and hosted CI green) are closed with retained evidence under `engineering/claims/`
and `engineering/evidence/`.
