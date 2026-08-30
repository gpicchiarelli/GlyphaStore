# Assurance catalog (authority)

Machine-readable requirements, hazards, quality gates, waivers, claims, and evidence
pointers for GlyphaStore. Markdown under `docs/production-readiness.md` and
`docs/assurance/` is **generated** from this tree.

GlyphaStore remains an **architectural prototype**. Do not treat closed checklist
boxes as production readiness.

## Layout

| Path | Contents |
| --- | --- |
| `schemas/` | JSON Schema for requirements, hazards, gates, waivers |
| `requirements/` | `GS-*` requirements by category |
| `hazards/` | Hazard register (brief §7 coverage) |
| `gates/` | Multi-state quality gates |
| `waivers/` | Time-bounded waivers (CI rejects expired) |
| `build/` | CMake dependency matrix + structure debt thresholds |
| `compatibility/` | N↔N-1 matrix |
| `performance/` | Hosted vs hardware budgets + soak/overload linkage |
| `claims/` / `evidence/` | Release claims and retained evidence pointers |
| `tools/validate_assurance.py` | Validator + Markdown generator |
| `tools/validate_documentation.py` | All tracked Markdown: UTF-8 and repository-local link integrity |
| `tools/run_clang_tidy_gate.py` | Production compile database: fail-closed high-signal diagnostics |
| `tools/validate_cmake_deps.py` | Phase C CMake layout + include rules |
| `tools/validate_structure_debt.py` | Size/TODO thresholds with waivers |
| `tools/validate_actions_pins.py` | SHA-pinned Actions |
| `tools/validate_compat_matrix.py` | N↔N-1 matrix |
| `tools/validate_claims.py` | Release claim schema |
| `tools/validate_perf_budgets.py` | Performance/soak budget catalog |
| `tools/release_identity.py` / `release_bundle.py` | Tag identity, manifests, checksums and transitive seals |
| `tools/prior_release.py` / fixture validators | Fail-closed prior ABI/wire release selection and retained consumer extraction |
| `tools/compare_release_rebuild.py` | Closed-set independent release archive and build-authority comparison |
| `tools/validate_bsd_packaging.py` | Reference-port invariants and native release prerequisites |
| `formal/shard_pair/` | Reduced TLA+ ShardPair model + TLC helper |
| `formal/persistence/` | Abstract write/sync/commit-slot/Manifest/recovery TLA+ model |

## Validate

```bash
python3 -m pip install PyYAML jsonschema
python3 engineering/tools/validate_assurance.py
python3 engineering/tools/validate_assurance.py --write-generated
python3 engineering/tools/validate_documentation.py
python3 engineering/tools/run_clang_tidy_gate.py --build-dir build/unix-clang-tidy
python3 engineering/tools/validate_cmake_deps.py
python3 engineering/tools/validate_structure_debt.py
python3 engineering/tools/validate_actions_pins.py
python3 engineering/tools/validate_compat_matrix.py
python3 engineering/tools/validate_claims.py
python3 engineering/tools/validate_perf_budgets.py
python3 engineering/tools/validate_bsd_packaging.py
python3 -m unittest scripts.tests.test_release_artifacts scripts.tests.test_bsd_packaging \
  scripts.tests.test_artifact_release_workflow
```

CI: `.github/workflows/assurance.yml`.
