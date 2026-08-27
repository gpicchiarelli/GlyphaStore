Status: living assurance policy (not generated)
Applies to: evidence paths cited by requirements, gates, benchmarks, and PRs
Owner: L0 / assurance maintainers
Last reviewed: 2026-08-27

# Evidence taxonomy

GlyphaStore remains an **architectural prototype**. Evidence labels below classify *how much*
a result may support; they do not close gates by themselves. A gate closes only when
requirement + spec/ADR + implementation + proofs + retained evidence + residual risk all
align ([AGENTS.md](../../AGENTS.md)).

## Labels

| Label | Meaning | Typical surfaces | May support |
| --- | --- | --- | --- |
| `local` | Developer or lab machine; environment not release-controlled | macOS laptop benches, unpinned cores, ad-hoc scripts under `benchmarks/results/local-*` | Debugging, harness bring-up, **non-scaling** relative signals |
| `CI` | Hosted or self-hosted workflow with pinned actions/scripts | `.github/workflows/*`, sanitizer/crash/fuzz smoke, formal TLC best-effort | Regression/correctness gates (`PROVATA_IN_CI` / local CI proofs) |
| `hardware` | Labeled hard-pinned performance or physical durability lab | Runner `glyphastore-linux-perf`, physical E3/E4 campaigns | Absolute throughput/p99 budgets, multi-core scaling claims, durability certification rows |
| `release` | Tagged artifact + retained supply-chain / compatibility evidence | Signed release assets, SBOM/SLSA, N↔N−1 fixtures, install-consumer on a tag | Distribution/compat claims at the gate’s release level |

One evidence path may carry only one primary label in gate/requirement YAML. Do not mix
`local` medians into a `hardware` claim by rewording the README.

## Hard bans

1. **Do not promote macOS unpinned rows to scaling or absolute performance claims.**
   macOS and other unpinned benches are `local` only. Scaling (1/2/4/8 affine pairs,
   ≥80% physical-core efficiency, no migration) and absolute budgets require the Linux
   hard-pinned path on runner label `glyphastore-linux-perf` with a `pass-candidate`
   manifest ([performance-budgets.md](performance-budgets.md),
   [paired-shards-linux-p1.md](../benchmarks/paired-shards-linux-p1.md)).
2. **Do not cite `src/experimental/` as daemon or install proof.** Experimental code may
   appear in lab/tests/benchmarks only until an ADR promotes it into the official paired
   runtime and install graph (`glyphastored` / installed libs stay free of experimental
   objects).
3. **Do not close a gate with narrative alone.** Design text or code presence without
   linked proof + evidence paths is not closure.
4. **Rehearsal ≠ certification.** E3 script rehearsal remains honesty scaffolding until
   physical campaign evidence exists.

## PR / merge checklist (evidence)

- [ ] Label stated (`local` / `CI` / `hardware` / `release`) next to any new bench or campaign path
- [ ] Scaling or absolute numbers → `hardware` + `glyphastore-linux-perf` (or leave advisory)
- [ ] Experimental paths not wired into install / `glyphastored`
- [ ] Requirement and gate `prove` / `evidenze` updated when behavior or proof moves

## Related

- [Debt remediation lanes](debt-remediation-lanes.md) — which wave owns evidence upgrades (Wave 6
  hardware scaling is blocked on `glyphastore-linux-perf` + physical E3 lab)
- [Performance budgets](performance-budgets.md) — absolute placeholders stay
  `specified_waiting_for_runner`
- [Release checklist](release-checklist.md)
- Generated gate view: [production-readiness.md](../production-readiness.md)
