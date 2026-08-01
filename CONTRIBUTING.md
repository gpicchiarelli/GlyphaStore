# Contributing

GlyphaStore is currently an architecture prototype under active design. Read
[AGENTS.md](AGENTS.md) for the promise = requirement + spec + implementation + proof + evidence +
residual risk rule and the non-negotiables.

## Licensing

GlyphaStore is BSD-3-Clause ([LICENSE](LICENSE)). Copyright (c) 2026 Giacomo Picchiarelli.
Third-party attributions: [NOTICE](NOTICE), [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
Contributor and redistributor rules: [docs/legal/licensing.md](docs/legal/licensing.md).

By contributing, you license your contribution under BSD-3-Clause and confirm you have the
right to do so. Do not add exclusive GPL/AGPL/SSPL runtime dependencies. Prefer SPDX headers on
new substantial source files:

```text
// SPDX-License-Identifier: BSD-3-Clause
// SPDX-FileCopyrightText: 2026 Giacomo Picchiarelli
```

## Development loop

1. Bootstrap the local toolchain with `./scripts/bootstrap-macos.sh` on macOS.
2. Create a focused branch.
3. Run `./scripts/dev.sh format`, `./scripts/dev.sh test`, and the relevant sanitizer.
4. Add regression tests for every correctness or memory-safety fix.
5. Update an ADR when changing a fixed architectural decision.
6. If the change affects a promised behavior or gate, update `engineering/` and run
   `python3 engineering/tools/validate_assurance.py` (use `--write-generated` when gates change).

Persistence work must follow the accepted
[durability and recovery contract](docs/architecture/durability-recovery.md). Public API work must
preserve the [ownership and compatibility contract](docs/architecture/public-api-contract.md).
Changing a commit point, routing metadata, recovery authority, or read lifetime requires a new ADR.

Use Conventional Commits. Keep public terminology aligned with the architecture documents: the
public model has a Store, Workers, an Index, and Segments. Do not introduce text-protocol
compatibility layers or unbounded client-controlled work without an accepted design change.

## Assurance

- Authority: [`engineering/`](engineering/README.md) (requirements, hazards, gates).
- Derived readiness view: [`docs/production-readiness.md`](docs/production-readiness.md).
- Baseline: [`docs/assurance/engineering-baseline.md`](docs/assurance/engineering-baseline.md).
- CI: `.github/workflows/assurance.yml`.

Do not claim production readiness or close a gate to `ACCETTATA_PER_RILASCIO` / `PROVATA_IN_CI`
without linked proofs and existing evidence paths.

## Safety gates

- No raw owning pointers or manual `new`/`delete` in production code.
- No persisted struct casts; use explicit little-endian codecs.
- No unchecked length, offset, capacity, or allocation arithmetic.
- No record parser without malformed-input tests and fuzz coverage.
- No reclaim while a reader may still address a segment.
- No performance claim without a reproducible benchmark and hardware description.
