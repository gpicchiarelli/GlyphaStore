# Contributing

GlyphaStore is currently an architecture prototype under active design.

## Development loop

1. Bootstrap the local toolchain with `./scripts/bootstrap-macos.sh` on macOS.
2. Create a focused branch.
3. Run `./scripts/dev.sh format`, `./scripts/dev.sh test`, and the relevant sanitizer.
4. Add regression tests for every correctness or memory-safety fix.
5. Update an ADR when changing a fixed architectural decision.

Persistence work must follow the accepted
[durability and recovery contract](docs/architecture/durability-recovery.md). Public API work must
preserve the [ownership and compatibility contract](docs/architecture/public-api-contract.md).
Changing a commit point, routing metadata, recovery authority, or read lifetime requires a new ADR.

Use Conventional Commits. Keep public terminology aligned with the architecture documents: the
public model has a Store, Workers, an Index, and Segments. Do not introduce text-protocol
compatibility layers or unbounded client-controlled work without an accepted design change.

## Safety gates

- No raw owning pointers or manual `new`/`delete` in production code.
- No persisted struct casts; use explicit little-endian codecs.
- No unchecked length, offset, capacity, or allocation arithmetic.
- No record parser without malformed-input tests and fuzz coverage.
- No reclaim while a reader may still address a segment.
- No performance claim without a reproducible benchmark and hardware description.
