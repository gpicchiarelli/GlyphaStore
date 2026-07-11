# ADR 0007: SwissTable Index partitions

- Status: accepted
- Date: 2026-07-11

Each Worker owns an Index partition implemented as a flat open-addressing SwissTable-style table with
8-slot groups, control-byte fingerprints (H2), H1 group probing, seeded key hashing, inline key
storage up to 24 bytes, and heap spill for longer keys. The bootstrap `std::unordered_map` backend
is removed from the runtime; recovery scratch maps during segment scan remain offline-only.

The table is the production shape. SIMD group matching, reference-based keys, and incremental
resize tuning are performance refinements on this layout, not alternate architectures.
