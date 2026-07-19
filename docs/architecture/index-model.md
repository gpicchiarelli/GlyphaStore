# Index model

The Store has one logical Index, physically partitionable by Worker ownership. Public design uses
the term `Index`, not `KeyIndex`.

```text
full key -> RecordRef(segment_id, offset, size, sequence, generation)
```

The production Index partition is a SwissTable-style flat open-addressing table with group control
bytes, hash fingerprints, a 7/8 load limit, cached full hashes, inline keys through 24 bytes, and a
long-key arena. The exact algorithm is specified in [Index v1](../spec/index-v1.md); rationale is in
[ADR 0007](../adr/0007-swiss-table-index.md).

Further layout and SIMD choices must be based on GlyphaStore workloads:

- key sizes 8, 16, 32, 64, and 256 bytes;
- hit and miss ratios;
- insert/replace/delete mixes;
- millions of entries;
- iteration during vacuum;
- memory bytes per entry;
- p50, p99, and p99.9 probe/operation costs.

The current Index owns and duplicates every key: up to 24 bytes inline, longer keys in `KeyArena`.
A future reference-based layout is permitted only after lifetime, vacuum publication, and cache
effects are proven.
