# Index model

The Store has one logical Index, physically partitionable by Worker ownership. Public design uses
the term `Index`, not `KeyIndex`.

```text
full key -> RecordRef(segment_id, offset, size, sequence, generation)
```

The production Index partition is a SwissTable-style flat open-addressing table with group control
bytes, hash fingerprints, controlled load factor, seeded hashing, and bounded group probing. See
`docs/adr/0007-swiss-table-index.md`.

The final choice must be based on GlyphaStore workloads:

- key sizes 8, 16, 32, 64, and 256 bytes;
- hit and miss ratios;
- insert/replace/delete mixes;
- millions of entries;
- iteration during vacuum;
- memory bytes per entry;
- p50, p99, and p99.9 probe/operation costs.

Key bytes may be stored in the Index or referenced from immutable Records. The safer bootstrap
duplicates keys. A reference-based layout is permitted only after lifetime, vacuum publication,
and cache effects are proven.
