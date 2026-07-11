# Index model

The Store has one logical Index, physically partitionable by Worker ownership. Public design uses
the term `Index`, not `KeyIndex`.

```text
full key -> RecordRef(segment_id, offset, size, sequence, generation)
```

The bootstrap backend deliberately uses a standard container for correctness. The production
destination is a benchmark-selected flat open-addressing table inspired by SwissTable/F14, with
group metadata, hash fingerprints, controlled load factor, seeded hashing, bounded probing policy,
and incremental resizing.

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
