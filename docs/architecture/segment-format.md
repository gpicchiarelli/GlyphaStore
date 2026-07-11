# Segment and record format

## Segment

Every Segment is exactly 64 MiB. The first 4 KiB are reserved for an explicitly encoded header and
future format metadata. Record bytes begin at offset 4096 and are appended with 8-byte alignment.

```text
0                    4096                                      64 MiB
+--------------------+----------------------------------------------+
| encoded header     | Record | Record | Record | unused bytes      |
+--------------------+----------------------------------------------+
```

Normal Records do not cross Segment boundaries and are limited to 1 MiB in the prototype. A future
large-object path must use explicit chunking rather than weakening this invariant.

Segment lifecycle:

```text
FREE -> ACTIVE -> SEALED -> RETIRED -> RECLAIMABLE -> FREE/deleted
```

The runtime catalog separately tracks residency, liveness, readers, dirty state, persistence, and
retirement epoch. Volatile pointers and file descriptors never appear in the persistent header.

## Record

The prototype codec uses explicit little-endian fields:

```text
magic, version, header size, total size,
key size, value size, checksum,
sequence, key hash, expiration,
opcode, value type, flags,
full key bytes, value bytes, zero padding
```

The checksum field is treated as zero while CRC32C is computed. Decoders validate every extent
before creating a view. Persisted bytes are never interpreted by casting them to a C++ struct.

The format is not stable yet. Any future stable format requires a compatibility matrix, golden
fixtures, migration policy, and corruption behavior specification.
