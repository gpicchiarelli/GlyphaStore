# Format compatibility matrix

This document records the formats understood by the current `0.1.x` code line. It is an executable
development matrix, not yet a released persistence guarantee: `durable_sync` remains disabled and
no released artifact migration has been exercised.

Format versions are independent. A library version never implies that manifest, Segment, Record,
and wire versions advance together.

## Current codecs

| Component | Current version | Current reader | Current writer | Canonical evidence | Status |
|---|---:|---|---|---|---|
| Manifest | 1 | Exact v1 only | Emits v1 | [`manifest_v1.hex`](../../tests/fixtures/manifest_v1.hex) | Internal codec complete |
| Segment container | 1 | Exact v1 only | Declares v1 in manifest/header | Segment and manifest fixtures | Filesystem integration pending |
| Segment header | 1 | Exact v1 only | Emits v1 | [`segment_header_v1.hex`](../../tests/fixtures/segment_header_v1.hex) | Internal codec complete |
| Commit slot | 1 | Exact v1, independently validated | Emits v1 | Both slots in the Segment fixture | Internal codec complete |
| Record | 1 | Exact canonical v1 only | Emits v1 | [`record_v1.hex`](../../tests/fixtures/record_v1.hex) | Internal codec complete |
| Native wire protocol | 2 | Exact v2 only | Emits v2 | Round-trip and malformed-frame tests | Experimental; golden fixture pending |

“Exact” means that unknown required versions, sizes, flags, and non-zero reserved bytes are rejected.
Record v1 additionally requires the minimal 8-byte-aligned extent and zero alignment padding, so
one logical Record cannot have multiple correctly checksummed encodings.

## Reader/writer matrix

| Artifact written as | Current reader result |
|---|---|
| Manifest v1 referencing Segment/Header/Commit/Record v1 | Accepted after full checksum and catalog validation |
| Segment header v1 with at least one valid commit slot v1 | Accepted; the greatest valid commit generation is selected |
| Segment header v1 with one torn or checksum-invalid slot | Accepted only through the other valid slot |
| Correctly checksummed unknown commit-slot version | Rejected as incompatible; no fallback to older committed data |
| Canonical Record v1 | Accepted after extent, checksum, enum, and zero-padding validation |
| Unknown manifest, Segment, header, Record, or wire version | Rejected before publication or service |
| Any pre-v1 persistent artifact | Unsupported; no legacy persistent format was released |
| Any future persistent version | Unsupported until a reader row, fixture, and migration decision are added |

The manifest pins all persistent component versions. Mixing a manifest v1 catalog with a Segment,
header, commit slot, or Record version not declared and supported by that manifest is an incompatible
Store, not an invitation to guess or partially recover.

## Upgrade and downgrade policy

There is currently no persistent upgrade, downgrade, or in-place migration path. Before the first
durable alpha release, every writer version must have:

1. a canonical fixture emitted independently of the production decoder;
2. compatibility tests using artifacts produced by every supported writer;
3. an explicit decision to read directly, migrate offline, or reject;
4. crash-safe migration ordering and rollback behavior where migration is supported;
5. release notes identifying the oldest readable and writable versions.

A future writer must not overwrite an older Store until compatibility has been decided and all
required source artifacts have been validated. An older binary encountering a correctly checksummed
newer required version fails closed. Read-only salvage, downgrade export, and destructive repair are
separate operator workflows and cannot be inferred from ordinary open.

## Evidence boundary

The three persistent fixtures now lock down every implemented v1 byte codec. They do not prove file
synchronization, atomic manifest publication, restart recovery, or compatibility across released
binaries. Those gates remain in the [durability and recovery contract](durability-recovery.md) and
[production-readiness checklist](../production-readiness.md).
