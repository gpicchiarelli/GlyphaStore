# Durable recovery implementation

This document describes the implemented internal recovery orchestrator. It converts one locked,
validated data directory into a deterministic manifest-aligned catalog, one rebuilt Index per
Worker, restored next sequences, and explicit rotation requirements. The public durable Store moves
this state into the [durable runtime catalog](durable-runtime-catalog.md) after completing only
validated bootstrap or rotation intents.

## Recovery pipeline

`recover_durable_state` performs no repair and publishes no files:

```text
read and decode authoritative manifest
  -> enumerate the locked directory and apply the read-only namespace policy
  -> group canonical catalog indexes by owner Worker
  -> for each Worker
       -> open one listed Segment
       -> verify complete encoded identity and selected commit
       -> validate manifest/commit lifecycle
       -> scan exactly the committed extent
       -> validate key hash and routing ownership
       -> retain the highest sequence decision for each full binary key
       -> close the Segment
       -> build that Worker's Index and restore maximum_sequence + 1
  -> return state only after every Worker and Segment succeeds
```

Manifest decoding has already established supported versions, one active Segment per Worker,
strict Segment-ID ordering, and bounded catalog sizes. A missing listed file is promoted from
`not_found` to corruption because the manifest is recovery authority. Header Store ID, Segment ID,
generation, and owner must all match the catalog; filenames alone are never trusted.

The namespace audit reports exact private crash temporaries without deleting them. Unlisted final
Segments, malformed engine names, unknown entries, unsafe filesystem objects, and missing catalog
files reject normal recovery before any Segment scan. The complete classification, descriptor-relative
algorithm, limits, and remaining quarantine requirements are specified in the
[data-directory namespace audit](namespace-policy.md).

## Bounded scan and resource strategy

The Segment layer exposes an internal visitor over one contiguous read of
`[4096, committed_end)`. Each Record is structurally decoded and checksum-verified once, then its
short-lived view is consumed directly by recovery. This avoids the former potential pattern of
scanning references and issuing one additional positional read per Record.

Catalog grouping uses a prefix-sum offset table plus one contiguous array of manifest indexes. It is
`O(segment count + Worker count)`, has no per-Segment allocation, and preserves canonical catalog
indexes in the output. Recovery intentionally processes one Worker and one Segment descriptor at a
time:

- file-descriptor usage is constant rather than proportional to catalog size;
- the temporary latest-key map exists for only one Worker;
- the scan buffer is bounded by the fixed 64 MiB Segment size;
- final per-Worker Indexes accumulate because they are the required recovery result.

Expected rebuild time is `O(committed bytes + Records + visible-key hashing)`. Peak temporary memory
is `O(one committed Segment + distinct keys for the largest Worker partition + catalog grouping)`;
the returned Index and selected-commit catalog are additional required state. Recovery is currently
serial. Parallel Worker scans may be added only with bounded I/O concurrency and measured benefit on
the target filesystem; unrestricted parallel scanning would amplify memory, descriptors, and random
I/O.

## Record and visibility invariants

For every committed Record, recovery requires:

- canonical Record extent and valid CRC32C;
- non-zero, strictly increasing sequence within its Segment;
- a first sequence strictly greater than the preceding non-empty manifest-ordered Segment owned by
  the same Worker;
- persisted key hash equal to FNV-1a over the complete binary key;
- `hash % persisted_worker_count` equal to the Segment owner;
- reference Segment ID/generation equal to the opened file.

Filesystem enumeration never determines recovery order. Within canonical Manifest order, non-empty
per-Worker Segment sequence ranges must be globally strict and non-overlapping. The highest sequence
for each exact key then wins. Equal or reversed cross-Segment ranges are corruption; equal winning
sequences for one key are a conflict. A newest tombstone or newest expired Record suppresses
older values; it is never skipped early in a way that could resurrect history. Sequence restoration
uses the maximum committed sequence across all of a Worker's Records, including tombstones and
expired decisions. `UINT64_MAX` fails read-write recovery instead of wrapping to zero.

The Index receives the already verified key hash, avoiding a redundant key hash during insertion.
Hash equality never substitutes for full-key equality.

## Lifecycle matrix

| Manifest role | Selected persisted state | Recovery result |
|---|---|---|
| sealed | sealed | accepted |
| sealed | active | corruption |
| active | active | accepted as writable candidate |
| active | sealed | accepted only as the documented interrupted-rotation transition; Worker is marked `active_requires_rotation` |

The recovery scan does not append to a sealed active Segment. The durable runtime treats it as an
intent marker and completes only the exact next-identity pristine replacement transition before
serving writes.

## Returned state and remaining gates

The returned Segment-state vector is index-for-index aligned with `manifest.segments`; catalog
identity is not duplicated. Every Worker state contains its rebuilt Index, next sequence, manifest
active Segment, and rotation flag. Segment descriptors are closed after scanning. The owning Store
must retain the locked `DataDirectory` and reopen backing files through verified identities when the
runtime durable catalog is integrated.

`DurableRuntimeCatalog` consumes this state without copying Index keys, retains the directory lock,
serves CRC-verified owning reads through at most one cached Segment descriptor per Worker, and
performs preflighted durable Record/slot/Index mutations. It completes a sealed-active marker only by
creating or validating the exact pristine next Segment and durably publishing the next manifest.

Integration tests cover multi-Worker rebuild, cross-Segment newest-value selection, tombstones,
expiration, sequence restoration, lifecycle transitions, missing files, Store-ID mismatch, stored
hash mismatch, wrong-Worker routing, equal winning sequences, sequence exhaustion, crash temporaries,
and unlisted-Segment rejection without adoption.

Still required before enabling `durable_sync`:

- add explicit operator quarantine/repair for arbitrary orphans;
- integrate the durable runtime with Store/daemon construction and network acknowledgement;
- run process-kill, disk-full, native-platform, and power-loss matrices.
