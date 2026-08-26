# Storage model

Status: descriptive of the current paired Store
Applies to: volatile memory and persistence v1 backends
Owner: storage-engine maintainers
Last reviewed: 2026-08-26

## Logical structure

```text
Store
  ShardPairRuntime (default concurrency)
    owner -> immutable ReadGeneration + one mutation executor
    volatile backend -> mutable Index + in-memory Segments
    or durable backend -> mutable Index + active file Segment + generation pins
  control/maintenance
    GlobalSegmentManager or DurableRuntimeCatalog
```

The Store presents one key-space. Routing chooses a Worker deterministically from the key hash.
The Worker owns mutation of its Index partition and appends immutable Records to an assigned active
Segment.

The volatile `Worker` and durable runtime Worker are separate backends under the same paired
ownership rule. The global manager/catalog is control plane; ordinary paired lookup remains in the
owner Reader's immutable generation and never scans other Workers. Mutable backend Index state is
Writer-only during normal paired operation. The normative component and dependency map is in the
[architecture specification](../spec/architecture.md).

## Read and write paths

```text
read:  key -> owner -> immutable ReadGeneration -> RecordRef/pin -> Segment -> value
write: key -> owner mutation executor -> encode -> append -> publish backend Index + ReadGeneration
```

Publishing a coherent `ReadGeneration` makes a new Record visible to paired readers. Replacing or
removing an Index entry updates Segment liveness accounting; the old Record remains physically
present until its Segment is reclaimed or compacted.

In durable-sync mode, successful synchronization of the alternate Segment commit slot is
the durable commit point and precedes in-memory publication. The full ordering and acknowledgement
rules are specified in the [durability and recovery contract](durability-recovery.md).

## Recovery

The Index can be reconstructed by scanning valid Records. Recovery compares full keys, never only
hashes. A higher sequence supersedes a lower one regardless of Segment scan order. A newest
tombstone removes visibility; a newest expired Record is omitted from the rebuilt Index.

No persisted Index checkpoint is part of persistence v1. Any future derived checkpoint must remain
discardable and permit fallback to validated Segment scanning.

Durable recovery uses the persisted routing algorithm, Worker count, and routing epoch. Machine
topology does not silently repartition an existing Store during reopen.

## Complexity

- Index lookup, insert, replace, and erase: expected O(1), plus O(key length) hashing/comparison.
- Positional Record access: O(1).
- Segment append and rotation: O(1), excluding Record bytes copied.
- Full Index rebuild: O(total Records scanned).
- Full vacuum rebuild: O(live Index entries and live bytes copied).
- Whole dead-Segment reclaim: O(1) logical retirement plus OS/storage release costs.
