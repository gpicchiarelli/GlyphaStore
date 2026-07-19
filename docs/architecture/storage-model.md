# Storage model

## Logical structure

```text
Store
  volatile runtime
    Worker -> Index + in-memory Segments + mutex
    GlobalSegmentManager -> allocation/lifecycle snapshots
  or durable runtime
    runtime Worker -> Index + active file Segment + mutex + hot cache
    DurableRuntimeCatalog -> manifest-aligned file namespace
```

The Store presents one key-space. Routing chooses a Worker deterministically from the key hash.
The Worker owns mutation of its Index partition and appends immutable Records to an assigned active
Segment.

The volatile `Worker` and durable runtime Worker are separate implementations of the same ownership
rule. The global manager/catalog is control plane; normal exact-key lookup remains in the routed
Worker data plane. The normative component and dependency map is in the
[architecture specification](../spec/architecture.md).

## Read and write paths

```text
read:  key -> Worker -> Index -> RecordRef -> Segment + validated offset -> Record
write: key -> Worker -> encode Record -> append -> publish Index reference
```

Publishing the Index reference makes a new Record visible. Replacing or removing an Index entry
updates Segment liveness accounting; the old Record remains physically present until its Segment
is reclaimed or vacuumed.

In durable-sync mode, successful synchronization of the alternate Segment commit slot is
the durable commit point and precedes in-memory publication. The full ordering and acknowledgement
rules are specified in the [durability and recovery contract](durability-recovery.md).

## Recovery

The Index can be reconstructed by scanning valid Records. Recovery compares full keys, never only
hashes. A higher sequence supersedes a lower one regardless of Segment scan order. A newest
tombstone removes visibility; a newest expired Record is omitted from the rebuilt Index.

An optional persisted Index checkpoint may accelerate startup later, but corruption of that cache
must always permit fallback to Segment scanning.

Durable recovery uses the persisted routing algorithm, Worker count, and routing epoch. Machine
topology does not silently repartition an existing Store during reopen.

## Complexity

- Index lookup, insert, replace, and erase: expected O(1), plus O(key length) hashing/comparison.
- Positional Record access: O(1).
- Segment append and rotation: O(1), excluding Record bytes copied.
- Full Index rebuild: O(total Records scanned).
- Full vacuum rebuild: O(live Index entries and live bytes copied).
- Whole dead-Segment reclaim: O(1) logical retirement plus OS/storage release costs.
