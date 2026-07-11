# Storage model

## Logical structure

```text
Store
  Workers
    Index partition
    active Segment
    owned Segment references
    monotonic sequence counter

Global Segment Manager
  Segment catalog
  free/retired Segment pools
  persistence and residency state
```

The Store presents one key-space. Routing chooses a Worker deterministically from the key hash.
The Worker owns mutation of its Index partition and appends immutable Records to an assigned active
Segment.

## Read and write paths

```text
read:  key -> Worker -> Index -> RecordRef -> Segment + validated offset -> Record
write: key -> Worker -> encode Record -> append -> publish Index reference
```

Publishing the Index reference makes a new Record visible. Replacing or removing an Index entry
updates Segment liveness accounting; the old Record remains physically present until its Segment
is reclaimed or vacuumed.

## Recovery

The Index can be reconstructed by scanning valid Records. Recovery compares full keys, never only
hashes. A higher sequence supersedes a lower one regardless of Segment scan order. A newest
tombstone removes visibility; a newest expired Record is omitted from the rebuilt Index.

An optional persisted Index checkpoint may accelerate startup later, but corruption of that cache
must always permit fallback to Segment scanning.

## Complexity

- Index lookup, insert, replace, and erase: expected O(1), plus O(key length) hashing/comparison.
- Positional Record access: O(1).
- Segment append and rotation: O(1), excluding Record bytes copied.
- Full Index rebuild: O(total Records scanned).
- Full vacuum rebuild: O(live Index entries and live bytes copied).
- Whole dead-Segment reclaim: O(1) logical retirement plus OS/storage release costs.
