# ADR 0015: Whole-Worker sealed-history compaction

- Status: amended
- Date: 2026-07-19
- Owners: persistence maintainers
- Related: ADR 0003, ADR 0004, ADR 0008
- Amended by: ADR 0039

## Context

Compacting one sealed Segment in isolation can discard a tombstone while retaining the older value
in another Segment, resurrecting deleted or expired state. Durable replacement must also survive a
crash between output creation, manifest publication, and source retirement.

## Alternatives considered

- rewrite active Segment in place: rejected because it destroys append-only authority;
- compact one arbitrary Segment: rejected because cross-Segment history may be required;
- retain every tombstone forever: correct but prevents effective reclamation;
- stop-the-world whole-Store compaction: rejected because unrelated Workers can remain independent.

## Decision

One transaction selects the complete sealed history of one Worker and never rewrites its active
Segment. It copies only currently visible, non-expired puts, preserving sequence and Record metadata,
then uses a checksummed intent containing exact old and replacement manifests. Replacement files are
validated before manifest publication; old sources are retired only after the replacement namespace
is durable. `Store::compact()` runs at most one explicit transaction per call.

## Consequences

Tombstones and obsolete values can be dropped safely because every older sealed source for that
Worker retires together. Other Workers continue. Temporary space and physical write amplification
must be preflighted. ADR 0039 moves complete replacement copy, seal and verification before the
durable intent and global publication lease; the target Worker is briefly quiesced only for snapshot
and final publication validation, while sequence drift still aborts the prepared transaction.

## Compatibility and verification

Record, Segment, and Manifest remain format v1; compaction intent has its own v1 codec. Recovery must
resolve old or replacement authority deterministically after interruption. The complete algorithm is
specified in [Crash-safe durable compaction](../architecture/durable-compaction.md).
