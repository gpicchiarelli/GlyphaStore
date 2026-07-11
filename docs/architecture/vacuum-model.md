# Vacuum model

Vacuum is copy-build-validate-publish maintenance over Index-reachable Records.

```text
current {Index, Segment set}
       -> copy visible Records into new fixed Segments
       -> build a new Index
       -> validate referential and record integrity
       -> atomically publish the new pair
       -> retire old Segments
       -> reclaim after readers and retention obligations are gone
```

The prototype implements a single-threaded offline builder proving that visible state is
preserved and source Segments remain immutable. Concurrent publication requires an explicit epoch
or equivalent reclamation design before it may enter the runtime.

Whole-Segment reclaim is cheaper than compaction. A sealed Segment with zero live Records and no
retention or reader obligation may be retired without scanning its Records. Partially live
Segments become vacuum candidates based on live-byte ratio and reclaim benefit.

No vacuum operation may resurrect a lower-sequence Record, mutate a published Record, delete a
Segment still addressable by a reader, or perform unbounded work in a latency-critical loop.
