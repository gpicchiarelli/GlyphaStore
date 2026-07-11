# Residency model

The Index and each Worker active Segment remain resident in RAM. A sealed Segment may transition
between resident, mapped, and evicted states only after a durable or otherwise valid secondary
copy exists.

```text
ACTIVE: resident and writable by its owning Worker
SEALED/HOT: resident or prefaulted mapping
SEALED/WARM: file-backed mapping, pages managed by the OS
SEALED/COLD: evicted or unmapped, load required on access
DEAD: retired and reclaimable after safety conditions
```

`/tmp` is never an architectural default because it may be a RAM-backed or automatically cleaned
filesystem. Data directories are configurable. Production defaults will follow each OS convention
without hard-coding paths in the core.

Residency policy uses high/low watermarks to avoid oscillation, never evicts active or pinned
Segments, and must expose page-fault and load latency to SLA metrics. `mmap` offers positional
access but does not guarantee physical residency or durability.
