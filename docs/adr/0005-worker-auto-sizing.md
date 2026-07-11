# ADR 0005: Automatic Worker sizing

- Status: accepted
- Date: 2026-07-11

GlyphaStore detects usable physical CPU and memory topology at startup, applies reserved-core,
maximum-Worker, memory-per-Worker, and explicit-override policies, and fixes the Worker count for
the process lifetime. The Store remains one logical key-space; physical Index partitions and
Segment assignment are internal.
