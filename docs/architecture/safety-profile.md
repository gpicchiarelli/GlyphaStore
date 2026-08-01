# GlyphaStore C++ safety profile

GlyphaStore is performance-oriented C++, not unchecked C++.

## Non-negotiable rules

1. No raw owning pointers and no manual `new`/`delete` outside audited allocator internals.
2. No unchecked buffer, offset, capacity, or allocation arithmetic.
3. No persisted bytes decoded through object-layout casts.
4. No unbounded allocation derived from untrusted metadata.
5. No non-owning view stored beyond its guaranteed source lifetime.
6. No Segment reuse without generation validation.
7. No unmap, unlink, or reclaim while an active reader can hold a `RecordRef`.
8. No data race or undefined behavior accepted as harmless.
9. No parser or recovery reader without malformed-input tests and fuzz coverage.
10. No optimization bypassing validation without a benchmark, invariant proof, and review.

## Required verification

- Strict compiler warnings in CI.
- ASan+UBSan and TSan jobs.
- Fuzz targets for Record decoding, Segment scanning, and Index rebuild.
- Regression tests for every discovered memory-safety or corruption issue.
- Explicit errors for invalid extents, checksums, references, state transitions, and overflow.

Unsafe OS boundaries such as `mmap`, file descriptors, and platform topology calls must live behind
small RAII wrappers or platform modules. Memory corruption or allocator invariant failure is a
fail-closed process event; the runtime must not pretend that continuing is safe.
