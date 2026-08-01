Status: lab notes
Applies to: 2026-08-01 hot-path performance program
Owner: performance maintainers
Last reviewed: 2026-08-01

# Rejected hot-path optimizations

Companion to [hot-path-performance-2026-08-01.md](hot-path-performance-2026-08-01.md).

| Optimization | Why rejected |
| --- | --- |
| Widen SPSC / TCP / disk-read queues | Breaks bounded memory and backpressure contracts |
| Remove key/value length or overflow checks | Hard safety constraint |
| ACK before Writer publication | Breaks RAW / client-visible happens-before |
| Drop ReadLease seq_cst fence without epoch lease protocol | Generation UAF risk under reclaim |
| Silent dual-runtime or hash/routing changes | Silent-change ban; needs ADR |
| Disable OperationGuard on GET | Breaks close admission linearization |
| shared_ptr-free generation pool copied from experimental prototype without ADR | Format/runtime authority change; needs requirements + proofs |
| Benchmark threshold widening / synthetic-only wins | Forbidden gaming |
| Global mutex around phase counters | Would distort the paths being measured |
| Thread-local DeltaPage/Block/Chunk freelist with custom `shared_ptr` deleter | Measured regression on uniform parallel PUT (Apple Silicon lab); allocator/custom-deleter tax exceeded reuse benefit |
| Expecting sync Writer coalesce alone to lift blocking `Store::put` | Blocking put presents N≈1; coalesce needs `put_batch` (or equivalent) to stage multi-mutation drains |

Accepted residuals remain documented in the main performance report (PUT ack cost,
uniform embedded PUT vs owner-bound daemon model).
