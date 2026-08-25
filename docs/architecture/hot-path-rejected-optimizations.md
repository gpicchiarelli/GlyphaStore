Status: lab notes
Applies to: 2026-08-01 hot-path performance program
Owner: performance maintainers
Last reviewed: 2026-08-25

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
| TLS `PairReadGeneration` shell freelist + custom `shared_ptr` deleter (ADR 0035) | Same-machine A/B: mild single-thread PUT gain, **−16% worker-affine PUT 2t**; rejected |
| Writer post-sync busy-spin (64 yields) before merge/park | Collapsed `store_put` ~378 k → ~170 k; dual-sided spin tax |
| Writer sync-before-merge reorder alone | No reliable 1t PUT win vs merge-first; affine mixed; keep merge-first |
| Conditional `notify_one` gated on `writer_waiting` | Same-machine A/B vs unconditional wake: `store_put` 359 k vs 349 k (noise), affine 502 k vs 509 k; no reliable win. Sync PUT parks the Writer every op so notify almost always fires anyway. Evidence: `benchmarks/results/local-macos-2026-08-02-writer-waiting/` |
| Coalesce buffered GET responses behind a decided mutation ACK | Halved measured socket-write calls, but required decoding the following non-GET twice and produced no stable throughput gain; local pipeline-128 Release runs ranged from neutral to a substantial regression. The current flush-before-resume schedule remains authoritative. |

Accepted residuals remain documented in the main performance report (PUT ack cost,
uniform embedded PUT vs owner-bound daemon model).
