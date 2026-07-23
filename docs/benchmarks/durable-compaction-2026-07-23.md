# Durable compaction exploratory benchmark — 2026-07-23

Status: exploratory measurement, not a release baseline
Applies to: public `Store::compact()` on persistence v1
Owner: performance and persistence maintainers
Last reviewed: 2026-07-23

## Question

How does whole-Worker durable compaction trade copied bytes and elapsed time for reclaimed
Segments, and where does the current automatic scheduling policy still need work?

This is an internal benefit/cost benchmark, not a competitive database comparison. Every measured
sample reopened the Store, ran `verify_index()`, and checked every expected key after the timed
compaction. Seed, close, reopen, and verification time are outside the compaction interval.

## Environment and method

| Item | Value |
| --- | --- |
| Source | `e6571e3c96c55ab5b0ae42f4a1975c69a38b783e`, dirty tree |
| Build | CMake `Release`, Apple clang 21.0.0, C++23, `-O3 -DNDEBUG`, ARM64, LTO off |
| Host | Apple M4, 10 physical / 10 logical cores, 16 GiB RAM |
| OS | macOS 26.5.2 (25F84), Darwin 25.5.0 |
| Storage | internal 494.4 GB SSD, APFS, volume 80% used at capture |
| Runtime state | AC power; 58% system memory free after capture; no CPU affinity |
| Thermal / power policy | not controlled or instrumented |
| Store | `durable-periodic`, one Worker, maintenance disabled, explicit flush before measurement |
| Workload | fresh Store per sample; 256 KiB values; sequential round-robin key updates |
| Sampling | seven measured repeats, no warmup; scenario order rotated between repeats |
| Command | `./scripts/dev.sh benchmark-compaction --warmup 0 --repeats 7` |
| Raw result | [`data/durable-compaction-2026-07-23.csv`](data/durable-compaction-2026-07-23.csv) |

The dirty source state, uncontrolled thermal/storage state, and absence of a warmup prevent these
numbers from becoming controlled baseline evidence under the benchmark standard. They are useful
for policy direction and for validating the new harness.

## Results

Medians are the comparison statistic. The range is shown because the copy-heavy paths slowed
materially in the second half of the run.

| Scenario | Segments | Reclaimed | Copied | Copied / reclaimed | Compact median (min–max) | Effective copy median | Reopen median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| High reclaim, 128 live keys | 5 → 2 | 192 MiB (60%) | 31.01 MiB | 0.16× | 258.1 ms (162.0–270.3) | 120.1 MiB/s | 15.5 ms |
| Medium reclaim, 512 live keys | 5 → 3 | 128 MiB (40%) | 127.04 MiB | 0.99× | 765.2 ms (492.0–859.0) | 166.0 MiB/s | 50.8 ms |
| Low reclaim, 768 live keys | 5 → 4 | 64 MiB (20%) | 191.06 MiB | 2.99× | 1,144.4 ms (726.4–1,312.0) | 167.0 MiB/s | 74.6 ms |
| Copy-heavy, 1,024 live keys | 7 → 5 | 128 MiB (28.6%) | 254.58 MiB | 1.99× | 1,222.4 ms (985.2–1,595.3) | 208.3 MiB/s | 99.7 ms |
| TTL 50%, 510 expired records | 5 → 3 | 128 MiB (40%) | 127.54 MiB | 1.00× | 799.0 ms (538.8–905.7) | 159.6 MiB/s | 48.5 ms |
| No physical gain | 5 → 5 | 0 MiB | 0 MiB reported | — | 148.8 ms (139.0–153.9) | not reported | 94.5 ms |

The TTL case verified 255.08 MiB of live Index-referenced source data, copied 127.54 MiB, and
dropped all 510 expired records. Its median effective verification rate was 319.3 MiB/s.

## What the measurement says

1. **The compaction transaction is useful and validates cleanly.** Every beneficial case reclaimed
   the expected complete Segments, reopened successfully, and preserved the model. The TTL case
   physically removed every expected expired record.
2. **Benefit is strongly nonlinear.** High reclaim copies only 0.16 MiB for every MiB recovered.
   Low reclaim copies 2.99 MiB for every MiB recovered and takes over four times the high-reclaim
   median. The existing planner's 4× physical write-amplification ceiling admits both, but they are
   not equally attractive normal-maintenance work.
3. **Reopen improves with a smaller catalog.** For equal 320 MiB starting Stores, median reopen
   fell from 94.5 ms in the no-gain layout to 15.5 ms after high reclaim, 50.8 ms after medium
   reclaim, and 74.6 ms after low reclaim. These are within-run measurements, not isolated recovery
   benchmarks.
4. **No-gain detection is not free.** Rejecting a 320 MiB / five-Segment layout still costs a
   148.8 ms median. The public no-gain result reports zero source/copy counters, so operators cannot
   distinguish a cheap scheduler skip from an expensive rejected plan using those counters alone.
5. **The copy path has material environmental variance.** Beneficial scenarios show 1.6×–1.8×
   min/max spreads, with a common slowdown later in the run; no-gain spread is only 1.1×. This is
   consistent with system or storage state affecting the I/O-heavy phase, but the benchmark does
   not identify the cause. Controlled cooldown, temperature/power instrumentation, and a clean
   build are required before using absolute throughput as a regression threshold.

## Policy audit and implemented follow-up

The benchmark originally found that the controller could not apply its configured normal-mode
dead-byte threshold. The follow-up now:

- maintains exact active and sealed Index-referenced Record-byte counters at recovery, mutation,
  lazy expiry, rotation, and compaction publication;
- reports sealed, live, dead, and basis-point ratio counters for the exact next round-robin Worker;
- skips that Worker below the inclusive `dead_byte_ratio_bp_normal` threshold in normal mode and
  passes the same selected Worker to the automatic compact call;
- continues to bypass the threshold, no-gain streak, and copy budget under pressure/emergency;
- exports the candidate counters through `MaintenanceSnapshot` and daemon `STATS`.

This prevents the measured overwrite-driven low-reclaim class from being selected by the default
50% normal threshold. Index-referenced TTL Records remain conservatively live until a validated
GET, recovery, or pressure-triggered compaction discovers expiration.

The result also drives a finite 128 MiB default for `max_copy_bytes_per_cycle`: the measured
normal-threshold-eligible high/medium classes copy about 31/127 MiB, while a normal candidate whose
exact conservative live-byte estimate exceeds the limit is rejected before the transaction.
Equality is accepted, zero explicitly means unlimited, and pressure/emergency bypass the limit.

One directly measured policy gap remains: no-gain planning work still lacks detailed public
counters.

## Decision and next benchmark gates

Before treating automatic reclaim as production-tuned:

1. expose no-gain planning work in `CompactionResult` and maintenance telemetry;
2. decide whether unread TTL needs a bounded normal-mode probe independent of pressure;
3. add a concurrent benchmark comparing maintenance disabled/cooperative/background under mixed
   GET/PUT load, reporting throughput plus p50/p95/p99/max foreground latency;
4. add long sealed-churn and idle-overhead workloads, then run a clean seven-repeat matrix on
   controlled macOS/APFS and Linux ext4/XFS hosts with raw artifacts.

Until those gates pass, durable compaction correctness remains ahead of reclaim-policy tuning. The
normal overwrite-driven threshold gap is closed, but the result does not close the production
benchmark or native power-loss requirements.
