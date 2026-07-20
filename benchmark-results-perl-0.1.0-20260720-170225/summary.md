# GlyphaStore Perl client benchmarks — version `0.1.0`

Parsed `20` result files from `benchmark-results-perl-0.1.0-20260720-170225`.

Workload: validated ordered `PUT`/`GET` pipeline read-after-write, value size 64 bytes,
volatile `glyphastored`, same-host loopback. Median ops/s is the comparison statistic.

| Execution | Workers | Pipeline pairs | Median ops/s | Min ops/s | Max ops/s | Median s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-process-worker-concurrent | 2 | 1 | 23,658 | 22,651 | 24,835 | 8.453837 |
| single-process-worker-concurrent | 2 | 128 | 81,799 | 80,078 | 82,829 | 2.445030 |
| single-process-worker-concurrent | 2 | 32 | 76,412 | 66,307 | 76,596 | 2.617375 |
| single-process-worker-concurrent | 2 | 8 | 62,334 | 58,261 | 62,637 | 3.208527 |
| single-process-worker-concurrent | 4 | 1 | 30,835 | 27,210 | 31,893 | 6.486121 |
| single-process-worker-concurrent | 4 | 128 | 80,903 | 76,261 | 83,448 | 2.472095 |
| single-process-worker-concurrent | 4 | 32 | 80,255 | 77,127 | 82,383 | 2.492047 |
| single-process-worker-concurrent | 4 | 8 | 65,783 | 64,828 | 68,990 | 3.040297 |
| single-process-worker-sequential | 1 | 1 | 26,385 | 23,851 | 27,247 | 7.580156 |
| single-process-worker-sequential | 1 | 128 | 85,839 | 78,380 | 86,560 | 2.329935 |
| single-process-worker-sequential | 1 | 32 | 78,960 | 77,735 | 80,421 | 2.532929 |
| single-process-worker-sequential | 1 | 8 | 60,741 | 58,795 | 62,060 | 3.292680 |
| single-process-worker-sequential | 2 | 1 | 24,235 | 21,264 | 25,595 | 8.252449 |
| single-process-worker-sequential | 2 | 128 | 47,293 | 29,005 | 78,687 | 4.228936 |
| single-process-worker-sequential | 2 | 32 | 78,234 | 71,016 | 80,446 | 2.556433 |
| single-process-worker-sequential | 2 | 8 | 59,465 | 56,483 | 60,659 | 3.363332 |
| single-process-worker-sequential | 4 | 1 | 26,220 | 20,076 | 27,985 | 7.627684 |
| single-process-worker-sequential | 4 | 128 | 85,124 | 78,857 | 86,841 | 2.349502 |
| single-process-worker-sequential | 4 | 32 | 79,835 | 75,190 | 81,613 | 2.505164 |
| single-process-worker-sequential | 4 | 8 | 61,876 | 60,506 | 63,565 | 3.232248 |

## Concurrent vs sequential (same Workers / pipeline)

| Workers | Pipeline | Sequential ops/s | Concurrent ops/s | Gain |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 1 | 24,235 | 23,658 | 0.98× |
| 2 | 8 | 59,465 | 62,334 | 1.05× |
| 2 | 32 | 78,234 | 76,412 | 0.98× |
| 2 | 128 | 47,293 | 81,799 | 1.73× |
| 4 | 1 | 26,220 | 30,835 | 1.18× |
| 4 | 8 | 61,876 | 65,783 | 1.06× |
| 4 | 32 | 79,835 | 80,255 | 1.01× |
| 4 | 128 | 85,124 | 80,903 | 0.95× |

## Notes

- Median ops/s counts **wire operations** (`OPS × 2`: PUT + GET). Older SDK 0.1.0 Perl rows used
  pair counts; see [analysis.md](analysis.md).
- `concurrent` uses `execute_worker_pipelines` (one select loop, overlap across Workers).
- `sequential` drains Workers one after another (fair compare to Python sequential).
- Workers=1 has no concurrent mode (single connection).
- Same-host loopback; do not treat as production capacity.
- Product scale still comes from one client per prefork process, not ithreads.
