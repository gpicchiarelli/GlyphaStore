# GlyphaStore Ruby client benchmarks — version `0.1.0`

Parsed `20` result files from `benchmark-results-ruby-0.1.0-20260720-175511`.

Workload: validated ordered `PUT`/`GET` pipeline read-after-write, value size 64 bytes,
volatile `glyphastored`, same-host loopback. Median ops/s is the comparison statistic.

| Execution | Workers | Pipeline pairs | Median ops/s | Min ops/s | Max ops/s | Median s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single-process-worker-concurrent | 2 | 1 | 13,206 | 11,084 | 13,806 | 15.144089 |
| single-process-worker-concurrent | 2 | 128 | 75,905 | 70,935 | 78,066 | 2.634856 |
| single-process-worker-concurrent | 2 | 32 | 66,655 | 30,320 | 72,896 | 3.000509 |
| single-process-worker-concurrent | 2 | 8 | 55,727 | 32,803 | 60,707 | 3.588941 |
| single-process-worker-concurrent | 4 | 1 | 14,914 | 14,576 | 15,671 | 13.409811 |
| single-process-worker-concurrent | 4 | 128 | 80,485 | 75,189 | 80,705 | 2.484949 |
| single-process-worker-concurrent | 4 | 32 | 75,593 | 70,287 | 75,842 | 2.645741 |
| single-process-worker-concurrent | 4 | 8 | 58,420 | 55,845 | 60,293 | 3.423509 |
| single-process-worker-sequential | 1 | 1 | 19,668 | 16,622 | 27,002 | 10.168581 |
| single-process-worker-sequential | 1 | 128 | 67,751 | 65,260 | 69,197 | 2.951979 |
| single-process-worker-sequential | 1 | 32 | 64,975 | 47,078 | 67,589 | 3.078110 |
| single-process-worker-sequential | 1 | 8 | 56,452 | 51,677 | 59,378 | 3.542825 |
| single-process-worker-sequential | 2 | 1 | 26,310 | 21,077 | 28,007 | 7.601550 |
| single-process-worker-sequential | 2 | 128 | 70,886 | 67,041 | 74,428 | 2.821448 |
| single-process-worker-sequential | 2 | 32 | 66,339 | 64,392 | 67,656 | 3.014806 |
| single-process-worker-sequential | 2 | 8 | 54,258 | 52,444 | 55,873 | 3.686109 |
| single-process-worker-sequential | 4 | 1 | 30,431 | 28,855 | 31,692 | 6.572238 |
| single-process-worker-sequential | 4 | 128 | 76,435 | 67,864 | 76,725 | 2.616596 |
| single-process-worker-sequential | 4 | 32 | 66,842 | 65,116 | 67,887 | 2.992126 |
| single-process-worker-sequential | 4 | 8 | 51,821 | 50,036 | 52,502 | 3.859440 |

## Concurrent vs sequential (same Workers / pipeline)

| Workers | Pipeline | Sequential ops/s | Concurrent ops/s | Gain |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 1 | 26,310 | 13,206 | 0.50× |
| 2 | 8 | 54,258 | 55,727 | 1.03× |
| 2 | 32 | 66,339 | 66,655 | 1.00× |
| 2 | 128 | 70,886 | 75,905 | 1.07× |
| 4 | 1 | 30,431 | 14,914 | 0.49× |
| 4 | 8 | 51,821 | 58,420 | 1.13× |
| 4 | 32 | 66,842 | 75,593 | 1.13× |
| 4 | 128 | 76,435 | 80,485 | 1.05× |

## Notes

- `concurrent` uses `threaded per-Worker pipelines` (one select loop, overlap across Workers).
- `sequential` drains Workers one after another (fair compare to Python sequential).
- Workers=1 has no concurrent mode (single connection).
- Same-host loopback; do not treat as production capacity.
- Product scale still comes from one client per prefork process, not MRI threads for CPU scale.
