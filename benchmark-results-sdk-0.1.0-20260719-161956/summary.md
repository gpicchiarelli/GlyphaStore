# GlyphaStore SDK client benchmarks — version `0.1.0`

Parsed `48` result files from `benchmark-results-sdk-0.1.0-20260719-161956`.

Workload: validated ordered `PUT`/`GET` pipeline read-after-write, value size 64 bytes,
volatile `glyphastored`, same-host loopback. Median ops/s is the comparison statistic.

| SDK | Runtime | Execution | Workers | Pipeline pairs | Median ops/s | Min ops/s | Max ops/s | Median s |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Python | async | concurrent | 1 | 1 | 26,179 | 24,884 | 26,457 | 3.819790 |
| Python | async | concurrent | 1 | 128 | 98,083 | 97,594 | 99,348 | 1.019549 |
| Python | async | concurrent | 1 | 32 | 86,061 | 67,191 | 87,854 | 1.161961 |
| Python | async | concurrent | 1 | 8 | 64,124 | 62,821 | 67,249 | 1.559488 |
| Python | async | concurrent | 2 | 1 | 34,545 | 33,646 | 35,106 | 2.894800 |
| Python | async | concurrent | 2 | 128 | 104,573 | 101,690 | 105,156 | 0.956271 |
| Python | async | concurrent | 2 | 32 | 97,560 | 92,544 | 99,368 | 1.025011 |
| Python | async | concurrent | 2 | 8 | 78,866 | 78,231 | 79,484 | 1.267968 |
| Python | async | concurrent | 4 | 1 | 43,979 | 43,388 | 45,114 | 2.273827 |
| Python | async | concurrent | 4 | 128 | 100,075 | 70,435 | 104,715 | 0.999255 |
| Python | async | concurrent | 4 | 32 | 99,626 | 95,041 | 100,069 | 1.003756 |
| Python | async | concurrent | 4 | 8 | 87,533 | 86,414 | 88,901 | 1.142430 |
| Python | sync | concurrent | 1 | 1 | 37,149 | 32,153 | 39,294 | 2.691833 |
| Python | sync | concurrent | 1 | 128 | 107,845 | 97,055 | 108,718 | 0.927257 |
| Python | sync | concurrent | 1 | 32 | 97,992 | 97,282 | 98,694 | 1.020488 |
| Python | sync | concurrent | 1 | 8 | 87,721 | 84,759 | 88,962 | 1.139972 |
| Python | sync | concurrent | 2 | 1 | 49,215 | 48,750 | 49,895 | 2.031882 |
| Python | sync | concurrent | 2 | 128 | 114,046 | 112,965 | 114,788 | 0.876839 |
| Python | sync | concurrent | 2 | 32 | 108,766 | 107,576 | 110,020 | 0.919407 |
| Python | sync | concurrent | 2 | 8 | 94,594 | 91,111 | 95,902 | 1.057144 |
| Python | sync | concurrent | 4 | 1 | 42,323 | 41,953 | 42,376 | 2.362800 |
| Python | sync | concurrent | 4 | 128 | 113,004 | 111,601 | 113,628 | 0.884925 |
| Python | sync | concurrent | 4 | 32 | 107,599 | 106,634 | 108,420 | 0.929377 |
| Python | sync | concurrent | 4 | 8 | 92,018 | 86,965 | 92,843 | 1.086742 |
| Python | sync | sequential | 1 | 1 | 38,599 | 37,734 | 38,823 | 2.590728 |
| Python | sync | sequential | 1 | 128 | 107,162 | 106,318 | 108,858 | 0.933164 |
| Python | sync | sequential | 1 | 32 | 95,456 | 89,258 | 99,196 | 1.047608 |
| Python | sync | sequential | 1 | 8 | 85,734 | 83,240 | 86,638 | 1.166396 |
| Python | sync | sequential | 2 | 1 | 39,670 | 38,960 | 39,900 | 2.520792 |
| Python | sync | sequential | 2 | 128 | 106,997 | 102,054 | 108,314 | 0.934607 |
| Python | sync | sequential | 2 | 32 | 94,819 | 94,104 | 95,335 | 1.054638 |
| Python | sync | sequential | 2 | 8 | 85,043 | 74,395 | 88,451 | 1.175875 |
| Python | sync | sequential | 4 | 1 | 39,281 | 38,193 | 39,414 | 2.545789 |
| Python | sync | sequential | 4 | 128 | 98,567 | 54,406 | 100,342 | 1.014539 |
| Python | sync | sequential | 4 | 32 | 94,950 | 94,000 | 96,540 | 1.053185 |
| Python | sync | sequential | 4 | 8 | 86,258 | 83,847 | 88,024 | 1.159308 |
| Perl | sync | single-process-worker-sequential | 1 | 1 | 20,640 | 17,386 | 20,842 | 4.844883 |
| Perl | sync | single-process-worker-sequential | 1 | 128 | 44,695 | 43,526 | 45,335 | 2.237389 |
| Perl | sync | single-process-worker-sequential | 1 | 32 | 43,710 | 43,399 | 43,779 | 2.287820 |
| Perl | sync | single-process-worker-sequential | 1 | 8 | 36,300 | 35,034 | 37,680 | 2.754850 |
| Perl | sync | single-process-worker-sequential | 2 | 1 | 21,127 | 20,417 | 21,319 | 4.733303 |
| Perl | sync | single-process-worker-sequential | 2 | 128 | 45,280 | 43,932 | 45,583 | 2.208503 |
| Perl | sync | single-process-worker-sequential | 2 | 32 | 43,043 | 42,932 | 43,183 | 2.323280 |
| Perl | sync | single-process-worker-sequential | 2 | 8 | 35,909 | 35,460 | 36,207 | 2.784779 |
| Perl | sync | single-process-worker-sequential | 4 | 1 | 20,880 | 20,489 | 20,916 | 4.789317 |
| Perl | sync | single-process-worker-sequential | 4 | 128 | 43,318 | 41,394 | 45,092 | 2.308522 |
| Perl | sync | single-process-worker-sequential | 4 | 32 | 41,789 | 41,147 | 42,444 | 2.392965 |
| Perl | sync | single-process-worker-sequential | 4 | 8 | 35,706 | 35,260 | 38,389 | 2.800612 |

## Notes

- Python `concurrent` uses one OS thread per Worker against one shared `Client`.
- Python `async` uses one `asyncio` task per Worker against one shared `AsyncClient`.
- Python `sequential` and Perl drain Workers one after another (fair cross-language compare).
- Perl has no shared-client multi-threaded mode; ithreads are not used.
- Do not treat same-host loopback numbers as production capacity.
