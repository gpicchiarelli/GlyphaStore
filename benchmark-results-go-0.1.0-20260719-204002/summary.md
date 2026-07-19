# GlyphaStore Go client benchmarks — version `0.1.0`

Parsed `24` result files from `benchmark-results-go-0.1.0-20260719-204002`.

Workload: validated ordered `PUT`/`GET` pipeline read-after-write, value size 64 bytes,
volatile `glyphastored`, same-host loopback. Median ops/s is the comparison statistic.

| Execution | Workers | Pipeline pairs | Median ops/s | Min ops/s | Max ops/s | Median s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| concurrent | 1 | 1 | 74,407 | 46,210 | 80,944 | 2.687916 |
| concurrent | 1 | 128 | 408,412 | 293,502 | 600,828 | 0.489702 |
| concurrent | 1 | 32 | 642,422 | 479,241 | 820,812 | 0.311322 |
| concurrent | 1 | 8 | 389,684 | 371,937 | 428,979 | 0.513236 |
| concurrent | 2 | 1 | 115,389 | 108,961 | 135,217 | 1.733273 |
| concurrent | 2 | 128 | 372,669 | 333,581 | 472,346 | 0.536670 |
| concurrent | 2 | 32 | 1,284,878 | 598,603 | 1,340,756 | 0.155657 |
| concurrent | 2 | 8 | 603,540 | 461,974 | 728,685 | 0.331378 |
| concurrent | 4 | 1 | 149,614 | 110,920 | 163,885 | 1.336777 |
| concurrent | 4 | 128 | 568,764 | 426,483 | 630,164 | 0.351640 |
| concurrent | 4 | 32 | 1,251,554 | 963,781 | 1,546,915 | 0.159801 |
| concurrent | 4 | 8 | 862,328 | 618,072 | 1,048,659 | 0.231930 |
| sequential | 1 | 1 | 65,928 | 52,494 | 79,072 | 3.033604 |
| sequential | 1 | 128 | 329,159 | 311,925 | 510,347 | 0.607609 |
| sequential | 1 | 32 | 769,062 | 426,026 | 869,224 | 0.260057 |
| sequential | 1 | 8 | 347,475 | 269,807 | 391,876 | 0.575580 |
| sequential | 2 | 1 | 77,210 | 71,809 | 79,064 | 2.590349 |
| sequential | 2 | 128 | 290,727 | 219,837 | 329,909 | 0.687931 |
| sequential | 2 | 32 | 466,904 | 386,674 | 748,892 | 0.428353 |
| sequential | 2 | 8 | 382,588 | 303,368 | 396,884 | 0.522756 |
| sequential | 4 | 1 | 78,642 | 70,588 | 78,922 | 2.543165 |
| sequential | 4 | 128 | 286,056 | 260,619 | 460,588 | 0.699164 |
| sequential | 4 | 32 | 591,935 | 429,908 | 616,311 | 0.337875 |
| sequential | 4 | 8 | 328,086 | 268,985 | 397,782 | 0.609597 |

## Notes

- `concurrent` uses one goroutine per Worker against one shared `Client`.
- `sequential` drains Workers one after another.
- Same-host loopback; do not treat as production capacity.
