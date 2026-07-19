# GlyphaStore benchmark report

Generated at `2026-07-19T14:42:33+00:00` from 125 result(s).

> Results from GitHub-hosted runners are suitable for observing large regressions, not for absolute performance claims. Runner contention and hardware can vary.

| Suite | Benchmark | Configuration | Median ops/s | Δ ops/s | Median ns/op | p50 | p95 | p99 | p99.9 | RSS | Duplex |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| client-api-latency-w4 | cpp_client_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=1 | 91.24 k | — | 10,959.70 | 40.17 µs | 66.88 µs | 86.00 µs | 154.38 µs | 58.36 MiB | 14.60 M |
| client-api-w1 | cpp_client_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=1 | 39.96 k | — | 25,024.20 | — | — | — | — | 42.48 MiB | 6.39 M |
| client-api-w2 | cpp_client_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=1 | 67.01 k | — | 14,922.30 | — | — | — | — | 46.78 MiB | 10.72 M |
| client-api-w4 | cpp_client_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=1 | 79.64 k | — | 12,556.10 | — | — | — | — | 48.91 MiB | 12.74 M |
| client-api-w8 | cpp_client_read_after_write | k=16, v=64, w=8, t=8, owner-bound, p=1 | 104.22 k | — | 9,594.88 | — | — | — | — | 45.48 MiB | 16.68 M |
| core-lto | index_insert | k=16, v=64, w=1, t=1, uniform | 13.43 M | — | 74.46 | — | — | — | — | 40.75 MiB | 0.00 |
| core-lto | index_replace | k=16, v=64, w=1, t=1, uniform | 9.98 M | — | 100.17 | — | — | — | — | 40.80 MiB | 0.00 |
| core-lto | index_find_hit | k=16, v=64, w=1, t=1, uniform | 12.46 M | — | 80.26 | — | — | — | — | 42.34 MiB | 0.00 |
| core-lto | index_find_miss | k=16, v=64, w=1, t=1, uniform | 13.78 M | — | 72.58 | — | — | — | — | 48.47 MiB | 0.00 |
| core-lto | index_churn_miss | k=16, v=64, w=1, t=1, uniform | 16.77 M | — | 59.62 | — | — | — | — | 64.73 MiB | 0.00 |
| core-lto | index_erase | k=16, v=64, w=1, t=1, uniform | 11.44 M | — | 87.45 | — | — | — | — | 64.73 MiB | 0.00 |
| core-lto | store_put | k=16, v=64, w=1, t=1, uniform | 4.00 M | — | 250.07 | — | — | — | — | 101.16 MiB | 0.00 |
| core-lto | store_get_copy | k=16, v=64, w=1, t=1, uniform | 3.68 M | — | 271.65 | — | — | — | — | 101.27 MiB | 0.00 |
| core-lto | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 3.61 M | — | 276.82 | — | — | — | — | 101.30 MiB | 0.00 |
| core-lto | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 4.40 M | — | 227.08 | — | — | — | — | 101.30 MiB | 0.00 |
| core-native | index_insert | k=16, v=64, w=1, t=1, uniform | 12.92 M | — | 77.43 | — | — | — | — | 40.83 MiB | 0.00 |
| core-native | index_replace | k=16, v=64, w=1, t=1, uniform | 9.81 M | — | 101.98 | — | — | — | — | 40.86 MiB | 0.00 |
| core-native | index_find_hit | k=16, v=64, w=1, t=1, uniform | 12.03 M | — | 83.14 | — | — | — | — | 40.86 MiB | 0.00 |
| core-native | index_find_miss | k=16, v=64, w=1, t=1, uniform | 13.99 M | — | 71.47 | — | — | — | — | 46.98 MiB | 0.00 |
| core-native | index_churn_miss | k=16, v=64, w=1, t=1, uniform | 17.10 M | — | 58.46 | — | — | — | — | 63.25 MiB | 0.00 |
| core-native | index_erase | k=16, v=64, w=1, t=1, uniform | 10.27 M | — | 97.38 | — | — | — | — | 63.27 MiB | 0.00 |
| core-native | store_put | k=16, v=64, w=1, t=1, uniform | 3.67 M | — | 272.15 | — | — | — | — | 99.70 MiB | 0.00 |
| core-native | store_get_copy | k=16, v=64, w=1, t=1, uniform | 3.61 M | — | 277.34 | — | — | — | — | 99.83 MiB | 0.00 |
| core-native | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 3.55 M | — | 281.64 | — | — | — | — | 100.00 MiB | 0.00 |
| core-native | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 4.02 M | — | 248.82 | — | — | — | — | 100.13 MiB | 0.00 |
| core-release | index_insert | k=16, v=64, w=1, t=1, uniform | 13.08 M | — | 76.48 | — | — | — | — | 40.81 MiB | 0.00 |
| core-release | index_replace | k=16, v=64, w=1, t=1, uniform | 10.94 M | — | 91.42 | — | — | — | — | 40.84 MiB | 0.00 |
| core-release | index_find_hit | k=16, v=64, w=1, t=1, uniform | 13.30 M | — | 75.21 | — | — | — | — | 40.84 MiB | 0.00 |
| core-release | index_find_miss | k=16, v=64, w=1, t=1, uniform | 14.04 M | — | 71.24 | — | — | — | — | 46.97 MiB | 0.00 |
| core-release | index_churn_miss | k=16, v=64, w=1, t=1, uniform | 16.81 M | — | 59.48 | — | — | — | — | 63.23 MiB | 0.00 |
| core-release | index_erase | k=16, v=64, w=1, t=1, uniform | 10.75 M | — | 92.99 | — | — | — | — | 63.25 MiB | 0.00 |
| core-release | store_put | k=16, v=64, w=1, t=1, uniform | 3.10 M | — | 322.31 | — | — | — | — | 101.11 MiB | 0.00 |
| core-release | store_get_copy | k=16, v=64, w=1, t=1, uniform | 3.35 M | — | 298.50 | — | — | — | — | 101.27 MiB | 0.00 |
| core-release | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 3.54 M | — | 282.69 | — | — | — | — | 102.11 MiB | 0.00 |
| core-release | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 4.05 M | — | 246.93 | — | — | — | — | 102.11 MiB | 0.00 |
| durable-group-parallel-put | store_durable_group_put | k=16, v=64, w=1, t=32, single-worker | 6.21 k | — | 161,147.00 | 4.99 ms | 6.13 ms | 6.86 ms | 7.21 ms | 3.09 MiB | 0.00 |
| durable-periodic-all | store_durable_periodic_put | k=16, v=64, w=1, t=1, uniform | 115.35 k | — | 8,669.28 | — | — | — | — | 13.59 MiB | 0.00 |
| durable-periodic-all | store_durable_periodic_get_copy | k=16, v=64, w=1, t=1, uniform | 2.92 M | — | 342.33 | — | — | — | — | 16.87 MiB | 0.00 |
| durable-periodic-all | store_durable_periodic_put_get_copy | k=16, v=64, w=1, t=1, uniform | 194.84 k | — | 5,132.28 | — | — | — | — | 17.66 MiB | 0.00 |
| durable-periodic-all | store_durable_periodic_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 219.88 k | — | 4,548.05 | — | — | — | — | 17.70 MiB | 0.00 |
| durable-recovery-open | store_durable_recovery_open | k=16, v=64, w=1, t=1, uniform | 250.71 k | — | 3,988.61 | — | — | — | — | 2.39 MiB | 0.00 |
| durable-sync-put | store_durable_put | k=16, v=64, w=1, t=1, uniform | 196.04 | — | 5,101,000.00 | — | — | — | — | 2.11 MiB | 0.00 |
| index-key-16 | index_insert | k=16, v=64, w=1, t=1, uniform | 13.16 M | — | 76.00 | — | — | — | — | 40.78 MiB | 0.00 |
| index-key-16 | index_replace | k=16, v=64, w=1, t=1, uniform | 10.91 M | — | 91.62 | — | — | — | — | 40.84 MiB | 0.00 |
| index-key-16 | index_find_hit | k=16, v=64, w=1, t=1, uniform | 12.84 M | — | 77.88 | — | — | — | — | 40.86 MiB | 0.00 |
| index-key-16 | index_find_miss | k=16, v=64, w=1, t=1, uniform | 14.28 M | — | 70.02 | — | — | — | — | 46.98 MiB | 0.00 |
| index-key-16 | index_churn_miss | k=16, v=64, w=1, t=1, uniform | 17.08 M | — | 58.53 | — | — | — | — | 63.52 MiB | 0.00 |
| index-key-16 | index_erase | k=16, v=64, w=1, t=1, uniform | 13.12 M | — | 76.21 | — | — | — | — | 63.52 MiB | 0.00 |
| index-key-256 | index_insert | k=256, v=64, w=1, t=1, uniform | 1.74 M | — | 575.25 | — | — | — | — | 216.38 MiB | 0.00 |
| index-key-256 | index_replace | k=256, v=64, w=1, t=1, uniform | 1.58 M | — | 633.11 | — | — | — | — | 216.48 MiB | 0.00 |
| index-key-256 | index_find_hit | k=256, v=64, w=1, t=1, uniform | 1.56 M | — | 642.54 | — | — | — | — | 216.53 MiB | 0.00 |
| index-key-256 | index_find_miss | k=256, v=64, w=1, t=1, uniform | 1.85 M | — | 540.25 | — | — | — | — | 287.61 MiB | 0.00 |
| index-key-256 | index_churn_miss | k=256, v=64, w=1, t=1, uniform | 1.96 M | — | 510.69 | — | — | — | — | 296.48 MiB | 0.00 |
| index-key-256 | index_erase | k=256, v=64, w=1, t=1, uniform | 1.16 M | — | 861.03 | — | — | — | — | 296.64 MiB | 0.00 |
| index-key-64 | index_insert | k=64, v=64, w=1, t=1, uniform | 5.47 M | — | 182.78 | — | — | — | — | 84.52 MiB | 0.00 |
| index-key-64 | index_replace | k=64, v=64, w=1, t=1, uniform | 5.40 M | — | 185.08 | — | — | — | — | 85.14 MiB | 0.00 |
| index-key-64 | index_find_hit | k=64, v=64, w=1, t=1, uniform | 5.55 M | — | 180.08 | — | — | — | — | 85.41 MiB | 0.00 |
| index-key-64 | index_find_miss | k=64, v=64, w=1, t=1, uniform | 5.44 M | — | 183.86 | — | — | — | — | 113.55 MiB | 0.00 |
| index-key-64 | index_churn_miss | k=64, v=64, w=1, t=1, uniform | 6.82 M | — | 146.67 | — | — | — | — | 126.36 MiB | 0.00 |
| index-key-64 | index_erase | k=64, v=64, w=1, t=1, uniform | 3.44 M | — | 290.67 | — | — | — | — | 126.36 MiB | 0.00 |
| index-key-8 | index_insert | k=8, v=64, w=1, t=1, uniform | 13.99 M | — | 71.47 | — | — | — | — | 40.80 MiB | 0.00 |
| index-key-8 | index_replace | k=8, v=64, w=1, t=1, uniform | 12.95 M | — | 77.23 | — | — | — | — | 40.83 MiB | 0.00 |
| index-key-8 | index_find_hit | k=8, v=64, w=1, t=1, uniform | 15.04 M | — | 66.49 | — | — | — | — | 40.84 MiB | 0.00 |
| index-key-8 | index_find_miss | k=8, v=64, w=1, t=1, uniform | 18.79 M | — | 53.22 | — | — | — | — | 46.97 MiB | 0.00 |
| index-key-8 | index_churn_miss | k=8, v=64, w=1, t=1, uniform | 23.41 M | — | 42.72 | — | — | — | — | 63.23 MiB | 0.00 |
| index-key-8 | index_erase | k=8, v=64, w=1, t=1, uniform | 15.17 M | — | 65.92 | — | — | — | — | 63.23 MiB | 0.00 |
| parallel-single-worker-w2 | store_parallel_put | k=16, v=64, w=2, t=2, single-worker | 2.29 M | — | 436.50 | — | — | — | — | 85.89 MiB | 0.00 |
| parallel-single-worker-w2 | store_parallel_get_copy | k=16, v=64, w=2, t=2, single-worker | 1.86 M | — | 537.09 | — | — | — | — | 87.08 MiB | 0.00 |
| parallel-single-worker-w2 | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, single-worker | 2.36 M | — | 423.76 | — | — | — | — | 87.16 MiB | 0.00 |
| parallel-uniform-w2 | store_parallel_put | k=16, v=64, w=2, t=2, uniform | 2.33 M | — | 428.69 | — | — | — | — | 83.69 MiB | 0.00 |
| parallel-uniform-w2 | store_parallel_get_copy | k=16, v=64, w=2, t=2, uniform | 2.14 M | — | 467.90 | — | — | — | — | 83.92 MiB | 0.00 |
| parallel-uniform-w2 | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, uniform | 2.54 M | — | 393.77 | — | — | — | — | 84.17 MiB | 0.00 |
| parallel-worker-affine-w2 | store_parallel_put | k=16, v=64, w=2, t=2, worker-affine | 7.87 M | — | 127.11 | — | — | — | — | 85.30 MiB | 0.00 |
| parallel-worker-affine-w2 | store_parallel_get_copy | k=16, v=64, w=2, t=2, worker-affine | 8.16 M | — | 122.57 | — | — | — | — | 90.27 MiB | 0.00 |
| parallel-worker-affine-w2 | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, worker-affine | 8.52 M | — | 117.39 | — | — | — | — | 90.38 MiB | 0.00 |
| parallel-zipf-w2 | store_parallel_put | k=16, v=64, w=2, t=2, zipf | 2.19 M | — | 457.49 | — | — | — | — | 84.91 MiB | 0.00 |
| parallel-zipf-w2 | store_parallel_get_copy | k=16, v=64, w=2, t=2, zipf | 1.89 M | — | 528.38 | — | — | — | — | 85.19 MiB | 0.00 |
| parallel-zipf-w2 | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, zipf | 2.41 M | — | 414.91 | — | — | — | — | 88.72 MiB | 0.00 |
| scaling-affine-w1 | store_parallel_put | k=16, v=64, w=1, t=1, worker-affine | 3.80 M | — | 262.92 | — | — | — | — | 85.09 MiB | 0.00 |
| scaling-affine-w1 | store_parallel_get_copy | k=16, v=64, w=1, t=1, worker-affine | 3.45 M | — | 289.45 | — | — | — | — | 85.27 MiB | 0.00 |
| scaling-affine-w1 | store_parallel_read_after_write_copy | k=16, v=64, w=1, t=1, worker-affine | 4.21 M | — | 237.46 | — | — | — | — | 86.27 MiB | 0.00 |
| scaling-affine-w2 | store_parallel_put | k=16, v=64, w=2, t=2, worker-affine | 7.39 M | — | 135.25 | — | — | — | — | 84.97 MiB | 0.00 |
| scaling-affine-w2 | store_parallel_get_copy | k=16, v=64, w=2, t=2, worker-affine | 7.65 M | — | 130.64 | — | — | — | — | 85.53 MiB | 0.00 |
| scaling-affine-w2 | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, worker-affine | 8.32 M | — | 120.13 | — | — | — | — | 85.55 MiB | 0.00 |
| scaling-affine-w4 | store_parallel_put | k=16, v=64, w=4, t=4, worker-affine | 7.21 M | — | 138.76 | — | — | — | — | 88.95 MiB | 0.00 |
| scaling-affine-w4 | store_parallel_get_copy | k=16, v=64, w=4, t=4, worker-affine | 15.90 M | — | 62.88 | — | — | — | — | 92.23 MiB | 0.00 |
| scaling-affine-w4 | store_parallel_read_after_write_copy | k=16, v=64, w=4, t=4, worker-affine | 17.48 M | — | 57.21 | — | — | — | — | 96.31 MiB | 0.00 |
| scaling-affine-w8 | store_parallel_put | k=16, v=64, w=8, t=8, worker-affine | 20.22 M | — | 49.46 | — | — | — | — | 82.84 MiB | 0.00 |
| scaling-affine-w8 | store_parallel_get_copy | k=16, v=64, w=8, t=8, worker-affine | 21.66 M | — | 46.17 | — | — | — | — | 91.22 MiB | 0.00 |
| scaling-affine-w8 | store_parallel_read_after_write_copy | k=16, v=64, w=8, t=8, worker-affine | 21.80 M | — | 45.87 | — | — | — | — | 91.25 MiB | 0.00 |
| scaling-uniform-w1 | store_parallel_put | k=16, v=64, w=1, t=1, uniform | 3.51 M | — | 285.30 | — | — | — | — | 84.91 MiB | 0.00 |
| scaling-uniform-w1 | store_parallel_get_copy | k=16, v=64, w=1, t=1, uniform | 3.57 M | — | 279.87 | — | — | — | — | 85.19 MiB | 0.00 |
| scaling-uniform-w1 | store_parallel_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 4.39 M | — | 227.83 | — | — | — | — | 87.23 MiB | 0.00 |
| scaling-uniform-w2 | store_parallel_put | k=16, v=64, w=2, t=2, uniform | 2.34 M | — | 426.72 | — | — | — | — | 84.33 MiB | 0.00 |
| scaling-uniform-w2 | store_parallel_get_copy | k=16, v=64, w=2, t=2, uniform | 1.95 M | — | 513.48 | — | — | — | — | 86.78 MiB | 0.00 |
| scaling-uniform-w2 | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, uniform | 2.56 M | — | 391.19 | — | — | — | — | 88.98 MiB | 0.00 |
| scaling-uniform-w4 | store_parallel_put | k=16, v=64, w=4, t=4, uniform | 2.27 M | — | 440.21 | — | — | — | — | 81.72 MiB | 0.00 |
| scaling-uniform-w4 | store_parallel_get_copy | k=16, v=64, w=4, t=4, uniform | 3.08 M | — | 324.73 | — | — | — | — | 86.39 MiB | 0.00 |
| scaling-uniform-w4 | store_parallel_read_after_write_copy | k=16, v=64, w=4, t=4, uniform | 5.14 M | — | 194.60 | — | — | — | — | 86.45 MiB | 0.00 |
| scaling-uniform-w8 | store_parallel_put | k=16, v=64, w=8, t=8, uniform | 3.25 M | — | 307.36 | — | — | — | — | 75.77 MiB | 0.00 |
| scaling-uniform-w8 | store_parallel_get_copy | k=16, v=64, w=8, t=8, uniform | 3.73 M | — | 267.82 | — | — | — | — | 77.33 MiB | 0.00 |
| scaling-uniform-w8 | store_parallel_read_after_write_copy | k=16, v=64, w=8, t=8, uniform | 4.44 M | — | 225.09 | — | — | — | — | 85.41 MiB | 0.00 |
| server-latency-w2-p32 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=32 | 1.69 M | — | 590.87 | 67.25 µs | 110.08 µs | 144.50 µs | 1.03 ms | 86.34 MiB | 270.79 M |
| server-latency-w4-p1 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=1 | 198.64 k | — | 5,034.25 | 36.04 µs | 63.29 µs | 89.88 µs | 254.12 µs | 84.16 MiB | 31.78 M |
| server-size-k256-v65536-p32 | server_tcp_read_after_write | k=256, v=65536, w=4, t=4, owner-bound, p=32 | 62.44 k | — | 16,015.10 | — | — | — | — | 399.05 MiB | 4.11 G |
| server-size-k32-v256 | server_tcp_read_after_write | k=32, v=256, w=4, t=4, owner-bound, p=128 | 5.09 M | — | 196.58 | — | — | — | — | 145.66 MiB | 1.87 G |
| server-size-k64-v4096 | server_tcp_read_after_write | k=64, v=4096, w=4, t=4, owner-bound, p=128 | 927.64 k | — | 1,078.00 | — | — | — | — | 268.48 MiB | 3.93 G |
| server-size-k8-v16 | server_tcp_read_after_write | k=8, v=16, w=4, t=4, owner-bound, p=128 | 8.20 M | — | 121.97 | — | — | — | — | 52.67 MiB | 852.68 M |
| server-tcp-w1-p1 | server_tcp_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=1 | 90.78 k | — | 11,015.70 | — | — | — | — | 71.20 MiB | 14.52 M |
| server-tcp-w1-p128 | server_tcp_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=128 | 2.27 M | — | 440.68 | — | — | — | — | 68.86 MiB | 363.08 M |
| server-tcp-w1-p32 | server_tcp_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=32 | 1.31 M | — | 760.81 | — | — | — | — | 68.36 MiB | 210.30 M |
| server-tcp-w1-p8 | server_tcp_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=8 | 571.55 k | — | 1,749.63 | — | — | — | — | 67.33 MiB | 91.45 M |
| server-tcp-w2-p1 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=1 | 120.39 k | — | 8,306.44 | — | — | — | — | 72.16 MiB | 19.26 M |
| server-tcp-w2-p128 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=128 | 4.04 M | — | 247.30 | — | — | — | — | 69.34 MiB | 646.98 M |
| server-tcp-w2-p32 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=32 | 2.36 M | — | 423.51 | — | — | — | — | 72.06 MiB | 377.79 M |
| server-tcp-w2-p8 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=8 | 612.85 k | — | 1,631.71 | — | — | — | — | 69.17 MiB | 98.06 M |
| server-tcp-w4-p1 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=1 | 170.46 k | — | 5,866.36 | — | — | — | — | 81.05 MiB | 27.27 M |
| server-tcp-w4-p128 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=128 | 5.42 M | — | 184.46 | — | — | — | — | 70.05 MiB | 867.40 M |
| server-tcp-w4-p32 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=32 | 3.08 M | — | 325.08 | — | — | — | — | 77.95 MiB | 492.19 M |
| server-tcp-w4-p8 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=8 | 1.23 M | — | 812.91 | — | — | — | — | 75.37 MiB | 196.82 M |
| store-size-k16-v64 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 3.52 M | — | 284.16 | — | — | — | — | 82.95 MiB | 0.00 |
| store-size-k256-v65536 | store_put_get_copy | k=256, v=65536, w=1, t=1, uniform | 62.35 k | — | 16,039.70 | — | — | — | — | 256.11 MiB | 0.00 |
| store-size-k32-v256 | store_put_get_copy | k=32, v=256, w=1, t=1, uniform | 2.54 M | — | 394.29 | — | — | — | — | 89.52 MiB | 0.00 |
| store-size-k64-v4096 | store_put_get_copy | k=64, v=4096, w=1, t=1, uniform | 758.36 k | — | 1,318.64 | — | — | — | — | 169.44 MiB | 0.00 |
| store-size-k8-v16 | store_put_get_copy | k=8, v=16, w=1, t=1, uniform | 4.25 M | — | 235.37 | — | — | — | — | 63.06 MiB | 0.00 |

## Run metadata

| Suite | Commit | Platform | Architecture | Compiler |
| --- | --- | --- | --- | --- |
| client-api-latency-w4 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| client-api-w1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| client-api-w2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| client-api-w4 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| client-api-w8 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| core-lto | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| core-native | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| core-release | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| durable-group-parallel-put | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| durable-periodic-all | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| durable-recovery-open | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| durable-sync-put | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| index-key-16 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| index-key-256 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| index-key-64 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| index-key-8 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| parallel-single-worker-w2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| parallel-uniform-w2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| parallel-worker-affine-w2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| parallel-zipf-w2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| scaling-affine-w1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| scaling-affine-w2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| scaling-affine-w4 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| scaling-affine-w8 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| scaling-uniform-w1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| scaling-uniform-w2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| scaling-uniform-w4 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| scaling-uniform-w8 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-latency-w2-p32 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-latency-w4-p1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-size-k256-v65536-p32 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-size-k32-v256 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-size-k64-v4096 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-size-k8-v16 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w1-p1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w1-p128 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w1-p32 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w1-p8 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w2-p1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w2-p128 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w2-p32 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w2-p8 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w4-p1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w4-p128 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w4-p32 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w4-p8 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| store-size-k16-v64 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| store-size-k256-v65536 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| store-size-k32-v256 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| store-size-k64-v4096 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| store-size-k8-v16 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
