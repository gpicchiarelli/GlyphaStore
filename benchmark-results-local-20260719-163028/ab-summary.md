# GlyphaStore benchmark report

Generated at `2026-07-19T14:52:35+00:00` from 46 result(s).

> Interlaced local Apple M4 comparison with Low Power Mode enabled and advisory affinity. Paired ratios are more informative than absolute rates.

| Suite | Benchmark | Configuration | Median ops/s | Δ ops/s | Median ns/op | p50 | p95 | p99 | p99.9 | RSS | Duplex |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ab-new-lto-round1 | index_insert | k=16, v=64, w=1, t=1, uniform | 13.54 M | — | 73.84 | — | — | — | — | 40.75 MiB | 0.00 |
| ab-new-lto-round1 | index_replace | k=16, v=64, w=1, t=1, uniform | 7.27 M | — | 137.58 | — | — | — | — | 40.80 MiB | 0.00 |
| ab-new-lto-round1 | index_find_hit | k=16, v=64, w=1, t=1, uniform | 13.33 M | — | 75.03 | — | — | — | — | 40.80 MiB | 0.00 |
| ab-new-lto-round1 | index_find_miss | k=16, v=64, w=1, t=1, uniform | 14.42 M | — | 69.36 | — | — | — | — | 47.19 MiB | 0.00 |
| ab-new-lto-round1 | index_churn_miss | k=16, v=64, w=1, t=1, uniform | 17.25 M | — | 57.97 | — | — | — | — | 63.45 MiB | 0.00 |
| ab-new-lto-round1 | index_erase | k=16, v=64, w=1, t=1, uniform | 11.88 M | — | 84.21 | — | — | — | — | 63.70 MiB | 0.00 |
| ab-new-lto-round1 | store_put | k=16, v=64, w=1, t=1, uniform | 4.18 M | — | 239.25 | — | — | — | — | 100.50 MiB | 0.00 |
| ab-new-lto-round1 | store_get_copy | k=16, v=64, w=1, t=1, uniform | 3.69 M | — | 271.35 | — | — | — | — | 102.70 MiB | 0.00 |
| ab-new-lto-round1 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 3.79 M | — | 264.12 | — | — | — | — | 102.75 MiB | 0.00 |
| ab-new-lto-round1 | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 4.73 M | — | 211.46 | — | — | — | — | 102.75 MiB | 0.00 |
| ab-new-lto-round2 | index_insert | k=16, v=64, w=1, t=1, uniform | 13.44 M | — | 74.40 | — | — | — | — | 40.75 MiB | 0.00 |
| ab-new-lto-round2 | index_replace | k=16, v=64, w=1, t=1, uniform | 10.69 M | — | 93.55 | — | — | — | — | 40.80 MiB | 0.00 |
| ab-new-lto-round2 | index_find_hit | k=16, v=64, w=1, t=1, uniform | 11.73 M | — | 85.27 | — | — | — | — | 40.80 MiB | 0.00 |
| ab-new-lto-round2 | index_find_miss | k=16, v=64, w=1, t=1, uniform | 14.25 M | — | 70.16 | — | — | — | — | 47.19 MiB | 0.00 |
| ab-new-lto-round2 | index_churn_miss | k=16, v=64, w=1, t=1, uniform | 17.29 M | — | 57.83 | — | — | — | — | 63.45 MiB | 0.00 |
| ab-new-lto-round2 | index_erase | k=16, v=64, w=1, t=1, uniform | 12.23 M | — | 81.76 | — | — | — | — | 63.47 MiB | 0.00 |
| ab-new-lto-round2 | store_put | k=16, v=64, w=1, t=1, uniform | 4.26 M | — | 234.89 | — | — | — | — | 99.89 MiB | 0.00 |
| ab-new-lto-round2 | store_get_copy | k=16, v=64, w=1, t=1, uniform | 3.72 M | — | 268.59 | — | — | — | — | 99.98 MiB | 0.00 |
| ab-new-lto-round2 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 3.89 M | — | 257.40 | — | — | — | — | 101.83 MiB | 0.00 |
| ab-new-lto-round2 | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 4.54 M | — | 220.34 | — | — | — | — | 102.77 MiB | 0.00 |
| ab-new-store-put-get-round1 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 2.45 M | — | 407.95 | — | — | — | — | 86.94 MiB | 0.00 |
| ab-new-store-put-get-round2 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 2.54 M | — | 393.47 | — | — | — | — | 87.39 MiB | 0.00 |
| ab-new-store-read-after-write-round1 | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 3.32 M | — | 301.41 | — | — | — | — | 85.94 MiB | 0.00 |
| ab-new-store-read-after-write-round2 | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 3.03 M | — | 329.56 | — | — | — | — | 82.92 MiB | 0.00 |
| ab-old-lto-round1 | index_insert | k=16, v=64, w=1, t=1, uniform | 13.80 M | — | 72.46 | — | — | — | — | 45.73 MiB | 0.00 |
| ab-old-lto-round1 | index_replace | k=16, v=64, w=1, t=1, uniform | 10.38 M | — | 96.34 | — | — | — | — | 47.30 MiB | 0.00 |
| ab-old-lto-round1 | index_find_hit | k=16, v=64, w=1, t=1, uniform | 11.36 M | — | 88.00 | — | — | — | — | 47.34 MiB | 0.00 |
| ab-old-lto-round1 | index_find_miss | k=16, v=64, w=1, t=1, uniform | 12.20 M | — | 81.98 | — | — | — | — | 53.19 MiB | 0.00 |
| ab-old-lto-round1 | index_erase | k=16, v=64, w=1, t=1, uniform | 10.74 M | — | 93.12 | — | — | — | — | 53.19 MiB | 0.00 |
| ab-old-lto-round1 | store_put | k=16, v=64, w=1, t=1, uniform | 4.19 M | — | 238.58 | — | — | — | — | 95.83 MiB | 0.00 |
| ab-old-lto-round1 | store_get_copy | k=16, v=64, w=1, t=1, uniform | 3.66 M | — | 273.26 | — | — | — | — | 96.05 MiB | 0.00 |
| ab-old-lto-round1 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 3.83 M | — | 261.43 | — | — | — | — | 96.09 MiB | 0.00 |
| ab-old-lto-round1 | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 5.28 M | — | 189.39 | — | — | — | — | 96.09 MiB | 0.00 |
| ab-old-lto-round2 | index_insert | k=16, v=64, w=1, t=1, uniform | 13.72 M | — | 72.89 | — | — | — | — | 45.73 MiB | 0.00 |
| ab-old-lto-round2 | index_replace | k=16, v=64, w=1, t=1, uniform | 10.87 M | — | 91.99 | — | — | — | — | 45.77 MiB | 0.00 |
| ab-old-lto-round2 | index_find_hit | k=16, v=64, w=1, t=1, uniform | 11.36 M | — | 88.07 | — | — | — | — | 45.80 MiB | 0.00 |
| ab-old-lto-round2 | index_find_miss | k=16, v=64, w=1, t=1, uniform | 13.57 M | — | 73.70 | — | — | — | — | 52.20 MiB | 0.00 |
| ab-old-lto-round2 | index_erase | k=16, v=64, w=1, t=1, uniform | 11.34 M | — | 88.19 | — | — | — | — | 52.20 MiB | 0.00 |
| ab-old-lto-round2 | store_put | k=16, v=64, w=1, t=1, uniform | 4.23 M | — | 236.54 | — | — | — | — | 94.31 MiB | 0.00 |
| ab-old-lto-round2 | store_get_copy | k=16, v=64, w=1, t=1, uniform | 3.55 M | — | 281.85 | — | — | — | — | 95.25 MiB | 0.00 |
| ab-old-lto-round2 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 4.28 M | — | 233.41 | — | — | — | — | 96.31 MiB | 0.00 |
| ab-old-lto-round2 | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 5.46 M | — | 183.11 | — | — | — | — | 96.33 MiB | 0.00 |
| ab-old-store-put-get-round1 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 2.79 M | — | 357.86 | — | — | — | — | 93.41 MiB | 0.00 |
| ab-old-store-put-get-round2 | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 2.87 M | — | 348.62 | — | — | — | — | 93.41 MiB | 0.00 |
| ab-old-store-read-after-write-round1 | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 3.24 M | — | 308.18 | — | — | — | — | 93.30 MiB | 0.00 |
| ab-old-store-read-after-write-round2 | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 3.24 M | — | 308.49 | — | — | — | — | 93.41 MiB | 0.00 |

## Run metadata

| Suite | Commit | Platform | Architecture | Compiler |
| --- | --- | --- | --- | --- |
| ab-new-lto-round1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-new-lto-round2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-new-store-put-get-round1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-new-store-put-get-round2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-new-store-read-after-write-round1 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-new-store-read-after-write-round2 | 28a12ae | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-old-lto-round1 | 7f54681 | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-old-lto-round2 | 7f54681 | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-old-store-put-get-round1 | 7f54681 | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-old-store-put-get-round2 | 7f54681 | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-old-store-read-after-write-round1 | 7f54681 | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| ab-old-store-read-after-write-round2 | 7f54681 | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
