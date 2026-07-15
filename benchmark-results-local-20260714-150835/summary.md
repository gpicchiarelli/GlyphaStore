# GlyphaStore benchmark report

Generated at `2026-07-14T13:10:23+00:00` from 43 result(s).

> Results from GitHub-hosted runners are suitable for observing large regressions, not for absolute performance claims. Runner contention and hardware can vary.

| Suite | Benchmark | Configuration | Median ops/s | Δ ops/s | Median ns/op | p50 | p95 | p99 | p99.9 | RSS | Duplex |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| core-lto | index_insert | k=16, v=64, w=1, t=1, uniform | 12.08 M | — | 82.81 | — | — | — | — | 45.73 MiB | 0.00 |
| core-lto | index_replace | k=16, v=64, w=1, t=1, uniform | 11.50 M | — | 86.93 | — | — | — | — | 47.30 MiB | 0.00 |
| core-lto | index_find_hit | k=16, v=64, w=1, t=1, uniform | 12.44 M | — | 80.41 | — | — | — | — | 47.30 MiB | 0.00 |
| core-lto | index_find_miss | k=16, v=64, w=1, t=1, uniform | 13.62 M | — | 73.43 | — | — | — | — | 53.45 MiB | 0.00 |
| core-lto | index_erase | k=16, v=64, w=1, t=1, uniform | 12.22 M | — | 81.80 | — | — | — | — | 53.47 MiB | 0.00 |
| core-lto | store_put | k=16, v=64, w=1, t=1, uniform | 5.00 M | — | 199.97 | — | — | — | — | 96.12 MiB | 0.00 |
| core-lto | store_get_copy | k=16, v=64, w=1, t=1, uniform | 4.61 M | — | 217.02 | — | — | — | — | 96.88 MiB | 0.00 |
| core-lto | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 4.75 M | — | 210.49 | — | — | — | — | 97.03 MiB | 0.00 |
| core-lto | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 6.10 M | — | 164.06 | — | — | — | — | 97.08 MiB | 0.00 |
| core | index_insert | k=16, v=64, w=1, t=1, uniform | 12.78 M | — | 78.23 | — | — | — | — | 45.70 MiB | 0.00 |
| core | index_replace | k=16, v=64, w=1, t=1, uniform | 10.67 M | — | 93.71 | — | — | — | — | 45.75 MiB | 0.00 |
| core | index_find_hit | k=16, v=64, w=1, t=1, uniform | 11.53 M | — | 86.72 | — | — | — | — | 45.77 MiB | 0.00 |
| core | index_find_miss | k=16, v=64, w=1, t=1, uniform | 13.65 M | — | 73.27 | — | — | — | — | 53.20 MiB | 0.00 |
| core | index_erase | k=16, v=64, w=1, t=1, uniform | 11.78 M | — | 84.87 | — | — | — | — | 53.20 MiB | 0.00 |
| core | store_put | k=16, v=64, w=1, t=1, uniform | 4.29 M | — | 233.14 | — | — | — | — | 95.34 MiB | 0.00 |
| core | store_get_copy | k=16, v=64, w=1, t=1, uniform | 4.42 M | — | 226.50 | — | — | — | — | 96.39 MiB | 0.00 |
| core | store_put_get_copy | k=16, v=64, w=1, t=1, uniform | 4.39 M | — | 227.91 | — | — | — | — | 97.05 MiB | 0.00 |
| core | store_read_after_write_copy | k=16, v=64, w=1, t=1, uniform | 5.60 M | — | 178.72 | — | — | — | — | 97.06 MiB | 0.00 |
| parallel-single-worker | store_parallel_put | k=16, v=64, w=2, t=2, single-worker | 2.44 M | — | 410.55 | — | — | — | — | 93.97 MiB | 0.00 |
| parallel-single-worker | store_parallel_get_copy | k=16, v=64, w=2, t=2, single-worker | 2.05 M | — | 487.83 | — | — | — | — | 94.16 MiB | 0.00 |
| parallel-single-worker | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, single-worker | 2.77 M | — | 361.59 | — | — | — | — | 94.50 MiB | 0.00 |
| parallel-uniform | store_parallel_put | k=16, v=64, w=2, t=2, uniform | 2.44 M | — | 409.74 | — | — | — | — | 93.12 MiB | 0.00 |
| parallel-uniform | store_parallel_get_copy | k=16, v=64, w=2, t=2, uniform | 2.15 M | — | 465.69 | — | — | — | — | 95.91 MiB | 0.00 |
| parallel-uniform | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, uniform | 2.93 M | — | 340.99 | — | — | — | — | 96.00 MiB | 0.00 |
| parallel-worker-affine | store_parallel_put | k=16, v=64, w=2, t=2, worker-affine | 8.38 M | — | 119.28 | — | — | — | — | 93.02 MiB | 0.00 |
| parallel-worker-affine | store_parallel_get_copy | k=16, v=64, w=2, t=2, worker-affine | 8.70 M | — | 114.89 | — | — | — | — | 96.55 MiB | 0.00 |
| parallel-worker-affine | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, worker-affine | 11.22 M | — | 89.13 | — | — | — | — | 96.58 MiB | 0.00 |
| parallel-zipf | store_parallel_put | k=16, v=64, w=2, t=2, zipf | 2.34 M | — | 427.78 | — | — | — | — | 93.75 MiB | 0.00 |
| parallel-zipf | store_parallel_get_copy | k=16, v=64, w=2, t=2, zipf | 2.07 M | — | 483.92 | — | — | — | — | 94.56 MiB | 0.00 |
| parallel-zipf | store_parallel_read_after_write_copy | k=16, v=64, w=2, t=2, zipf | 2.79 M | — | 358.21 | — | — | — | — | 94.61 MiB | 0.00 |
| server-latency-w2-p32 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=32 | 1.37 M | — | 727.89 | 81.00 µs | 142.00 µs | 214.33 µs | 1.19 ms | 86.98 MiB | 219.81 M |
| server-tcp-w1-p1 | server_tcp_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=1 | 86.27 k | — | 11,591.80 | — | — | — | — | 70.72 MiB | 13.80 M |
| server-tcp-w1-p128 | server_tcp_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=128 | 2.49 M | — | 401.03 | — | — | — | — | 71.05 MiB | 398.98 M |
| server-tcp-w1-p32 | server_tcp_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=32 | 1.44 M | — | 692.63 | — | — | — | — | 71.36 MiB | 231.00 M |
| server-tcp-w1-p8 | server_tcp_read_after_write | k=16, v=64, w=1, t=1, owner-bound, p=8 | 582.64 k | — | 1,716.33 | — | — | — | — | 71.63 MiB | 93.22 M |
| server-tcp-w2-p1 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=1 | 77.89 k | — | 12,838.20 | — | — | — | — | 72.97 MiB | 12.46 M |
| server-tcp-w2-p128 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=128 | 2.77 M | — | 360.38 | — | — | — | — | 74.00 MiB | 443.98 M |
| server-tcp-w2-p32 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=32 | 2.02 M | — | 495.30 | — | — | — | — | 73.42 MiB | 323.04 M |
| server-tcp-w2-p8 | server_tcp_read_after_write | k=16, v=64, w=2, t=2, owner-bound, p=8 | 728.40 k | — | 1,372.86 | — | — | — | — | 73.45 MiB | 116.55 M |
| server-tcp-w4-p1 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=1 | 114.74 k | — | 8,715.62 | — | — | — | — | 78.06 MiB | 18.36 M |
| server-tcp-w4-p128 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=128 | 4.75 M | — | 210.45 | — | — | — | — | 80.13 MiB | 760.29 M |
| server-tcp-w4-p32 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=32 | 2.56 M | — | 391.14 | — | — | — | — | 74.69 MiB | 409.06 M |
| server-tcp-w4-p8 | server_tcp_read_after_write | k=16, v=64, w=4, t=4, owner-bound, p=8 | 835.24 k | — | 1,197.25 | — | — | — | — | 75.27 MiB | 133.64 M |

## Run metadata

| Suite | Commit | Platform | Architecture | Compiler |
| --- | --- | --- | --- | --- |
| core-lto | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| core | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| parallel-single-worker | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| parallel-uniform | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| parallel-worker-affine | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| parallel-zipf | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-latency-w2-p32 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w1-p1 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w1-p128 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w1-p32 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w1-p8 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w2-p1 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w2-p128 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w2-p32 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w2-p8 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w4-p1 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w4-p128 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w4-p32 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| server-tcp-w4-p8 | 7f54681-dirty | macos | arm64 | Apple LLVM 21.0.0 (clang-2100.1.1.101) |
| timing | unknown | unknown | unknown | unknown |
