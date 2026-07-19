# Reproduction commands

All primary measurements ran from a detached, clean worktree at `28a12ae`. The binaries were built
with the `macos-release`, `macos-release-lto`, and `macos-native-release` presets. Unless shown
otherwise, results use one warmup and seven measured repetitions.

## Core and optimizer variants

```sh
glyphastore_benchmarks --filter all --ops 200000 --warmup 1 --repeats 7
```

The command ran once with each Release, LTO, and native-CPU binary.

## Index and value-size matrix

```sh
for key in 8 16 32 64 256; do
  glyphastore_benchmarks --filter index-all --ops 200000 \
    --key-size "$key" --value-size 64 --warmup 1 --repeats 5
done

glyphastore_benchmarks --filter store-put-get --ops 200000 \
  --key-size 16 --value-size 0 --warmup 1 --repeats 5
glyphastore_benchmarks --filter store-put-get --ops 200000 \
  --key-size 16 --value-size 64 --warmup 1 --repeats 5
glyphastore_benchmarks --filter store-put-get --ops 200000 \
  --key-size 16 --value-size 256 --warmup 1 --repeats 5
glyphastore_benchmarks --filter store-put-get --ops 20000 \
  --key-size 64 --value-size 4096 --warmup 1 --repeats 5
glyphastore_benchmarks --filter store-put-get --ops 2000 \
  --key-size 256 --value-size 65536 --warmup 1 --repeats 5
```

Additional `k8/v16` and `k32/v256` points preserve realistic correlated-size workloads.

## Parallel Store

```sh
for distribution in uniform worker-affine single-worker zipf; do
  glyphastore_benchmarks --filter store-parallel-all --ops 200000 \
    --workers 2 --threads 2 --distribution "$distribution" --warmup 1 --repeats 7
done

for topology in 1 2 4 8 10; do
  for distribution in worker-affine uniform; do
    glyphastore_benchmarks --filter store-parallel-all --ops 200000 \
      --workers "$topology" --threads "$topology" --distribution "$distribution" \
      --warmup 1 --repeats 7
  done
done
```

## Persistence

```sh
glyphastore_benchmarks --filter store-durable-put --ops 256 \
  --workers 1 --warmup 1 --repeats 3
glyphastore_benchmarks --filter store-durable-periodic-all --ops 20000 \
  --workers 1 --warmup 1 --repeats 5
glyphastore_benchmarks --filter store-durable-group-parallel-put --ops 1024 \
  --workers 1 --threads 32 --distribution single-worker --latency --warmup 1 --repeats 3
glyphastore_benchmarks --filter store-durable-recovery-open --ops 256 \
  --workers 1 --warmup 1 --repeats 3
```

Recovery uses 256 records because setup performs synchronous durable PUTs outside the timed open. A
20,000-record attempt was stopped when it became clear it would measure fixture `fsync` time for
minutes rather than recovery.

## TCP and public C++ client

```sh
for workers in 1 2 4; do
  for pipeline in 1 8 32 128; do
    glyphastore_server_benchmarks --ops 100000 --workers "$workers" \
      --clients "$workers" --pipeline "$pipeline" --executor-affinity \
      --warmup 1 --repeats 7
  done
done

glyphastore_server_benchmarks --ops 100000 --workers 2 --clients 2 --pipeline 32 \
  --executor-affinity --latency --warmup 1 --repeats 7
glyphastore_server_benchmarks --ops 100000 --workers 4 --clients 4 --pipeline 1 \
  --executor-affinity --latency --warmup 1 --repeats 7

for topology in 1 2 4 8; do
  glyphastore_server_benchmarks --client-api --ops 100000 --workers "$topology" \
    --clients "$topology" --executor-affinity --warmup 1 --repeats 7
done
glyphastore_server_benchmarks --client-api --ops 100000 --workers 4 --clients 4 \
  --executor-affinity --latency --warmup 1 --repeats 7
```

The TCP size matrix used four Workers/clients and pipeline 128 for values through 4 KiB. A 65,536-byte
value exceeded the default 4 MiB per-connection watermark at pipeline 128 and correctly failed sample
validation; `server-size-k256-v65536-p128-expected-failure.txt` preserves the command outcome. The same
value passed at pipeline 32 and is recorded in `server-size-k256-v65536-p32.txt`.

## CPU sample

```sh
glyphastore_server_benchmarks --ops 1000000 --workers 4 --clients 4 --pipeline 32 \
  --executor-affinity --warmup 1 --repeats 20 &
pid=$!
sample "$pid" 5 1 -mayDie -fullPaths -file cpu-sample-server-w4-p32.txt
```

The profiling workload is stored separately and excluded from the canonical report because sampling
perturbs throughput.

## Controlled historical A/B

Clean LTO binaries for `7f54681` and `28a12ae` ran in alternating order. Two full-suite rounds and two
isolated rounds of `store-put-get` and `store-read-after-write` are retained as `ab-*.txt` and summarized
separately. The older checked-in report alone is not treated as controlled evidence.
