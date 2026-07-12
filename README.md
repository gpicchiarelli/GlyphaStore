# GlyphaStore

GlyphaStore is an early C++23 architecture prototype for a high-performance, log-indexed,
segmented, memory-first key-value store. It is designed around immutable records, fixed 64 MiB
segments, a derived exact-key Index, automatic worker sizing, and bounded background maintenance.

> **Status:** architectural prototype. Not production ready. There is no stable disk format,
> network protocol, durability contract, or compatibility promise yet.

## What it is

- A single logical key-space with worker-owned Index partitions.
- Fixed-size 64 MiB append-only segments using bump allocation.
- Immutable, checksummed, explicitly little-endian records.
- Exact-key lookup through an abstract Index (`key -> RecordRef`).
- Index recovery where the highest valid sequence wins and tombstones remove visibility.
- Copy-build-validate vacuum foundations and whole-segment liveness accounting.
- A macOS-first developer experience with generated Xcode projects and sanitizer presets.

## What it is not

- Not Redis-compatible and not a RESP implementation.
- Not a SQL database, ordered query engine, or general-purpose document store.
- Not a production server yet; the experimental binary TCP protocol has no compatibility promise.
- Not evidence of claimed throughput. Performance numbers will be published only from reproducible
  benchmarks.

## Quick start on macOS

```bash
./scripts/bootstrap-macos.sh
./scripts/open-xcode.sh
```

The bootstrap installs CMake and Ninja into a repository-local Python virtual environment, so it
does not require administrator rights or alter the system toolchain. Xcode remains the compiler,
debugger, profiler, and primary IDE.

Command-line workflow:

```bash
./scripts/dev.sh configure
./scripts/dev.sh build
./scripts/dev.sh test
./scripts/dev.sh asan
./scripts/dev.sh benchmark
```

See [macOS and Xcode development](docs/development-macos.md) for schemes, Instruments, Apple
Silicon notes, and troubleshooting.

## Portable CMake workflow

With CMake 3.25+ and Ninja available:

```bash
cmake --preset macos-debug
cmake --build --preset macos-debug
ctest --preset macos-debug
```

On Linux and FreeBSD, use the `unix-debug` and `unix-release` presets.

## Architecture

```text
key -> route(worker) -> Index partition -> RecordRef
                                      -> segment_id + offset -> immutable record

write -> append to worker active 64 MiB segment -> update Index
read  -> Index lookup -> validated positional segment access
```

The Index is an acceleration structure, not the only recovery truth. It can be rebuilt by scanning
valid records and retaining the highest sequence for each full key. See the
[architecture charter](docs/architecture/architecture-charter.md) and
[storage model](docs/architecture/storage-model.md).

## Tests, sanitizers, fuzzing, and benchmarks

```bash
./scripts/dev.sh test
./scripts/dev.sh asan
./scripts/dev.sh tsan
./scripts/dev.sh benchmark
./scripts/dev.sh benchmark --filter store-parallel-all --workers 4 --threads 4 --distribution uniform
./scripts/dev.sh fuzz-build
```

Fuzzers use Clang/libFuzzer and are disabled in normal builds. Benchmarks are bootstrap
microbenchmarks, not comparative product claims. Parallel Store benchmarks support `uniform`
(independent clients crossing Worker boundaries), `worker-affine` (one client thread per Worker),
`single-worker` (maximum contention), and `zipf` (skewed ownership) distributions.

## Experimental TCP daemon

`glyphastored` is the native non-blocking TCP server bootstrap. It uses `kqueue` on macOS/FreeBSD
and edge-triggered `epoll` on Linux. The current protocol milestone implements pipelined `HELLO`,
`PING`, `GET`, `PUT`, and `ERASE`. Store operations are hash-routed into bounded per-Worker MPSC
inboxes and completed asynchronously back on the owning network Reactor.

```bash
./scripts/dev.sh build
./build/macos-debug/glyphastored --bind 127.0.0.1 --port 7379
```

See the [server model](docs/architecture/server-model.md) for framing and concurrency invariants.

## Supported platforms

- macOS on Apple Silicon is the primary development platform.
- macOS Intel remains a supported CMake target.
- Linux and FreeBSD are architectural targets and CI/packaging coverage will expand with the core.

## Security

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md). Persisted bytes are
treated as untrusted input and all encoded lengths and offsets must be validated.

## Contributing

This repository is initially private. Development rules and review expectations are in
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

BSD-3-Clause. See [LICENSE](LICENSE).
