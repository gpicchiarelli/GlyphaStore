<h1 align="center">GlyphaStore</h1>

<p align="center">
  <strong>A log-indexed, segmented, memory-first key-value store in C++23.</strong>
</p>

<!-- build and quality -->
<p align="center">
  <a href="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/benchmarks.yml"><img alt="Benchmarks" src="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/benchmarks.yml/badge.svg"></a>
  <a href="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/sanitizers.yml"><img alt="Sanitizers" src="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/sanitizers.yml/badge.svg"></a>
  <a href="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/static-analysis.yml"><img alt="Static analysis" src="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/static-analysis.yml/badge.svg"></a>
  <a href="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/codeql.yml"><img alt="CodeQL" src="https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/codeql.yml/badge.svg"></a>
</p>

<!-- toolchain and release posture -->
<p align="center">
  <a href="https://en.cppreference.com/w/cpp/23.html"><img alt="C++23" src="https://img.shields.io/badge/C%2B%2B-23-00599C?logo=cplusplus&amp;logoColor=white"></a>
  <a href="CMakeLists.txt"><img alt="CMake 3.25+" src="https://img.shields.io/badge/CMake-3.25%2B-064F8C?logo=cmake&amp;logoColor=white"></a>
  <a href="CHANGELOG.md"><img alt="Version 0.1.0" src="https://img.shields.io/badge/version-0.1.0-blue"></a>
  <a href="#project-status"><img alt="Architectural prototype" src="https://img.shields.io/badge/status-architectural%20prototype-orange"></a>
  <a href="SECURITY.md"><img alt="Security policy" src="https://img.shields.io/badge/security-policy-critical?logo=github"></a>
  <a href="LICENSE"><img alt="BSD-3-Clause" src="https://img.shields.io/badge/license-BSD--3--Clause-blue"></a>
</p>

<!-- project targets -->
<p align="center">
  <a href="#supported-platforms"><img alt="Linux target since 0.1.0" src="https://img.shields.io/badge/target-Linux%20since%200.1.0-FCC624?logo=linux&amp;logoColor=black"></a>
  <a href="#supported-platforms"><img alt="macOS target since 0.1.0" src="https://img.shields.io/badge/target-macOS%20since%200.1.0-000000?logo=apple&amp;logoColor=white"></a>
  <a href="#supported-platforms"><img alt="FreeBSD target since 0.1.0" src="https://img.shields.io/badge/target-FreeBSD%20since%200.1.0-AB2B28?logo=freebsd&amp;logoColor=white"></a>
  <a href="#supported-platforms"><img alt="OpenBSD target since 0.1.0" src="https://img.shields.io/badge/target-OpenBSD%20since%200.1.0-F2CA30?logo=openbsd&amp;logoColor=black"></a>
</p>

![GlyphaStore storage engine laboratory](docs/assets/glyphastore-hero.png)

<p align="center">
  <a href="#architecture">Architecture</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#engineering-evidence">Engineering evidence</a> ·
  <a href="#experimental-tcp-daemon">TCP server</a> ·
  <a href="#documentation">Documentation</a>
</p>

GlyphaStore explores a high-performance storage architecture built around immutable records,
fixed 64 MiB segments, a derived exact-key Index, automatic Worker sizing, and bounded background
maintenance. It presents one logical key-space while keeping ownership, recovery, and reclamation
explicit.

## Project status

> [!IMPORTANT]
> **Architectural prototype — not production ready.** The target alpha durability and public API
> contracts are documented and `Store::open(durable_sync)` supports crash-recoverable creation,
> mutation, rotation, and restart on the current development format. Stable formats, cross-platform
> crash evidence, compatibility guarantees, and production certification do not exist yet.

## Design at a glance

| Concern | Design |
| --- | --- |
| Key-space | One logical Store, deterministically routed to Worker-owned Index partitions |
| Storage | Fixed 64 MiB append-only Segments with bump allocation |
| Records | Immutable, checksummed, explicitly little-endian, and positionally addressed |
| Lookup | Exact full-key Index mapping to `RecordRef(segment, offset, size, sequence)` |
| Recovery | Rebuild by scanning valid Records; highest sequence wins, tombstones remove visibility |
| Maintenance | Whole-Segment liveness plus copy-build-validate vacuum foundations |
| Server | Native non-blocking TCP with `epoll` on Linux and `kqueue` on BSD/macOS |

The scope is intentionally narrow: GlyphaStore is not Redis-compatible, a RESP implementation, a
SQL or document database, or evidence of unverified throughput claims. The current TCP protocol is
experimental and has no compatibility promise.

## Quick start

### macOS and Xcode

```bash
./scripts/bootstrap-macos.sh
./scripts/open-xcode.sh
# optional full verification: ./scripts/verify-xcode.sh
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

### Portable CMake

With CMake 3.25+ and Ninja available:

```bash
cmake --preset macos-debug
cmake --build --preset macos-debug
ctest --preset macos-debug
```

On Linux, FreeBSD, and OpenBSD, use the `unix-debug` and `unix-release` presets.

### Embedded durable Store

Deployment-oriented experiments should prefer `durable_periodic` unless strict acknowledgement is
required. `durable_group` preserves strict acknowledgement while sharing the ordered Record and
commit-slot durability phases across concurrent writers; `durable_sync` applies them to every
mutation. On supported macOS storage the first phase uses an ordering barrier and the final phase
retains the full durable flush.

All durable modes use the same persistent v1 Manifest, Segment, commit-slot, and Record formats.
There is no alternate persistent format or automatic format migration path.

> [!WARNING]
> Durable mode is not certified on any filesystem yet. Use it only on local development storage;
> NFS, SMB, FUSE, overlay and other remote or user-space filesystems are unsupported. Process-kill
> and deterministic syscall-fault tests do not replace repeated block-device power-cut evidence.

```cpp
glyphastore::StoreConfig config{
    .worker_config = {.explicit_count = 4},
    .storage_mode = glyphastore::StorageMode::durable_periodic,
    .data_directory = "/private/path/to/store",
    .durable_open_mode = glyphastore::DurableOpenMode::open_or_create,
    .durable_periodic = {.sync_interval_ms = 1000}, // 4096 records / 4 MiB / 1000 ms by default
    .durable_limits = {
        .max_store_bytes = 65ULL * 1024 * 1024 * 1024,
        .reserved_free_bytes = 2ULL * 1024 * 1024 * 1024,
        .max_segment_count = 1024,
        .max_recovery_memory_bytes = 2ULL * 1024 * 1024 * 1024,
        .max_live_keys = 5'000'000,
        .max_temporary_compaction_bytes = 8ULL * 1024 * 1024 * 1024,
    },
};
auto store = glyphastore::Store::open(config);
if (!store) {
    // handle store.error()
}
// use **store
auto compacted = (*store)->compact(); // explicit: at most one Worker transaction
if (compacted && compacted->compacted) {
    // inspect compacted->worker_index and copy statistics
}
auto closed = (*store)->close(); // observe the final durability barrier
```

Compaction is cooperative maintenance: GlyphaStore does not create a compaction thread. Each
`compact()` call examines Workers in round-robin order, skips exact no-gain rewrites, and executes at
most one crash-safe whole-Worker transaction. Concurrent calls are not queued. An empty successful
result means no Worker currently offers a physical Segment gain.

Strict durability with group commit (batched fsync, zero loss on ack):

```cpp
glyphastore::StoreConfig config{
    .worker_config = {.explicit_count = 4},
    .storage_mode = glyphastore::StorageMode::durable_group,
    .data_directory = "/private/path/to/store",
    .durable_open_mode = glyphastore::DurableOpenMode::open_or_create,
    .durable_group =
        {.max_records = 32, .max_bytes = 65536, .max_wait_ms = 10, .min_records = 1},
};
auto store = glyphastore::Store::open(config);
```

Strict group batching starts at `max_records`, contracts toward observed deadline occupancy, and
grows again when more producers are already admitted. `min_records` and `max_records` are hard
bounds; adaptation never changes the absolute deadline or successful-ack durability.

Per-record strict durability:

```cpp
glyphastore::StoreConfig config{
    .worker_config = {.explicit_count = 4},
    .storage_mode = glyphastore::StorageMode::durable_sync,
    .data_directory = "/private/path/to/store",
    .durable_open_mode = glyphastore::DurableOpenMode::open_or_create,
};
auto store = glyphastore::Store::open(config);
```

`create_new` refuses an existing leaf, `open_existing` never initializes one, and
`open_or_create` initializes only a missing or otherwise pristine directory. Reopening uses the
persisted Worker count; an explicit conflicting count is rejected.

Resource limits are runtime policy and are not written into persistence v1. Defaults cap the Store
at 8 GiB/127 Segments, preserve 256 MiB of filesystem space, allow a 1 MiB manifest and 512 Store
descriptors, and bound recovery to 1 GiB and 10 million live keys. Creation, reopen, and rotation
fail before their first persistent transition when the configured policy cannot cover the required
peak. Deployments should set explicit values from their storage, process, and recovery envelope.

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

## Engineering evidence

```bash
./scripts/dev.sh test
./scripts/dev.sh asan
./scripts/dev.sh tsan
./scripts/dev.sh benchmark
./scripts/dev.sh benchmark-server --workers 4 --clients 4 --executor-affinity
./scripts/dev.sh benchmark --filter store-parallel-all --workers 4 --threads 4 --distribution uniform
./scripts/dev.sh fuzz-build
```

Fuzzers use Clang/libFuzzer and are disabled in normal builds. Benchmarks are bootstrap
microbenchmarks, not comparative product claims. Parallel Store benchmarks support `uniform`
(independent clients crossing Worker boundaries), `worker-affine` (one client thread per Worker),
`single-worker` (maximum contention), and `zipf` (skewed ownership) distributions. Durable-sync
filters (`store-durable-*`) use temporary data directories, write-through persistence, and the same
CLI knobs as volatile Store benchmarks.

```bash
./scripts/dev.sh benchmark-durable --ops 20000 --repeats 3
./scripts/dev.sh benchmark --filter store-durable-periodic-read-after-write --ops 50000
./scripts/dev.sh benchmark --filter store-durable-read-after-write --ops 50000
./scripts/dev.sh benchmark --filter store-durable-parallel-all --workers 4 --threads 4 \
    --distribution worker-affine
./scripts/dev.sh benchmark --filter store-durable-group-parallel-put --ops 4096 \
    --workers 1 --threads 32 --distribution single-worker --latency
```

`--latency` is available for durable parallel-put filters and records per-request p50, p95, p99,
and p99.9 latency with `steady_clock`. Leave it disabled for throughput-only regression comparisons
that must avoid per-operation timing overhead.

The [benchmark workflow](https://github.com/gpicchiarelli/GlyphaStore/actions/workflows/benchmarks.yml)
runs a fixed Release suite on pushes to `main`, every Monday, and on manual dispatch. Every run
publishes a Markdown summary plus raw text, runner metadata, and machine-readable JSON as a
90-day artifact. When available, it also compares throughput with the previous successful run on
`main`. GitHub-hosted runner measurements are regression signals, not absolute performance claims;
publishable numbers still require controlled hardware.

## Experimental TCP daemon

`glyphastored` is the native non-blocking TCP server bootstrap. Each executor pairs one Reactor
with one Worker: `kqueue` on macOS/FreeBSD/OpenBSD and edge-triggered `epoll` on Linux. The protocol
implements pipelined `INIT`, `BIND_WORKER`, `PING`, `GET`, `PUT`, and `ERASE`. Binding moves
ownership of the socket reference and buffered state once to the selected Reactor. Store
operations then execute only on that Worker; a mismatched key receives `WRONG_OWNER` and is never
forwarded internally.

The TCP benchmark runs the real binary protocol over loopback with validated pipelined responses:

```bash
./scripts/dev.sh benchmark-server --ops 100000 --workers 4 --clients 4 \
  --pipeline 32 --executor-affinity --warmup 1 --repeats 5
```

Its output includes exact timed ingress and egress frame bytes, ingress/egress/duplex bandwidth,
current RSS, RSS growth from the post-setup baseline, and peak process RSS. `INIT`, `BIND_WORKER`,
connection setup, and client-thread creation remain outside the timed region.
Add `--latency` to record pipelined response latency from batch submission through each validated
response, including p50, p95, p99, and p99.9. Latency collection is opt-in so its per-response
clock reads do not distort throughput-only runs. The automated workflow exercises 1, 2, and 4
workers at pipeline depths 1, 8, 32, and 128, plus a dedicated latency run.

```bash
./scripts/dev.sh build
./build/macos-debug/glyphastored --bind 127.0.0.1 --port 7379
```

See the [server model](docs/architecture/server-model.md) for framing and concurrency invariants.
Command-line conventions, exit codes, signals, and operational examples are documented in the
[CLI reference](docs/cli.md).

## Documentation

| Read | Purpose |
| --- | --- |
| [Architecture charter](docs/architecture/architecture-charter.md) | Fixed decisions, scope, and performance contract |
| [Storage model](docs/architecture/storage-model.md) | Segments, Records, visibility, recovery, and complexity |
| [Durability and recovery](docs/architecture/durability-recovery.md) | Alpha commit point, manifest ordering, crash states, and recovery |
| [Durable Segment files](docs/architecture/segment-filesystem.md) | Platform allocation, alternate commit slots, and bounded recovery scan |
| [Recovery implementation](docs/architecture/recovery-implementation.md) | Manifest validation, partitioned Index rebuild, and sequence restoration |
| [Namespace audit](docs/architecture/namespace-policy.md) | Descriptor-relative enumeration, orphan/temporary policy, and fail-closed limits |
| [Durable runtime catalog](docs/architecture/durable-runtime-catalog.md) | Bounded file handles, verified reads, and sticky fail-closed state |
| [Crash-safe durable compaction](docs/architecture/durable-compaction.md) | Whole-Worker sealed-history replacement and retirement protocol |
| [Public API contract](docs/architecture/public-api-contract.md) | Supported surface, read ownership, errors, and compatibility |
| [Index model](docs/architecture/index-model.md) | Exact-key lookup and SwissTable-oriented design |
| [Worker model](docs/architecture/worker-model.md) | Ownership, routing, topology, and concurrency |
| [Server model](docs/architecture/server-model.md) | Reactors, protocol framing, handoff, and backpressure |
| [Vacuum model](docs/architecture/vacuum-model.md) | Copy-build-validate publication and reclamation |
| [Development guide](docs/development.md) | Portable workflow and automated benchmark reports |
| [Production readiness](docs/production-readiness.md) | Explicit gates from prototype to stable release |
| [Persistence v1 production roadmap](docs/v1-production-roadmap.md) | Repository-wide audit, priorities, and acceptance criteria |

## Supported platforms

- macOS on Apple Silicon is the primary development platform; macOS Intel remains a target.
- Linux and macOS are built and tested in CI.
- Linux, macOS, FreeBSD, and OpenBSD are architectural targets since `0.1.0`.
- FreeBSD and OpenBSD use the `kqueue` backend but do not yet have native CI runners; their badges
  describe target status, not verified release support.

## Security

Read the [security policy](SECURITY.md) before reporting a vulnerability. Do not disclose suspected
security issues through public issues or pull requests. Persisted bytes are treated as untrusted
input and all encoded lengths and offsets must be validated.

## Contributing

This repository is initially private. Development rules and review expectations are in
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

BSD-3-Clause. See [LICENSE](LICENSE).
