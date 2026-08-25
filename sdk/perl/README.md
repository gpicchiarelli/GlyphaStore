# GlyphaStore Perl client

Native synchronous Perl client for GlyphaStore wire protocol v2. It uses only core modules, keeps
one bound TCP connection per Worker, supports arbitrary byte strings, retries safe reads (including
transient `unavailable` reconnects while routing metadata is stable), and preserves indeterminate
mutation outcomes. Portable error/retry/deadline rules:
[client semantics v1](../../docs/spec/client-semantics-v1.md).

Worker routing follows ADR 0030: plain `GlyphaStore/2` is FNV-1a; the extended INIT identity selects SipHash-2-4.

**Security posture:** cleartext TCP by default (no authentication). Opt-in TLS 1.3 via
`tls => 1` with `tls_ca` / `ca_file`, `cert_file` / `key_file` (mTLS), `server_name`, and
`insecure_skip_verify` (lab only). Requires `IO::Socket::SSL`; if missing, TLS requests fail closed.
Hostname/SNI verification is on by default. Use loopback / private network / sidecar for cleartext
([security roadmap](../../docs/security/roadmap.md); OpenBSD uses LibreSSL on the daemon).

License: BSD-3-Clause. Requires Perl ≥ 5.32.

### Thread and fork contract

`GlyphaStore::Client` is **not** shareable across ithreads and has no internal mutexes. Do not use
sockets created before `fork` in the child; open a new client per process (and per thread if you
use threads). Prefer `execute_worker_pipelines` to overlap Workers inside one process.

```perl
use GlyphaStore::Client;

my $cache = GlyphaStore::Client->connect(host => '127.0.0.1', port => 7379);
my $stored = $cache->put("session\x00key", "payload");
my $value = $cache->get("session\x00key") if $stored->{outcome} eq 'committed';
$cache->close;
```

Ordered, non-atomic pipelines use hash references and return one positional result per request.
`execute_batch` groups by Worker, overlaps per-Worker pipelines, and restores caller order (not a
transaction). Prefer `execute_worker_pipelines` when you already have per-Worker vectors.

```perl
my $responses = $cache->execute_pipeline([
    { opcode => 'put', key => 'key', value => 'value' },
    { opcode => 'get', key => 'key' },
]);
my $ordered = $cache->execute_batch(\@mixed_worker_requests);
```

## Install

From CPAN (once published):

```bash
cpanm GlyphaStore
```

From this source tree:

```bash
cd sdk/perl && perl Makefile.PL && make && make test && make install
```

## Tests and packaging

```bash
./scripts/test-perl-client.sh
./scripts/package-perl-client.sh
```

`./scripts/test-perl-client.sh` runs **Perl::Critic at severity 1** (brutal) against
`lib/` using `.perlcriticrc`, then the unit tests. Install the develop tools with
`cpanm --installdeps --with-develop .` from `sdk/perl/`, or `cpanm Perl::Critic Perl::Tidy`.

`GlyphaStore::Protocol` exposes the full bidirectional codec and FNV-1a Worker routing. See
[PACKAGING.md](PACKAGING.md) for PAUSE/MetaCPAN upload steps.

## Performance model

The client is pure Perl, but its design is deliberately pipeline-first rather than a sequence of
object-heavy synchronous calls:

- one TCP connection is created and bound per Worker;
- a Worker pipeline is encoded into one contiguous scalar and drained under one absolute deadline;
- response bytes accumulate in a reusable connection buffer and multiple complete frames are parsed
  from each `sysread`;
- pipeline result slots are materialized once, at their decided success or failure outcome, rather
  than allocating placeholder failure hashes that the success path immediately replaces;
- `execute_worker_pipelines` drives all active Worker sockets through one `IO::Select` loop;
- `execute_batch` hashes each key exactly once while grouping, reuses that validated ownership while
  encoding, overlaps the Worker pipelines, then restores caller order.
- Worker routing reuses the immutable identity validated during `INIT`; arbitrary routing hashes
  passed to the public protocol helpers are still normalized and checked on every call.

Those choices reduce syscalls and expose server parallelism. They do **not** make the current Perl
path allocation-free: public requests and results are hash references, encoded frames are assembled
for each call, and successful GET values become owned Perl scalars. These are measurable costs and
must not be described as negligible without a profile.

For a throughput-sensitive application:

- send ordered pipelines instead of alternating synchronous `put`/`get` calls;
- use `execute_batch` for mixed owners or `execute_worker_pipelines` when work is already sharded;
- create the client after `fork` and use one client per prefork process; do not share it across
  ithreads;
- choose pipeline depth from a throughput **and** tail-latency sweep on the deployment workload.

Optimization work follows measured cost, in this order: Perl-level allocation/copy reduction,
buffer reuse and parser shape; syscall and scheduling behavior; then an optional narrow XS codec or
routing kernel if profiles still place CPU there. XS is a possible tool, not a pre-decided answer.
Wrapping the C++ client through FFI is outside the current native-SDK design.

`connections_per_worker` and an event-loop adapter remain measurement-gated. More connections may
help hot-Worker concurrency while increasing ordering, memory and reconnect complexity; an async
adapter may improve application concurrency without improving single-pipeline CPU throughput.

```bash
# Sequential and concurrent matrix from the repository root:
./scripts/benchmark_perl_client.sh
# Set WORKER_HASH_SEED to run the same matrix with the server's keyed SipHash routing.

# One configuration (default overlaps Workers when workers > 1):
perl sdk/perl/benchmarks/client_benchmark.pl --port 7379 --workers 4 \
  --ops 100000 --pipeline 128 --warmup 1 --repeats 7
# --no-concurrent forces sequential drain across Workers
# --batch measures mixed-owner execute_batch grouping and ordered results
```

Compare SDKs only through `./scripts/benchmark_sdk_clients.sh`, which fixes the validated workload
and result semantics. Do not promote a single local run or an old cross-runtime ratio into a client
limit.
