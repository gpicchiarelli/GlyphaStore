# GlyphaStore Perl client

Native synchronous Perl client for GlyphaStore wire protocol v2. It uses only core modules, keeps
one bound TCP connection per Worker, supports arbitrary byte strings, retries safe reads (including
transient `unavailable` reconnects while routing metadata is stable), and preserves indeterminate
mutation outcomes. Portable error/retry/deadline rules:
[client semantics v1](../../docs/spec/client-semantics-v1.md).

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

## Performance

### Production (what actually scales)

- Prefer deep ordered pipelines and `execute_worker_pipelines` / `execute_batch` so Workers overlap
  inside one process.
- Run **one client per process** (Hypnotoad / prefork). Do not share a client across ithreads; that
  is the documented contract. Process count is the parallel scale-out knob.
- An event-loop adapter (Mojolicious / `IO::Async` / AnyEvent) is a later roadmap item: it raises
  web-app concurrency by not blocking the reactor, not the sync microbench ops/s number.

### Microbench vs Python

The pure-Perl hot path is already tight (`pack 'Q<'`, zero-copy octets, integer FNV, in-place
`sysread`, reused `IO::Select`, `encode_request_hot`). The remaining gap versus Python sequential at
deep pipelines (~2–2.5× in the 0.1.0 sequential baseline) is not closed by further generic
micro-tuning. Compare Perl concurrent (`workers>1`, default harness) to a fair peer; the published
0.1.0 Perl rows were largely sequential.

The only large remaining SDK-side leap is optional **XS on encode/decode/FNV** (heavier packaging).
FFI wrapping the C++ client is out of design. Secondary wins (fewer hashrefs, less copying) are
typically single-digit percent.

`connections_per_worker` waits on measurement, same as the other SDKs. Suite 0.2 should add p50/p95
latency so bottlenecks are visible.

```bash
# published sequential + concurrent matrix:
./scripts/benchmark_perl_client.sh

# one config (default overlaps Workers when workers>1):
perl benchmarks/client_benchmark.pl --port 7379 --workers 4 \
  --ops 100000 --pipeline 128 --warmup 1 --repeats 7
# --no-concurrent forces sequential drain across Workers
```
