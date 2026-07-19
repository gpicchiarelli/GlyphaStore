# GlyphaStore Perl client

Native synchronous Perl client for GlyphaStore wire protocol v2. It uses only core modules, keeps
one bound TCP connection per Worker, supports arbitrary byte strings, retries safe reads (including
transient `unavailable` reconnects while routing metadata is stable), and preserves indeterminate
mutation outcomes.

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

Ordered, non-atomic pipelines use hash references and return one positional result per request:

```perl
my $responses = $cache->execute_pipeline([
    { opcode => 'put', key => 'key', value => 'value' },
    { opcode => 'get', key => 'key' },
]);
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

## Performance benchmark

```bash
perl benchmarks/client_benchmark.pl --port 7379 --workers 1 \
  --ops 100000 --pipeline 128 --warmup 1 --repeats 7
```

With multiple Workers the harness defaults to overlapping pipelines via
`execute_worker_pipelines` (`--no-concurrent` forces the old sequential drain).

The client keeps pure-Perl framing but hot paths use native `pack 'Q<'`, avoid
copying octet strings, wrap FNV-1a in 64-bit integer arithmetic, and reuse
`IO::Select` / in-place `sysread` buffers.
