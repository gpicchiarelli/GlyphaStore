# GlyphaStore Perl client

Native synchronous Perl client for GlyphaStore wire protocol v2. It uses only core modules, keeps
one bound TCP connection per Worker, supports arbitrary byte strings, retries safe reads (including
transient `unavailable` reconnects while routing metadata is stable), and preserves indeterminate
mutation outcomes.

License: BSD-3-Clause. Requires Perl ≥ 5.32.

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

`GlyphaStore::Protocol` exposes the full bidirectional codec and FNV-1a Worker routing. See
[PACKAGING.md](PACKAGING.md) for PAUSE/MetaCPAN upload steps.

## Performance benchmark

```bash
perl benchmarks/client_benchmark.pl --port 7379 --workers 1 \
  --ops 100000 --pipeline 128 --warmup 1 --repeats 7
```

Perl ithreads clone interpreter state and are not a shared-client concurrency mechanism. Use one
client per process/thread, or an event-loop adapter, when driving several Workers concurrently.
