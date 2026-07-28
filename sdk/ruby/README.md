# GlyphaStore Ruby client

Native Ruby client for GlyphaStore wire protocol v2. One TCP connection per Worker
(`TCP_NODELAY`), canonical FNV-1a routing, at-most-one automatic retry for safe reads /
zero-byte mutations, and `committed` / `rejected` / `indeterminate` outcomes.

Implements [client semantics v1](../../docs/spec/client-semantics-v1.md). Roadmap:
[Ruby SDK roadmap](../../docs/architecture/ruby-sdk-roadmap.md).

Current routing limit: the Ruby client accepts the plain FNV `GlyphaStore/2` identity only. It fails
closed on the keyed SipHash `INIT` extension used by `--secure-profile`; see the shared
[SDK roadmap](../../docs/architecture/sdk-roadmap.md).

**Gem:** `glyphastore` · **Module:** `GlyphaStore` · **Ruby:** ≥ 3.2 · **License:** BSD-3-Clause

Cleartext TCP by default: treat the server as loopback / private network / sidecar. Opt-in TLS 1.3
via `ClientConfig#tls` (CA / mTLS / hostname verify) matches Go/C++/Python/Perl/Erlang (ADR 0020).
See the [security roadmap](../../docs/security/roadmap.md).

```ruby
require "glypha_store"

config = GlyphaStore::ClientConfig.defaults
config.port = 7379
# Opt-in TLS 1.3 (fail closed; no cleartext fallback):
# config.tls = true
# config.tls_ca = "/path/to/ca.pem"
# config.server_name = "glyphastore.example"
# config.cert_file / config.key_file for mTLS
client = GlyphaStore::Client.connect(config)

result = client.put("session\x0042".b, "payload".b)
raise result.error unless result.committed?

value = client.get("session\x0042".b)
client.close
```

## Async (optional)

```ruby
# gem install async
require "glypha_store/async_client"

Async do
  client = GlyphaStore::AsyncClient.connect(config)
  client.get("key".b)
  client.close
end
```

Cancellation / task stop poisons the in-flight Worker connection (client-semantics §6.3).

## Concurrency

| Environment | Rule |
| --- | --- |
| MRI threads | One sync `Client` may be shared; each Worker connection is mutex-protected. |
| `fork` (Puma clustered, Unicorn) | Construct a **new** client in the child. Never reuse parent sockets. |
| Async / Fiber | Use `GlyphaStore::AsyncClient` inside an `Async` reactor (`async` gem). |

## Install (from this tree)

```bash
cd sdk/ruby
ruby -Ilib -e 'require "glypha_store"; puts GlyphaStore::VERSION'
./scripts/test-ruby-client.sh   # from repo root
```

## Layout

| Path | Role |
| --- | --- |
| `lib/glypha_store/protocol.rb` | Wire codec + FNV routing |
| `lib/glypha_store/error.rb` | Structured errors + outcome types |
| `lib/glypha_store/client.rb` | Sync TCP/TLS client |
| `lib/glypha_store/tls.rb` | TLS 1.3 context + wrap helpers |
| `exe/glyphastore-interop` | Interop CLI for `scripts/test-sdk-interop.sh` |
| `test/fixtures/` | Vendored wire goldens |

## Performance

Prefer deep pipelines and `execute_batch` so Workers overlap. Scale out with one client per
prefork worker process. Publish loopback numbers with:

```bash
./scripts/benchmark_ruby_client.sh
```

Optional C extension for framing remains roadmap Phase 2.6 (measure first).
