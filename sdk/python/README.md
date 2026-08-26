# GlyphaStore Python client

Pure-Python client for GlyphaStore wire protocol v2. It opens and binds one TCP connection per
Worker, routes binary keys with canonical FNV-1a 64-bit, retries reads after a transient
disconnect, and never reports an uncertain mutation as rejected.

The package ships both a thread-safe synchronous `Client` and an `asyncio` `AsyncClient` that share
the same codec, configuration, and outcome model. Runtime dependency: none (Python ≥ 3.11 stdlib).
Portable error/retry/deadline rules:
[client semantics v1](../../docs/spec/client-semantics-v1.md).

Worker routing follows ADR 0030: plain `GlyphaStore/2` is FNV-1a; the extended INIT identity selects SipHash-2-4.

**Security posture:** cleartext TCP by default (no authentication). Opt-in TLS 1.3 via
`ClientConfig(tls=True, tls_ca=..., cert_file=..., key_file=..., server_name=...,
insecure_skip_verify=...)` on both `Client` and `AsyncClient` — hostname/SNI verification on by
default, fail closed (no cleartext fallback). Use loopback / private network / sidecar for
cleartext; mTLS authn is ADR 0021
([security roadmap](../../docs/security/roadmap.md); OpenBSD uses LibreSSL on the daemon).

License: BSD-3-Clause.

```python
from glyphastore import Client

with Client.connect() as cache:
    stored = cache.put(b"session:42", b"payload")
    if stored.committed:
        print(cache.get(b"session:42"))
```

```python
from glyphastore import AsyncClient, PipelineOpcode, PipelineRequest

async with await AsyncClient.connect() as cache:
    responses = await cache.execute_pipeline([
        PipelineRequest(PipelineOpcode.PUT, b"key", b"value"),
        PipelineRequest(PipelineOpcode.GET, b"key"),
    ])
    assert all(response.succeeded for response in responses)
    # Multi-Worker: groups by Worker, overlaps pipelines, restores caller order.
    ordered = await cache.execute_batch([
        PipelineRequest(PipelineOpcode.PUT, b"a", b"1"),
        PipelineRequest(PipelineOpcode.PUT, b"b", b"2"),
        PipelineRequest(PipelineOpcode.GET, b"a"),
        PipelineRequest(PipelineOpcode.GET, b"b"),
    ])
```

## Online backup

`Client.backup(destination)` and `await AsyncClient.backup(destination)` issue wire `BACKUP` to
create an online fenced durable catalog copy in a new empty server-side path. They return the bounded
ASCII report as `bytes`. This is an admin operation under secure authz and is not a zero-fence hot
snapshot; ambiguous transport/`INTERNAL_ERROR` outcomes require reconciliation, not blind retry.

## Install

From PyPI (once published):

```bash
pip install glyphastore
```

From this source tree:

```bash
pip install ./sdk/python
```

## Tests

```bash
./scripts/test-python-client.sh
```

Packaging verification (sdist, wheel, `twine check`, isolated install tests for both artifacts):

```bash
./scripts/package-python-client.sh
```

See [PACKAGING.md](PACKAGING.md) for TestPyPI/PyPI upload steps and the version-bump checklist.

## Performance benchmark

Start `glyphastored`, then run the same ordered `PUT`/`GET` workload used by the C++ client
benchmark:

```bash
python3 benchmarks/client_benchmark.py --port 7379 --workers 4 \
  --ops 100000 --pipeline 128 --warmup 1 --repeats 7
```

Use `--execution batch` to benchmark `execute_batch()` grouping and fan-out directly, and
`--runtime async --execution batch` for `AsyncClient`. Benchmark keys are assigned with the
routing identity negotiated from the running server, including keyed SipHash configurations.
