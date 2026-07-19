# GlyphaStore Python client

Pure-Python client for GlyphaStore wire protocol v2. It opens and binds one TCP connection per
Worker, routes binary keys with canonical FNV-1a 64-bit, retries reads after a transient
disconnect, and never reports an uncertain mutation as rejected.

The package ships both a thread-safe synchronous `Client` and an `asyncio` `AsyncClient` that share
the same codec, configuration, and outcome model. Runtime dependency: none (Python ≥ 3.11 stdlib).
Portable error/retry/deadline rules:
[client semantics v1](../../docs/spec/client-semantics-v1.md).

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

Packaging verification (sdist, wheel, `twine check`, install-from-wheel tests):

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
