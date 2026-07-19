# GlyphaStore Python client

Pure-Python, synchronous client for GlyphaStore wire protocol v2. It opens and binds one TCP
connection per Worker, routes binary keys with canonical FNV-1a 64-bit, retries reads after a
transient disconnect, and never reports an uncertain mutation as rejected.

```python
from glyphastore import Client

with Client.connect() as cache:
    stored = cache.put(b"session:42", b"payload")
    if stored.committed:
        print(cache.get(b"session:42"))
```

For throughput-sensitive paths, submit an ordered, non-atomic pipeline whose keys route to one
Worker:

```python
from glyphastore import PipelineOpcode, PipelineRequest

responses = cache.execute_pipeline([
    PipelineRequest(PipelineOpcode.PUT, b"key", b"value"),
    PipelineRequest(PipelineOpcode.GET, b"key"),
])
assert all(response.succeeded for response in responses)
```

This first package surface is deliberately synchronous. An `asyncio` implementation will share the
codec and semantic tests but use one ordered task per Worker rather than wrapping this client in a
thread pool.

## Performance benchmark

Start `glyphastored`, then run the same ordered `PUT`/`GET` workload used by the C++ client benchmark:

```bash
python3 benchmarks/client_benchmark.py --port 7379 --workers 4 \
  --ops 100000 --pipeline 128 --warmup 1 --repeats 7
```

The benchmark prepares keys and request objects before timing, validates every response and reports
operations per second over both PUT and GET frames. It intentionally includes Python request
encoding and owned response construction, just as an application experiences them.
