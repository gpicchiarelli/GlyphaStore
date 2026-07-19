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

This first package surface is deliberately synchronous. An `asyncio` implementation will share the
codec and semantic tests but use one ordered task per Worker rather than wrapping this client in a
thread pool.
