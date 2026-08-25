# Changelog

## Unreleased

- Route the first pipeline key once instead of hashing it once for initialization and again during
  ownership validation; subsequent keys retain the same single-Worker check.
- Add a mixed-owner `ExecuteBatch` benchmark mode and generate keys with the routing identity
  negotiated from the server, including keyed SipHash configurations.

## 0.1.0

- Initial synchronous Go client: wire protocol v2 codec, structured errors, pipelines, batch,
  monotonic deadlines, per-call timeouts, interop CLI, and golden fixtures.
- `client.Version` exported for packaging and lockstep checks.
- Packaging tests a tracked-file module snapshot and an external consumer before tagging.
