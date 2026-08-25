# Changelog

## 0.1.0

- Initial synchronous Go client: wire protocol v2 codec, structured errors, pipelines, batch,
  monotonic deadlines, per-call timeouts, interop CLI, and golden fixtures.
- `client.Version` exported for packaging and lockstep checks.
- Packaging tests a tracked-file module snapshot and an external consumer before tagging.
