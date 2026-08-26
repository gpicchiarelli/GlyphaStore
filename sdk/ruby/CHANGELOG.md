# Changelog

## Unreleased

- Make the interop CLI assert the structured `permission_denied` mutation outcome used by the
  all-SDK secure-profile prefix matrix.

## 0.1.0

- Initial sync client: protocol v2 codec, structured errors, pipelines, batch, monotonic
  deadlines, per-call `timeout:`, interop CLI, golden fixtures.
- Phase 2: `AsyncClient` (optional `async` gem), encode scratch reuse, published benchmark harness.
- Phase 3.1: opt-in TLS 1.3 (`ClientConfig#tls`, `tls_ca`, mTLS `cert_file`/`key_file`,
  `server_name`, `insecure_skip_verify` lab escape); fail closed; interop CLI TLS flags; sync/async
  ping coverage.
