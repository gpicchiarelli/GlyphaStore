# Changelog

## Unreleased

- Make the interop CLI assert structured `permission_denied` and `overloaded` mutation outcomes in
  the all-SDK secure-profile prefix/quota matrix, with a bounded burst for quota exhaustion.
- Load `glypha_store` through the gem load path and verify the installed interop executable, so the
  packaged CLI cannot accidentally depend on the source-tree `lib/` layout.

## 0.1.0

- Initial sync client: protocol v2 codec, structured errors, pipelines, batch, monotonic
  deadlines, per-call `timeout:`, interop CLI, golden fixtures.
- Phase 2: `AsyncClient` (optional `async` gem), encode scratch reuse, published benchmark harness.
- Phase 3.1: opt-in TLS 1.3 (`ClientConfig#tls`, `tls_ca`, mTLS `cert_file`/`key_file`,
  `server_name`, `insecure_skip_verify` lab escape); fail closed; interop CLI TLS flags; sync/async
  ping coverage.
