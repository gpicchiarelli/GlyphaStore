# Changelog

## Unreleased

- Reuse each batch request's validated Worker assignment while encoding its per-Worker pipeline,
  avoiding a second routing hash in both synchronous and asyncio clients.
- Add direct sync/async batch benchmark modes using the routing identity negotiated from the server.

## 0.1.0 — 2026-07-19

- Initial PyPI-ready release of the native wire-protocol v2 client.
- Synchronous `Client` and asyncio `AsyncClient` with one bound TCP connection per Worker.
- Full request/response codec, FNV-1a routing, ordered pipelines, and indeterminate mutation outcomes.
- Packaging installs wheel and normalized sdist independently and runs conformance against both.
- Async cancellation lifecycle tests close transports locally and treat resource leaks as errors.
