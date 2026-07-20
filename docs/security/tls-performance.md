Status: descriptive  
Applies to: secure-profile TLS performance interpretation (not a wire or disk contract)  
Owner: security maintainers  
Last reviewed: 2026-07-20

# TLS performance note (Phase 2.5)

Secure-profile TLS 1.3 (ADR 0020) adds confidentiality and integrity on the outer transport. This
note records how to measure the **TLS tax** on the same Go client pipeline harness used for
cleartext TCP benches, and what a representative local run showed.

## Method

Use `scripts/benchmark_tls_tax.sh`. It:

1. Starts a volatile `glyphastored` on loopback in cleartext mode and again in TLS-only mode
   (ephemeral self-signed cert with SAN `DNS:localhost` / `IP:127.0.0.1`).
2. Runs `sdk/go/cmd/glyphastore-bench` with identical `--ops`, `--pipeline`, `--workers`,
   `--execution concurrent`, warmup, and repeats.
3. Writes per-cell outputs under `cleartext/` and `tls/`, plus a Markdown summary with
   `TLS/cleartext` median ops/s ratios.

Flags on the Go bench (same semantics as interop): `--tls`, `--tls-ca`, `--server-name`, optional
mTLS `--tls-cert`/`--tls-key`, and `--insecure-skip-verify` for lab-only escapes.

```bash
# Requires a TLS-capable glyphastored (GLYPHASTORE_ENABLE_TLS + LibreSSL/OpenSSL).
OPS=50000 WARMUP=1 REPEATS=5 ./scripts/benchmark_tls_tax.sh
```

Do not treat hosted CI runners as absolute throughput evidence. Prefer a quiet, thermally stable
developer host before publishing absolute numbers.

## Representative local result (macOS, OpenSSL 3.x daemon)

Captured 2026-07-20 on Apple Silicon against a Debug/Release TLS-capable `glyphastored` (OpenSSL
backend), volatile storage, Go SDK 0.1.0, `OPS=20000`, `REPEATS=3`, concurrent execution:

| workers | pipeline | cleartext median ops/s | TLS median ops/s | TLS/cleartext |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 38,265 | 37,509 | 0.980 |
| 1 | 32 | 94,338 | 90,562 | 0.960 |
| 1 | 128 | 98,799 | 99,569 | 1.008 |
| 4 | 1 | 113,544 | 102,713 | 0.905 |
| 4 | 32 | 360,342 | 369,840 | 1.026 |
| 4 | 128 | 394,905 | 377,655 | 0.956 |

Ratios near 1.0 (and occasional TLS ≥ cleartext within noise) are expected on a warm loopback
connection with deep pipelines. The largest relative tax here was about **10%** at `workers=4`,
`pipeline=1`. Re-run with higher `OPS`/`REPEATS` on a quiet host before publishing capacity claims.

## Interpretation

- **Shallow pipelines (p=1)** usually show the largest relative TLS tax: per-pair handshake/record
  overhead and syscall boundaries dominate when the engine is not amortizing work across a deep
  pipeline.
- **Deep pipelines (p=32–128)** typically recover most cleartext throughput. Record-layer cost is
  amortized across many framed ops on a warm connection; the engine and SDK remain the bottleneck
  more often than crypto.
- **TLS is not free, but it is usually affordable** for the session/cache/gateway patterns in
  [where-performance-matters.md](../architecture/where-performance-matters.md). Prefer measuring
  your own loopback baseline before sizing hardware for a secure-profile deployment.
- **OpenBSD / LibreSSL** must be re-measured on a native host (or the OpenBSD CI VM for smoke only).
  Do not extrapolate macOS/OpenSSL ratios to OpenBSD; `fdatasync`≡`fsync` and LibreSSL performance
  characteristics differ. CI gate: `.github/workflows/openbsd-libressl.yml` +
  `scripts/ci-openbsd-libressl.sh` (correctness, not throughput).

## Non-goals

- This note does not change wire protocol v2, client semantics, or durability contracts.
- It does not authorize weakening TLS 1.3-only policy, hostname verification defaults, or
  fail-closed secure-profile configuration for speed.
- Absolute ops/s from CI VMs are informational only.

## Related

- [Security roadmap](roadmap.md) Phase 2.5 / 2.6
- [ADR 0020](../adr/0020-tls-outer-transport.md)
- [Development / benchmark reports](../development.md)
- `scripts/benchmark_go_client.sh` (cleartext baseline matrix)
