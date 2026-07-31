# Secure profile certificate rotation and revocation

Status: descriptive operator runbook  
Applies to: `glyphastored` secure profile (mTLS + `--authz-map`)  
Owner: security maintainers  
Last reviewed: 2026-07-28

> [!NOTE]
> Official SDKs parse the keyed SipHash Worker-routing `INIT` identity selected by
> `--secure-profile` ([ADR 0030](../adr/0030-keyed-worker-routing.md)). Smoke:
> `scripts/test-secure-profile-interop.sh` (mTLS, authz, prefix, CRL, quotas, keyed routing;
> cpp/python/go plus perl/ruby/erlang when toolchains are present). See the
> [SDK roadmap](../architecture/sdk-roadmap.md).

## Rotate server or client CA material

1. Issue replacement PEMs (server cert/key, client CA, client cert/key) with short lifetimes.  
2. Install files with mode `0600` for private keys; keep paths stable when possible.  
3. Recycle the daemon (or restart the listener process) so `TlsContext` reloads PEMs. There is no
   in-band credential opcode.  
4. Confirm `--dump-config` still shows path-only TLS fields and that `--log-format json` (or
   `--secure-profile`) emits `auth` accept events for healthy clients.  
5. Roll client SDK `cert_file` / `key_file` / `ca_file` in the same release window.

## Revoke a principal

Preferred, immediate control plane:

1. Remove the principal line from `--authz-map` (or replace the map file) and recycle the daemon.  
2. Data-plane opcodes for that principal return wire `PERMISSION_DENIED` (8). Lifecycle
   `HEALTH`/`READY` still require a verified mTLS principal.

Cryptographic revocation (fail closed):

1. Publish an updated PEM CRL that lists the revoked serial.  
2. Point the daemon at it with `--tls-crl /path/clients.crl`.  
3. Optionally set `--tls-ocsp-fail-closed` to refuse startup unless `--tls-crl` is also set (live AIA
   OCSP HTTP lookups are **unsupported** — they conflict with OpenBSD `pledge` without `dns`).  
4. Recycle the daemon. Revoked peers fail the handshake; JSON audit emits
   `{"event":"auth","decision":"deny",...}` and `STATS` increments `auth_denies`.

## Incident response (auth)

| Signal | Action |
| --- | --- |
| Spike in `auth_denies` / `tls_errors` | Check CRL freshness, client CA trust, clock skew |
| Spike in `authz_denies` | Confirm map contents; look for principal typos (exact SAN/CN match) |
| Compromised client key | Revoke serial via CRL **and** remove principal from authz map |

## Residual honesty

- Multi-tenant adversarial isolation remains Phase 8.  
- Physical E3/E4 power-loss certification remains open.  
- Live OCSP HTTP is intentionally out of scope; CRL is the supported fail-closed revocation path.
