# Secure profile reference

Status: normative for the secure daemon profile (ADR 0020–0022)  
Applies to: `glyphastored` TLS/mTLS/authz; official SDK TLS credential options  
Owner: security maintainers  
Last reviewed: 2026-07-25

This document freezes the **secure profile** operators enable before binding beyond a trusted
loopback/private perimeter. Protocol v2 framing is unchanged; TLS is an outer transport
([ADR 0020](../adr/0020-tls-outer-transport.md)).

## 1. Profile definition

| Requirement | Mechanism |
| --- | --- |
| Confidentiality / integrity | TLS 1.3 only (`--tls-cert` / `--tls-key`) |
| Authentication | Mutual TLS (`--tls-client-ca`); anonymous peers rejected |
| Authorization | Static principal capability map (`--authz-map`); default deny |
| Abuse / DoS bounds | Phase 5 accept/connection/principal rate limits + idle/request deadlines |
| Fail closed | `--secure-profile` refuses dual cleartext+TLS (`--tls-port`), missing mTLS, missing authz map, or disabled Phase 5 limits |

Trusted cleartext (default loopback) remains valid **without** this profile. The secure profile is
opt-in.

### TLS 1.3 cipher policy (Phase 2)

Secure-profile listeners and official TLS clients pin TLS 1.3 AEAD suites only:

- `TLS_AES_128_GCM_SHA256`
- `TLS_AES_256_GCM_SHA384`
- `TLS_CHACHA20_POLY1305_SHA256`

TLS 1.2 and earlier are rejected at protocol version policy. Hostname/CA verification is on by
default for SDK connects; lab-only insecure escapes remain explicit.

```bash
glyphastored --secure-profile \
  --bind 127.0.0.1 --port 7379 \
  --tls-cert /etc/glyphastore/server.crt \
  --tls-key /etc/glyphastore/server.key \
  --tls-client-ca /etc/glyphastore/clients-ca.crt \
  --authz-map /etc/glyphastore/authz.map
```

Non-loopback bind without `--secure-profile` prints a warning. Leaving the trusted perimeter requires
`--secure-profile` (Phases 2–5). Hostile public Internet still needs Phase 6 audit polish and an
explicit CRL/OCSP policy ([security roadmap](roadmap.md)).

## 1a. Phase 5 abuse controls

| Flag | Secure-profile default | Effect |
| --- | --- | --- |
| `--max-accepts-per-sec` | 128 | Process-wide accept/handshake admissions per second; excess peers are dropped |
| `--idle-timeout-ms` | 60000 | Close quiet connections (monotonic idle) |
| `--request-timeout-ms` | 30000 | Bound partial-frame assembly and in-flight response wait; Store work already executing is not cancelled |
| `--connection-max-requests-per-sec` | 256 | Per-connection request admission; exceed ⇒ wire `overloaded` |
| `--principal-max-requests-per-sec` | 1024 | Per-mTLS-principal request admission |
| `--principal-max-bytes-per-sec` | 32MiB | Per-principal request key+value + response value bytes |

Trusted cleartext leaves these at `0` (disabled). `--secure-profile` fills zeros with the defaults
above and refuses explicit `0`. `HEALTH` / `READY` / `STATS` stay exempt from request/bandwidth
quotas. `STATS` exports `abuse_*` reject/close counters.

## 2. Principal extraction (ADR 0021)

After a successful mTLS handshake, the daemon derives a stable **principal id** from the verified
client certificate. First match wins:

1. First URI SAN (`subjectAltName` uniformResourceIdentifier), e.g. SPIFFE ID  
2. Else first DNS SAN  
3. Else subject Common Name (CN)

If none are present, the connection is rejected (fail closed). The principal string is the raw SAN
or CN value (no type prefix). Operators must put that exact string in `--authz-map`.

Credential material never appears in logs, `--dump-config` (paths only), or client `Error` messages.

## 3. Capability map (ADR 0022)

File format (UTF-8 text):

```text
# principal capability[,capability...]
reader.example read
writer.example write
admin.example admin
spiffe://cluster.local/ns/app/sa/store-writer write
```

| Capability | Allows |
| --- | --- |
| `read` | `GET`, `PING`, `STATS` |
| `write` | `PUT`, `ERASE` (implies `read`) |
| `admin` | Reserved admin surface; **implies `write` ⇒ `read`** for v1 convenience |

Rules:

- Missing principal ⇒ deny data-plane opcodes with wire `PERMISSION_DENIED` (8).  
- Empty map with authz enabled ⇒ nobody can access the data plane.  
- `INIT`, `BIND_WORKER`, `HEALTH`, and `READY` need a verified mTLS principal only (any mapped or
  unmapped authenticated peer may bootstrap / probe liveness).  
- Namespace/prefix scope is out of scope for v1.

## 4. Wire status

`PERMISSION_DENIED` (value **8**) is additive within protocol v2. Official clients map it to
category `permission_denied` with `retryability=never`. Mutations are `rejected`.

## 5. Credential provisioning and rotation

1. Operate a private CA (or SPIFFE) that issues short-lived client certs.  
2. Install server cert/key and client CA on the daemon host with mode `0600` for keys.  
3. Distribute client cert/key/CA to SDKs via filesystem paths (same option names across languages:
   `cert_file` / `key_file` / `ca_file`).  
4. **Rotation:** replace PEM files on disk, then recycle the daemon (or listener) per deployment
   policy. There is no in-band credential opcode. Revocation: remove the principal from
   `--authz-map` and/or stop trusting the client CA serial (CRL/OCSP policy remains open).  
5. Never put PEM bytes in Store options, env vars that are logged, or v2 frames.

## 6. SDK checklist

Every official SDK in the same release must expose TLS 1.3 connect options with hostname verify on
by default (lab `insecure_skip_verify` / equivalent only). mTLS uses the same client cert/key
paths. Cleartext remains available for trusted deployments.

## 7. Residual risks (still block hostile public bind)

- Phase 6: full security audit trail and admin principal counts (structured lifecycle logs exist;
  auth-specific events remain thin). OpenBSD `pledge`/`unveil` confinement (6.5) is implemented
  and is not a substitute for audit polish.  
- CRL/OCSP fail-closed policy when revocation is configured.  
- Hostile multi-tenant isolation (Phase 8).

Phase 5 abuse controls (accept/connection/principal rates, idle/request deadlines) are implemented;
see §1a. The security roadmap treats Phases 0–5 as the gate for leaving a trusted perimeter with
`--secure-profile`. Production readiness still lists Phase 6 audit before calling a deployment
“public Internet ready.”

## References

- [ADR 0020](../adr/0020-tls-outer-transport.md) · [ADR 0021](../adr/0021-secure-profile-authentication.md) ·
  [ADR 0022](../adr/0022-authorization-capabilities.md)  
- [Threat model](threat-model.md) · [Security roadmap](roadmap.md) ·
  [Wire protocol v2](../spec/wire-protocol-v2.md) · [Client semantics v1](../spec/client-semantics-v1.md)
