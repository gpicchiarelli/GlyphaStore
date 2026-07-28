# Security policy

## Supported versions

GlyphaStore is pre-alpha software. No release currently carries a production support, durability,
wire-compatibility, or disk-format guarantee.

| Version | Security status |
| --- | --- |
| `main` / `0.1.x` | Best-effort security fixes during prototype development |
| Earlier snapshots | Unsupported |

## Reporting a vulnerability

Do not open a GitHub issue, discussion, or pull request for a suspected vulnerability.

1. Open the repository's **Security** page and use **Report a vulnerability** when that control is
   available.
2. If private vulnerability reporting is unavailable, contact the repository owner through the
   existing private project channel and ask for a draft GitHub Security Advisory. Do not include
   exploit details in a public channel.

Include as much of the following as possible:

- the affected version and exact commit;
- operating system, architecture, compiler, and build preset;
- a minimal reproduction or malformed input sample;
- expected impact and realistic attack preconditions;
- sanitizer output, stack trace, logs, or corrupted segment metadata;
- whether the issue is already public or subject to a disclosure deadline.

Expect an acknowledgement within three business days. Acknowledgement is not a confirmation of
severity or eligibility for a CVE.

## Current network posture

`glyphastored` defaults to **cleartext TCP** with **no authentication**. Optional **TLS 1.3** outer
transport is available when built with LibreSSL/OpenSSL (`--tls-cert`/`--tls-key`; dual
`--tls-port`; see [docs/security/secure-profile.md](docs/security/secure-profile.md)). The safe
default remains bind `127.0.0.1` (loopback) or an otherwise trusted private boundary. Do not expose
the listener to untrusted networks until mTLS principals and capabilities are enabled.

The **secure profile** is specified in [docs/security/roadmap.md](docs/security/roadmap.md) and ADRs
[0020](docs/adr/0020-tls-outer-transport.md)–[0022](docs/adr/0022-authorization-capabilities.md)
([0025](docs/adr/0025-key-prefix-tenant-scope.md) optional key-prefix scope).
Phase 2 (outer TLS) is implemented; Phases 3–4 (mTLS principals + `--authz-map` /
`--secure-profile`) are implemented in the daemon and C++/Python/Perl/Go/Erlang clients. **Ruby**
remains cleartext-only until its SDK Phase 3 (explicit exception to the “same train” TLS policy).
**OpenBSD** is a first-class portability target (LibreSSL; `pledge`/`unveil` after
`Server::create` — Phase 6.5; not a durable storage certification). CI builds and tests the daemon against system LibreSSL
([`.github/workflows/openbsd-libressl.yml`](.github/workflows/openbsd-libressl.yml)). Enable the full secure profile (TLS + mTLS + capabilities + Phase 5 abuse limits + Phase 6 audit) before leaving a
trusted perimeter. Configure `--tls-crl` (and optionally `--tls-ocsp-fail-closed`) before hostile
public Internet. Optional `--authz-map` `prefix=` isolates GET/PUT/ERASE key namespaces (Phase 8
first slice); keyed routing and full multi-tenant readiness remain residual. Live OCSP HTTP is
unsupported. Physical E3 remains open.
Threat model: [docs/security/threat-model.md](docs/security/threat-model.md).

## Security scope

Persisted segment files, native network frames, and recovery metadata are all treated as untrusted
input. A crash, out-of-bounds access, integer overflow, use-after-free, data race, or silent
corruption is considered security-relevant.

Reports are especially relevant when they involve:

- memory safety or undefined behavior reachable from untrusted bytes;
- unchecked lengths, offsets, sequence numbers, or allocation arithmetic;
- corruption that survives validation or causes an incorrect recovery result;
- cross-Worker ownership violations or concurrency defects that expose or corrupt data;
- denial of service that bypasses documented resource bounds;
- build, dependency, or release-pipeline compromise.

Performance differences, unsupported-platform build failures, and ordinary correctness bugs
without a security boundary or realistic security impact should use the normal bug template.

## Coordinated disclosure

Please allow time to reproduce, assess, fix, and test the issue before disclosure. The maintainer
will coordinate a disclosure date and credit through the private advisory when the report is
confirmed. Do not share repository secrets, personal data, or third-party confidential material in
the report.
