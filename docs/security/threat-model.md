# GlyphaStore Threat Model

Status: initial normative security boundary
Applies to: embedded engine and TCP protocol v2
Owner: security maintainers
Last reviewed: 2026-07-25

## 1. Assets

Protected assets are availability and integrity of Store state, confidentiality of key/value data, correctness of routing and recovery, process memory safety, and exclusive ownership of a durable directory.

## 2. Trust boundaries

| Boundary | Trust assumption |
|---|---|
| C++ caller | trusted to obey C++ object lifetime; byte contents and sizes still validated |
| TCP peer | untrusted |
| Persistent bytes | malformed/untrusted decoder input, even on a trusted local disk |
| Data-directory parent and OS | trusted for configured permissions and documented filesystem semantics |
| Same-host process | untrusted unless OS permissions deny namespace/socket access |
| Hardware/filesystem durability | trusted only to the extent explicitly certified |
| Platform targets | Linux, macOS, FreeBSD, and OpenBSD are in-scope; OpenBSD uses LibreSSL and may apply `pledge`/`unveil` once implemented |

## 3. Current security posture

Protocol v2 itself carries no authentication, authorization, tenant isolation, or request quota
identity **inside the frame**. **Optional outer TLS 1.3** is implemented
([ADR 0020](../adr/0020-tls-outer-transport.md), [secure-profile reference](secure-profile.md)).
The **secure profile** adds mTLS principals and coarse capabilities (Phases 3–4) plus Phase 5
abuse controls. Cleartext remains the default for trusted loopback. Anyone who can complete a
session on a reachable listener without the secure profile may still attempt Store operations. The
daemon must therefore bind only to an explicitly trusted interface (default `127.0.0.1`) or run
behind a trusted authenticated proxy/firewall unless the full secure profile is enabled (Phases 2–5).

CRC32C detects accidental corruption; it is not a MAC and provides no protection against deliberate byte modification. File permissions and directory ownership are the current confidentiality/integrity boundary for persistence.

The **secure profile** ([ADR 0020](../adr/0020-tls-outer-transport.md)–[0022](../adr/0022-authorization-capabilities.md),
[secure-profile reference](secure-profile.md)) adds TLS 1.3 (Phase 2), mTLS principal extraction
(Phase 3), coarse capabilities (Phase 4), and abuse controls (Phase 5). Hostile public Internet
exposure still wants Phase 6 audit polish and CRL/OCSP policy; multi-tenant isolation remains Phase 8.

## 3a. Attacker profiles (planning)

| Profile | Attacker capability | Intended control (target) |
|---|---|---|
| Local OS user | Can open loopback ports / read world-readable sockets | OS permissions; default bind `127.0.0.1`; OpenBSD `unveil` of data dir + cert/authz paths after create |
| On-path LAN | See cleartext TCP on a non-loopback bind | Do not bind non-loopback without secure profile; TLS (ADR 0020) |
| Compromised authenticated client | Valid client cert for one principal | Capability limits (ADR 0022); revoke/rotate certs; Phase 5 quotas |
| TLS sidecar / mesh peer | Terminates TLS before daemon | Daemon may stay cleartext only on a strictly private interface; official secure profile still prefers in-daemon TLS |
| Malicious stored bytes | Crafted segments/manifests | Existing fail-closed codecs + fuzzing; not solved by TLS |
| Hostile multi-tenant share | Many principals, adversarial keys | **Unsupported** until keyed routing + stronger isolation (roadmap Phase 8) |

## 4. Primary threats and controls

| Threat | Existing control | Residual risk / required work |
|---|---|---|
| Oversized/malformed frame | checked lengths, 2 MiB frame maximum, bounded buffers, close on unsafe framing | CPU-rate DoS mitigated by Phase 5 admission/rate limits when enabled |
| Slow reader/writer | bounded connection buffers and backpressure; `--idle-timeout-ms` / `--request-timeout-ms` | Tune deadlines for workload; Store mutations in execution are never cancelled |
| Connection-handoff flood | bounded MPSC queue; close on saturation; `--max-accepts-per-sec` | Fair admission counters in `STATS` |
| Crafted persistent length/version | checked arithmetic, exact codecs, allocation bounds, fail-closed recovery | continuous fuzzing required |
| Symlink/special-file namespace attack | descriptor-relative audit and strict engine namespace | platform-specific audit and privilege guidance required; OpenBSD `unveil` confines the daemon to the data directory + configured TLS/authz paths |
| Concurrent directory open | exclusive lock file | network filesystems may not satisfy required semantics |
| Torn/reordered write | checksummed alternating commit slots and sync ordering | power-loss certification is platform dependent (OpenBSD `fdatasync`≡`fsync` cost) |
| Malicious disk modification | checksums and identities detect many changes | no cryptographic authenticity or encryption |
| Memory exhaustion | configured resource preflight, bounded queues/caches | allocator fragmentation and many connections require limits |
| Cross-tenant key access | none | multi-tenant deployment unsupported |
| Cleartext eavesdropping | trusted bind / network boundary; optional TLS 1.3 outer transport when enabled | Enable TLS (ADR 0020) before non-loopback exposure; LibreSSL on OpenBSD |
| Anonymous remote mutate | mTLS + `--authz-map` / `--secure-profile` (default-deny; wire `PERMISSION_DENIED`) | Phase 6 audit polish + CRL/OCSP before hostile public Internet |

## 5. Fail-closed requirement

When persistent authority is ambiguous or runtime publication can no longer agree with durable state, the Store stops serving affected operations and reports an error. It must not guess a manifest, adopt an orphan, ignore committed corruption, or continue after an unrecoverable post-commit publication failure.

Fail-closed protects integrity but can reduce availability. Repair/salvage must be a separate explicit operator mode that never silently changes normal-open semantics.

Secure-profile misconfiguration (missing CA, failed client verify, empty capability map) must likewise fail closed rather than degrade to anonymous cleartext.

## 6. Availability limits

Deployments must configure limits for connections, input/output buffering, handoff capacity, Store resources, descriptors, and reserved disk space. Phase 5 adds optional accept/connection/principal rate limits and idle/request deadlines (`--secure-profile` applies non-zero defaults). Exposing the daemon to hostile networks without the secure profile remains unsupported.

Hash-flood resistance is not a current security guarantee: FNV-1a and the fixed Index mix are optimized for stable routing, not adversarial collision resistance. An adversarial multi-tenant/network deployment needs a versioned keyed-hash and routing compatibility design.

## 7. Secrets and logging

Keys and values may be sensitive. Errors and logs should report sizes, identifiers, offsets, and stable categories without dumping payload bytes. Core dumps, benchmark fixtures, quarantined files, and crash artifacts can contain data and need operator-controlled permissions and retention.

No credentials should be stored in Store options or protocol fields until a credential lifecycle and redaction policy exists. TLS private keys and client certificates must never be written to logs or client `Error` messages.

## 8. Security change requirements

A network exposure change must define authentication, authorization granularity, TLS termination, replay/retry semantics, rate limiting, timeout behavior, and audit logging. A persistent-authenticity change must define key management, rotation, backup/restore, and interaction with compaction and migration.

Security-sensitive parsers require fuzz targets and bounds tests. Platform namespace changes require symlink, hard-link, rename-race, permissions, and filesystem-semantics review. OpenBSD changes that add `pledge`/`unveil` require an explicit promise set review so TLS, data-directory I/O, and logging keep working without ambient home-directory access.

## 9. Supported deployment statement

The current safe deployment model is a single trusted application using the embedded API, or a daemon reachable only by mutually trusted local/private clients and protected by operating-system/network controls. Public Internet exposure and hostile multi-tenancy are not supported.

Ordered implementation plan: [security roadmap](roadmap.md). Decisions: [ADR 0020](../adr/0020-tls-outer-transport.md), [ADR 0021](../adr/0021-secure-profile-authentication.md), [ADR 0022](../adr/0022-authorization-capabilities.md).
