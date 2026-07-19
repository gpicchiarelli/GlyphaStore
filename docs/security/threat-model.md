# GlyphaStore Threat Model

Status: initial normative security boundary
Applies to: embedded engine and TCP protocol v2
Owner: security maintainers
Last reviewed: 2026-07-19

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

## 3. Current security posture

Protocol v2 provides no authentication, authorization, TLS, tenant isolation, or request quota identity. Anyone who can reach the listener can attempt Store operations. The daemon must therefore bind only to an explicitly trusted interface or run behind a trusted authenticated proxy/firewall.

CRC32C detects accidental corruption; it is not a MAC and provides no protection against deliberate byte modification. File permissions and directory ownership are the current confidentiality/integrity boundary for persistence.

## 4. Primary threats and controls

| Threat | Existing control | Residual risk / required work |
|---|---|---|
| Oversized/malformed frame | checked lengths, 2 MiB frame maximum, bounded buffers, close on unsafe framing | CPU-rate DoS remains without admission/rate limits |
| Slow reader/writer | bounded connection buffers and backpressure | timeout policy is not yet specified/implemented |
| Connection-handoff flood | bounded MPSC queue; close on saturation | fair admission and metrics required |
| Crafted persistent length/version | checked arithmetic, exact codecs, allocation bounds, fail-closed recovery | continuous fuzzing required |
| Symlink/special-file namespace attack | descriptor-relative audit and strict engine namespace | platform-specific audit and privilege guidance required |
| Concurrent directory open | exclusive lock file | network filesystems may not satisfy required semantics |
| Torn/reordered write | checksummed alternating commit slots and sync ordering | power-loss certification is platform dependent |
| Malicious disk modification | checksums and identities detect many changes | no cryptographic authenticity or encryption |
| Memory exhaustion | configured resource preflight, bounded queues/caches | allocator fragmentation and many connections require limits |
| Cross-tenant key access | none | multi-tenant deployment unsupported |

## 5. Fail-closed requirement

When persistent authority is ambiguous or runtime publication can no longer agree with durable state, the Store stops serving affected operations and reports an error. It must not guess a manifest, adopt an orphan, ignore committed corruption, or continue after an unrecoverable post-commit publication failure.

Fail-closed protects integrity but can reduce availability. Repair/salvage must be a separate explicit operator mode that never silently changes normal-open semantics.

## 6. Availability limits

Deployments must configure limits for connections, input/output buffering, handoff capacity, Store resources, descriptors, and reserved disk space. The server currently lacks an authenticated per-client quota and protocol-level idle/request deadlines. Exposing it to hostile networks is unsupported.

Hash-flood resistance is not a current security guarantee: FNV-1a and the fixed Index mix are optimized for stable routing, not adversarial collision resistance. An adversarial multi-tenant/network deployment needs a versioned keyed-hash and routing compatibility design.

## 7. Secrets and logging

Keys and values may be sensitive. Errors and logs should report sizes, identifiers, offsets, and stable categories without dumping payload bytes. Core dumps, benchmark fixtures, quarantined files, and crash artifacts can contain data and need operator-controlled permissions and retention.

No credentials should be stored in Store options or protocol fields until a credential lifecycle and redaction policy exists.

## 8. Security change requirements

A network exposure change must define authentication, authorization granularity, TLS termination, replay/retry semantics, rate limiting, timeout behavior, and audit logging. A persistent-authenticity change must define key management, rotation, backup/restore, and interaction with compaction and migration.

Security-sensitive parsers require fuzz targets and bounds tests. Platform namespace changes require symlink, hard-link, rename-race, permissions, and filesystem-semantics review.

## 9. Supported deployment statement

The current safe deployment model is a single trusted application using the embedded API, or a daemon reachable only by mutually trusted local/private clients and protected by operating-system/network controls. Public Internet exposure and hostile multi-tenancy are not supported.
