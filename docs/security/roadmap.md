# GlyphaStore security implementation roadmap

Status: roadmap  
Applies to: daemon TCP surface, official SDKs, durable namespace ops  
Owner: security maintainers  
Last reviewed: 2026-07-28

This roadmap turns [threat-model.md](threat-model.md) and the security rows in
[production-readiness.md](../production-readiness.md) / [v1-production-roadmap.md](../v1-production-roadmap.md)
into an ordered implementation plan. It does **not** redefine wire or persistence contracts; those
changes require ADRs and compatibility evidence first.

## Principles (non-negotiable)

1. **Correctness before exposure.** Fail-closed storage/recovery rules are never weakened for auth
   convenience.
2. **Same train for all official SDKs.** When TLS or session auth lands, C++ / Python / Perl / Go /
   Erlang / Ruby ship in the **same release**. No official SDK may silently keep cleartext defaults
   while peers go secure ([sdk-roadmap.md](../architecture/sdk-roadmap.md)).
3. **Secure profiles fail closed.** A deployment that asks for TLS/auth must not fall back to
   cleartext or anonymous access by default.
4. **OS/network boundary is not a product feature.** Loopback / private / sidecar remains the
   *documented* posture until Phases 2–4 are done; it is not a substitute for those phases.
5. **UDS is not security.** Unix-domain sockets may come later as a same-host transport; they do
   not replace TLS/auth on reachable listeners ([server-model.md](../architecture/server-model.md)).
6. **No payload secrets in logs.** Categories, sizes, request IDs — never key/value dumps
   ([threat-model.md](threat-model.md) §7).
7. **OpenBSD is first-class.** Secure-profile TLS uses **LibreSSL** on OpenBSD; CI and docs must
   not treat OpenBSD as a best-effort port ([ADR 0020](../adr/0020-tls-outer-transport.md)).
   `pledge`/`unveil` land with the secure profile hardening, not as a substitute for TLS.

## Platform matrix (secure profile)

| OS | TLS library expectation | Extra confinement |
| --- | --- | --- |
| OpenBSD | LibreSSL (system) | `pledge` / `unveil` for data dir + cert paths (**done** Phase 6.5) |
| FreeBSD | LibreSSL or OpenSSL 3.x | Capsicum evaluation later (existing v1 roadmap note) |
| Linux | OpenSSL 3.x or LibreSSL | standard file perms; optional Landlock later |
| macOS | LibreSSL/OpenSSL as provided by build | Keychain integration optional later |

## Current baseline

| Area | Today |
| --- | --- |
| Transport | Cleartext TCP (protocol v2) by default; optional TLS 1.3 outer transport when built with LibreSSL/OpenSSL (`--tls-cert`/`--tls-key`); optional dual cleartext+TLS via `--tls-port` (ADR 0020) |
| Authn / authz | Secure profile: mTLS principals + coarse `--authz-map` capabilities (ADR 0021/0022); cleartext trusted profile unchanged |
| Quotas / rate limits | Phase 5 abuse controls: accept/connection/principal rate limits, idle + request deadlines (secure-profile defaults; cleartext 0=off) |
| Audit | Phase 6: auth/authz/tls JSON audit events + `STATS` counters; lifecycle JSON logs |
| At-rest crypto | None (permissions + CRC32C; CRC is not a MAC) |
| Revocation | Optional `--tls-crl` fail-closed (+ `--tls-ocsp-fail-closed` requires CRL; no live OCSP HTTP) |
| Safe deployment | Trusted loopback/private deployment; official SDKs decode keyed-routing `INIT` (ADR 0030) |

**Beyond trusted boundary:** the daemon controls through Phase 6 are implemented, and official SDKs
decode the keyed-routing `INIT` extension (ADR 0030). Internet / hostile multi-tenant exposure
remains unsupported.

## Dependency graph (high level)

```text
Phase 0  Posture & hygiene
    │
Phase 1  Decisions (threat model + ADRs)
    ├──────────────┬────────────────┐
Phase 2  TLS       Phase 5a DoS     Phase 7 Supply chain
    │              (can overlap)     (can overlap)
Phase 3  Authn
    │
Phase 4  Authz
    │
Phase 5b Quotas / idle timeouts (identity-aware)
    │
Phase 6  Audit & admin surface
    │
Phase 8  Later: UDS, at-rest crypto, multi-tenant keyed routing
```

## Phase 0 — Posture and hygiene

**Goal:** Document the unsafe surface clearly; close documentation gaps that block security design.

| ID | Deliverable | Acceptance |
| --- | --- | --- |
| 0.1 | Daemon / README / SDK connect docs state cleartext + no auth | **done** (daemon banner + SDK READMEs) |
| 0.2 | Bind defaults remain loopback-oriented; non-loopback bind documented as explicit opt-in | **done** (CLI help + stderr warning) |
| 0.3 | Log/error redaction audit (no key/value bytes in default logs) | Ongoing; policy in threat model §7 |
| 0.4 | Link this roadmap from docs index and production readiness | **done** |

**Out of scope here:** new wire bytes.

**Effort:** days, mostly docs + small daemon messaging.

---

## Phase 1 — Decide the security architecture (gate) — **done 2026-07-20**

**Goal:** Freeze *what* we will build before coding TLS/auth into the wire.

| ID | Deliverable | Status |
| --- | --- | --- |
| 1.1 | Expand [threat-model.md](threat-model.md) with attacker profiles | **done** (§3a) |
| 1.2 | ADR transport security | **done** [ADR 0020](../adr/0020-tls-outer-transport.md) — TLS 1.3 outer; LibreSSL on OpenBSD |
| 1.3 | ADR authentication | **done** [ADR 0021](../adr/0021-secure-profile-authentication.md) — mTLS v1 |
| 1.4 | ADR authorization | **done** [ADR 0022](../adr/0022-authorization-capabilities.md) — coarse capabilities |
| 1.5 | Spec sketch: TLS wrapper, protocol v2 unchanged | **done** (in ADR 0020) |
| 1.6 | Security release process stub | **done** ([SECURITY.md](../../SECURITY.md) reporting + supported window) |

**Next:** Phase 6.1–6.4 audit + CRL fail-closed landed 2026-07-25 (with 6.5 OpenBSD confinement).
Hostile public Internet still wants multi-tenant Phase 8 and physical E3; live OCSP HTTP remains
unsupported by design (CRL is the fail-closed revocation path).
Phase 2 outer-transport TLS is complete; Phases 3–4 (mTLS principal + capabilities) landed
2026-07-23 — see [secure-profile.md](secure-profile.md).

---

## Phase 2 — Transport security (TLS) — **done (2026-07-23)**

**Goal:** Confidentiality and integrity of the byte stream; optional client identity via certificates.

| ID | Deliverable | Acceptance |
| --- | --- | --- |
| 2.1 | Daemon secure listen profile (cert, key, CA, min TLS 1.3, cipher policy) | **done** — `TlsContext` + `--tls-cert`/`--tls-key`/`--tls-client-ca`; TLS 1.3-only; explicit AEAD cipher suites; CMake `GLYPHASTORE_ENABLE_TLS` (LibreSSL on OpenBSD, OpenSSL 3.x elsewhere); [secure-profile.md](secure-profile.md) |
| 2.2 | Cleartext vs TLS listeners: explicit flags; no dual-mode “opportunistic TLS” | **done** (2026-07-20) — TLS without `--tls-port` keeps `--port` TLS-only; `--tls-port` enables dual cleartext+TLS on distinct ports (fail closed if ports collide); never opportunistic on one endpoint |
| 2.3 | Official SDKs: TLS connect options (CA, cert, hostname verify on by default in secure profile) | **done** for C++ / Python / Perl / Go / Erlang / Ruby — opt-in TLS 1.3 (`tls`/`Enable`, `ca_file`/`tls_ca`, `cert_file`/`key_file`, `server_name`, insecure lab escape); fail closed; no silent cleartext fallback |
| 2.4 | Interop matrix: every SDK PUT→GET over TLS | **done** — `test-sdk-interop.sh` cleartext + TLS matrices (ephemeral certs; Erlang included when OTP available; Perl TLS soft-excluded when `IO::Socket::SSL` is missing) |
| 2.5 | Perf note: TLS tax measured on same harness as Go/TCP benches | **done** (2026-07-20) — `scripts/benchmark_tls_tax.sh` + [tls-performance.md](tls-performance.md); Go bench gained `--tls` flags |
| 2.6 | OpenBSD CI: native LibreSSL build + TLS smoke | **done** (2026-07-20) — `.github/workflows/openbsd-libressl.yml` via `vmactions/openbsd-vm` + `scripts/ci-openbsd-libressl.sh` (LibreSSL-only configure, full ctest, Go TLS PUT→GET) |

**Daemon usage (when built with TLS):**

```text
# Cleartext (default; unchanged)
glyphastored --bind 127.0.0.1 --port 7379

# TLS-only on --port (protocol v2 inside TLS 1.3)
glyphastored --bind 127.0.0.1 --port 7379 \
  --tls-cert /path/server.crt --tls-key /path/server.key

# Dual listeners: cleartext on --port, TLS on --tls-port (ADR 0020)
glyphastored --bind 127.0.0.1 --port 7379 --tls-port 7380 \
  --tls-cert /path/server.crt --tls-key /path/server.key

# mTLS hook (ADR 0021): require client certs signed by this CA
glyphastored ... --tls-cert ... --tls-key ... --tls-client-ca /path/clients-ca.crt
```

Build: `GLYPHASTORE_ENABLE_TLS=AUTO` (default) enables TLS when LibreSSL/OpenSSL is found;
`ON` fails configure if missing; `OFF` forces cleartext-only builds. OpenBSD prefers system
LibreSSL; macOS/Linux typically need OpenSSL 3.x (`OPENSSL_ROOT_DIR` if not on the default path).

**Wire protocol:** Prefer **TLS as outer transport** with protocol v2 unchanged (no opcode churn),
unless ADR 1.5 requires an in-band upgrade — default is outer TLS.

**Effort:** 2–4 weeks daemon + SDK train (calendar longer if OpenSSL/Secure Transport matrices).

---

## Phase 3 — Authentication (authn) — **done 2026-07-23 (mTLS principals)**

**Goal:** Every admitted session has a stable **principal id** (or is rejected).

| ID | Deliverable | Acceptance |
| --- | --- | --- |
| 3.1 | Principal model (cert CN/SAN, token subject, or both per ADR) | **done** — URI SAN → DNS SAN → CN; [secure-profile.md](secure-profile.md) |
| 3.2 | Session establishment after TLS (if token-based): single auth exchange, then normal v2 ops | **n/a** for mTLS-only v1 (handshake is the auth exchange) |
| 3.3 | Credential provisioning docs (files, env, rotation without downtime) | **done** (secure-profile §5) |
| 3.4 | SDK credential hooks (no secrets in `Error.Error()` / logs / `inspect`) | **done** (existing TLS options; paths only in dump/logs) |
| 3.5 | Reject anonymous peers when secure profile enabled | **done** (`SSL_VERIFY_FAIL_IF_NO_PEER_CERT` + principal extraction) |

---

## Phase 4 — Authorization (authz) — **done 2026-07-23 (coarse capabilities)**

**Goal:** Principals are limited to allowed operations / namespaces.

| ID | Deliverable | Acceptance |
| --- | --- | --- |
| 4.1 | Capability table: e.g. `read`, `write`, `admin` (exact set in ADR) | **done** — [secure-profile.md](secure-profile.md) §3 |
| 4.2 | Enforcement points: before mutate admission; GET/PING policy explicit | **done** — Reactor pre-admission; wire `PERMISSION_DENIED` |
| 4.3 | Namespace or prefix scope (if ADR requires) | **done (Phase 8 first slice, 2026-07-25)** — [ADR 0025](../adr/0025-key-prefix-tenant-scope.md); keyed routing still later |
| 4.4 | Default-deny in secure profile | **done** — `--secure-profile` + empty/unmapped deny |

---

## Phase 5 — Abuse controls and limits — **done (2026-07-25)**

**Goal:** Bound damage from authenticated or pre-auth peers (DoS, slowloris, fan-out).

Split so transport-agnostic work can start early:

| ID | When | Deliverable | Acceptance |
| --- | --- | --- | --- |
| 5.1 | Parallel to Phase 2 | Connection / handshake rate limits; max connections | **done** — `--max-accepts-per-sec` + existing `--max-connections`; `STATS` `abuse_accepts_rejected` |
| 5.2 | Parallel to Phase 2 | Idle and request deadlines (daemon-side), aligned with client semantics | **done** — `--idle-timeout-ms` / `--request-timeout-ms` (monotonic); Store mutations already executing are never cancelled |
| 5.3 | After Phase 3 | Per-principal request and bandwidth quotas | **done** — `--principal-max-requests-per-sec` / `--principal-max-bytes-per-sec` + `--connection-max-requests-per-sec` |
| 5.4 | After Phase 3 | Pipeline / frame admission policy under overload | **done** — existing frame/buffer bounds; quota exceed ⇒ wire `overloaded`; accept flood ⇒ drop |

`--secure-profile` applies non-zero Phase 5 defaults and refuses explicit `0` (fail closed). Trusted
cleartext keeps limits at `0` (disabled) unless operators set them.

**Effort:** 2–3 weeks cumulative; 5.1–5.2 are high leverage even before public exposure.

---

## Phase 6 — Audit, admin, and operability — **done (2026-07-25)** (CRL local; live OCSP deferred)

**Goal:** Operators can see *who did what* and manage secure deployments without folklore.

| ID | Deliverable | Acceptance |
| --- | --- | --- |
| 6.1 | Security audit events: connect, auth success/fail, authz deny, TLS errors | **done** — JSON-lines `auth`/`authz`/`tls` via `SecurityAudit`; no payloads; `--secure-profile` or `--log-format json` |
| 6.2 | Admin/diagnostic surface: listener mode, TLS status, principal counts (no secret material) | **done** — `STATS` `tls_*`, `authz_*`, `auth_*`, `tls_errors`; readiness sticky rules unchanged |
| 6.3 | Runbooks: rotate certs/tokens, revoke principal, incident response | **done** — [secure-profile-certs.md](../operations/secure-profile-certs.md) |
| 6.4 | Backup/restore note: credentials and data-dir permissions | **done** — threat model §7 + certs runbook |
| 6.5 | OpenBSD `pledge`/`unveil` after `Server::create` (data dir + TLS/authz paths) | **done (2026-07-25)** — fail-closed apply; CRL path unveiled; no-op on Linux/macOS/FreeBSD |

**Revocation:** `--tls-crl` enables fail-closed CRL checking for mTLS. `--tls-ocsp-fail-closed`
requires `--tls-crl` (live AIA OCSP HTTP is unsupported under OpenBSD `pledge` without `dns`).

**Effort:** 1–2 weeks (+ ongoing polish). Item 6.5 is platform confinement, not a substitute for
revocation configuration on hostile binds.

---

## Phase 7 — Supply chain and security process (parallel track)

**Goal:** Trust the bits you ship, not only the wire.

| ID | Deliverable | Acceptance |
| --- | --- | --- |
| 7.1 | Dependency + secret scanning in CI | Gate on main |
| 7.2 | SBOM + checksums/signatures for release artifacts | **partial** — checksums + SPDX SBOM CI (`.github/workflows/supply-chain.yml`, `SYFT_REQUIRED=1`); Sigstore/GPG signing still open |
| 7.3 | Fuzz regression intake for parsers (wire + persistence) | Documented owners |
| 7.4 | Supported security maintenance window | Published policy |

**Effort:** ongoing; bootstrap 1–2 weeks.

---

## Phase 8 — Later / optional (after secure TCP profile exists)

Do **not** schedule these as blockers for “leave loopback”:

| Item | Status |
| --- | --- |
| Key-prefix capability scope (shared daemon) | **Done (2026-07-25)** — [ADR 0025](../adr/0025-key-prefix-tenant-scope.md); optional `prefix=` in `--authz-map` |
| Keyed Index mix seed (hash-flood slice) | **Done (2026-07-25)** — [ADR 0026](../adr/0026-keyed-index-hash-seed.md); `--index-hash-seed`; secure-profile randomizes |
| STATS isolation for prefix principals | **Done (2026-07-25)** — [ADR 0027](../adr/0027-stats-isolation-prefix-principals.md); prefix ⇒ `STATS` needs `admin` |
| Per-tenant data-dir / Store isolation | **Deferred** — [ADR 0028](../adr/0028-per-tenant-data-dir-deferred.md) (proposed); use one process per trust domain |
| Unix-domain socket transport | **Done (2026-07-25)** — [ADR 0029](../adr/0029-uds-peercred.md); `--unix-socket` + optional `--unix-peercred` (`unix:uid=N`); not a TLS replacement |
| At-rest encryption / MAC for segments | Key management, rotation, compaction, backup — large design |
| Keyed Worker routing (SipHash + wire seed) | **Done (2026-07-31)** — [ADR 0030](../adr/0030-keyed-worker-routing.md); daemon + C++ / Python / Perl / Go / Erlang / Ruby INIT decode + SipHash routing; default Stores stay FNV |
| Protocol-level compression / multiplexing | Unrelated; separate ADRs |

---

## Suggested calendar (indicative)

Assuming one focused security track alongside normal engine work:

| Window | Focus |
| --- | --- |
| Week 0–1 | Phase 0 + Phase 1 ADRs |
| Week 2–6 | Phase 2 TLS (daemon + SDK train) including **OpenBSD/LibreSSL CI** + Phase 5.1–5.2 |
| Week 6–8 | Phase 3 mTLS authn (mostly config on top of Phase 2) |
| Week 8–10 | Phase 4 capabilities |
| Week 9–11 | Phase 5.3–5.4 + Phase 6 + OpenBSD `pledge`/`unveil` pass |
| Continuous | Phase 7 |

OpenBSD stretches the TLS calendar (native builders, LibreSSL API quirks) and is **required**, not
optional. Prefer **shipping mTLS + local principals first**, federation later.

## SDK train checklist (every security release)

- [ ] Spec / ADR merged before SDK API freeze  
- [ ] C++ client  
- [ ] Python sync + async  
- [ ] Perl  
- [ ] Go  
- [ ] Ruby (if already past Phase 1 of its roadmap)  
- [ ] Interop harness green on secure profile  
- [ ] Cleartext profile still tested for trusted deployments (explicit, not default in secure docs)  
- [ ] Changelog + migration notes (how to enable TLS/auth without downtime)

## Relationship to other roadmaps

| Document | Role |
| --- | --- |
| [threat-model.md](threat-model.md) | Assets, boundaries, residual risks — **authority for “what we fear”** |
| This file | Ordered **how/when we implement controls** |
| [v1-production-roadmap.md](../v1-production-roadmap.md) | Broader P1 security/storage namespace bullets |
| [sdk-roadmap.md](../architecture/sdk-roadmap.md) | SDK train constraint for TLS/auth |
| [production-readiness.md](../production-readiness.md) | Checklist gates for beta/RC |

## Definition of done (security beta)

- Accepted ADRs for TLS + authn + authz granularity  
- Secure profile: TLS + authn + default-deny authz + basic rate/idle limits  
- All official SDKs in the same release  
- Threat model updated (“residual risk” rows closed or explicitly deferred to Phase 8)  
- Audit events + rotate/revoke runbook  
- Supply-chain scanning + vulnerability response owner named  
- Document unsupported surfaces (public Internet multi-tenant, at-rest crypto, …)
