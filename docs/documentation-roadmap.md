# Documentation Roadmap

Status: roadmap
Applies to: post-audit documentation work after 2026-07-19
Owner: project maintainers
Last reviewed: 2026-07-23

Tracks remaining evidence and operator-facing material after the P0 specification set. Does not
override specifications.

| Priority | Document / update | Purpose | Audience | Suggested length | Dependencies | Estimate |
|---|---|---|---|---:|---|---:|
| P0 | Wire v2 golden fixtures and fixture commentary | Freeze canonical bytes for every opcode/status and boundaries | client authors, maintainers | **done:** independent request/response fixtures cover every current opcode/status and are verified across official SDKs | wire v2 spec | — |
| P0 | Cross-SDK interoperability suite | Prove C++/Python/Perl/Go/Ruby PUT→GET matrices (binary, expiry, pipeline, errors, 1–8 Workers) | client authors, CI | **done:** 1/2/4/8-Worker cleartext matrix, every deterministic owner, structured `NOT_FOUND`, local 2 MiB rejection, pipeline, expiry, binary/empty values, plus TLS matrix for every official SDK | harness + CI job | — |
| P0 | Compaction intent v1 golden fixture | Freeze the last persistent codec without independent golden bytes | persistence maintainers | **done:** canonical dual-Manifest fixture, independent generator/verifier, decode-only and exact encoder evidence | persistence v1 | — |
| P0 | Error taxonomy ADR | Decide compatibility categories including format incompatibility and indeterminate outcomes | API/client maintainers | 4–6 pages | **done:** [ADR 0019](adr/0019-client-error-retry-timeout.md) + [client semantics v1](spec/client-semantics-v1.md) | — |
| P0 | Platform durability evidence matrix | Record which filesystem/device guarantees were actually tested | release and storage maintainers | **harness foundation done:** [E0–E4 evidence policy and matrix](architecture/platform-durability-evidence.md), E2 collector, E3 block-reset harness for linux-ext4 loopback / macOS APFS diskimage, CI rehearsal workflow; **no row is E3/E4 certified** | dedicated crash/power-loss infrastructure + reviewed campaign artifacts | campaign execution and review |
| P0 | Recovery state-transition matrix | Enumerate every bootstrap, rotation, flush, and compaction interruption outcome | persistence maintainers | **done:** [normative v1 matrix](spec/recovery-state-matrix-v1.md) with authority, visibility, resume/fail-closed outcome, and evidence mapping | persistence v1, crash tests | — |
| P1 | Operations handbook | Start/stop, signals, directories, limits, backup/restore constraints, metrics, incidents | operators | 20–30 pages | stable daemon configuration | **partial:** [operations runbooks](operations/README.md) cover drain/overload, offline backup/restore, and corruption repair; full handbook (config reference, incidents, observability) remains open | 4–6 days |
| P1 | Compatibility and migration manual | Turn format matrix into released-version upgrade/downgrade procedures | operators, release engineers | 10–15 pages | first released durable artifact | 3–4 days |
| P1 | TCP client conformance guide | Give pseudocode, retry rules, malformed examples, and conformance vectors | client implementers | 12–18 pages | wire fixtures | 3 days |
| P1 | Ruby SDK isomorphism roadmap | Phase gates for a complete Ruby client (correctness → security → performance) | client maintainers | living doc | **done:** [ruby-sdk-roadmap](architecture/ruby-sdk-roadmap.md) | — |
| P1 | SDK roadmap execution notes | Track batch API, per-request deadlines, structured errors, Perl clock/fork, Go sequencing, Ruby | client maintainers | living doc | [sdk-roadmap](architecture/sdk-roadmap.md) | ongoing |
| P1 | Rebalance design ADR/specification | Define epoch transition and ownership propagation before online resizing exists | architects | 12–20 pages | routing v1 and Worker-affine server | 4–6 days |
| P1 | Observability reference | Define counters, units, cardinality, health/readiness, and stable log fields | operators | 10–15 pages | metrics implementation design | 3 days |
| P1 | Backup and restore specification | Define consistent snapshot boundary and supported copy/restore procedure | operators, storage maintainers | 10–15 pages | persistence and flush contracts | 3–4 days |
| P1 | Release checklist | Bind fixtures, tests, sanitizer/platform evidence, provenance, and docs snapshot | release maintainers | 4–6 pages | CI/release workflow | 1–2 days |
| P2 | Performance history policy | Define controlled runners, retention, regression thresholds, and baseline changes | performance maintainers | 6–8 pages | benchmark standard, stable runners | 2 days |
| P2 | Allocation and residency reference | Quantify per-entry/connection/Worker memory and file-backed RSS interpretation | performance/operators | 8–12 pages | instrumentation | 2–3 writing days after measurement |
| P2 | Fuzzing corpus guide | Define seeds, dictionaries, retention, minimization, and compatibility corpora | security/quality maintainers | 5–8 pages | fuzz targets | 2 days |
| P2 | Contributor architecture exercises | Guided changes that teach routing, codec, recovery, and Reactor invariants | new contributors | 10–15 pages | code tour | 3 days |

## Review cadence

- On every disk/wire/API change: update affected specification, ADR relationship, fixtures, and
  compatibility matrix in the same change.
- Monthly during active development: review roadmap and readiness statements for stale completion
  claims.
- Before every release: verify all local links, freeze a documentation snapshot with the tag, and
  record the exact commit in benchmark/evidence artifacts.
- Annually after 1.0: review terminology, supported-platform evidence, deprecated API, migration
  paths, and whether each accepted ADR still has an explicit successor relationship.

## Definition of done

A document is complete when its intended reader can make the named decision or perform the named
operation without reading implementation source or asking an original author; examples and tests
agree with it; ownership and review date are present; and no higher-authority document contradicts
it.
