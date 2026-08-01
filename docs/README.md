# GlyphaStore documentation system

This directory is the entry point for GlyphaStore's technical documentation. Its purpose is to
make the project maintainable without relying on oral history or knowledge held by one maintainer.

## Current implementation boundary

For 0.1.0, `glyphastored` uses a single runtime: paired Reader–Writer shard pairs
([ADR 0031](adr/paired-reader-writer-shards.md)). Each pair has one Reader/Reactor and one serial
Writer connected by bounded SPSC mutation and completion lanes; GET adopts an immutable
`ReadGeneration` on the Reader. There is no second selectable daemon model.

The volatile TCP engine under `src/experimental/` (`paired_shard` / `paired_reactor`) remains
**lab-only**: compiled into tests and dedicated benchmarks, not installed, and not reachable from
`glyphastored`. Residual P1 performance and Linux hard-pinned A/B work is tracked in the
benchmark plan — it does not reopen a dual-runtime choice.

- Daemon runtime: [architecture specification](spec/architecture.md) and
  [server model](architecture/server-model.md)
- Decision and migration state: [accepted ADR 0031](adr/paired-reader-writer-shards.md) and
  [benchmark gates](benchmarks/paired-shards-plan.md)
- Lab-only prototype: [paired-shard volatile note](architecture/paired-shard-volatile-prototype.md)

## Authority and document classes

When documents disagree, use this order of authority:

1. A versioned specification under `docs/spec/` for the version it names.
2. An accepted Architecture Decision Record under `docs/adr/`.
3. A reference contract under `docs/reference/`.
4. An architecture or development guide.
5. The repository README.
6. A roadmap, changelog entry, benchmark result, or historical note.
7. Generated readiness views (`production-readiness.md`, `assurance/*.md`) are **not** normative;
   their authority is [`engineering/`](../engineering/README.md).

Code and tests are evidence that an implementation follows a contract; they are not a substitute
for a missing external contract. When code and a normative specification disagree, do not silently
edit either side. Record the discrepancy, decide which behavior is intended, update or supersede
the relevant ADR, and add compatibility evidence before changing a persisted or wire-visible rule.

Assurance program entry points: [engineering baseline](assurance/engineering-baseline.md),
[hazard register](assurance/hazards.md), [quality gates](assurance/gates.md),
[memory-order inventory](assurance/memory-order-inventory.md),
[GitHub branch-protection checklist](assurance/github-branch-protection.md), and
[AGENTS.md](../AGENTS.md).

## Required document metadata

New normative documents must begin with:

```text
Status: normative | descriptive | roadmap | historical
Applies to: named API, disk, wire, or implementation version
Owner: maintainer role or team
Last reviewed: YYYY-MM-DD
```

Use `normative` only for required behavior. Architecture guides describe the current implementation
and must label planned behavior explicitly. Roadmaps may track unfinished work but cannot redefine a
format, acknowledgement point, ownership rule, or compatibility guarantee.

## Stable specifications

| Document | Authority |
|---|---|
| [Architecture](spec/architecture.md) | Components, layering, runtime selection, ownership planes |
| [Concurrency and memory model](spec/concurrency-memory-model.md) | Threads, locks, atomics, linearization, shutdown |
| [Index v1](spec/index-v1.md) | Exact-key Index algorithm and storage invariants |
| [Native wire protocol v2](spec/wire-protocol-v2.md) | Client/server framing and session semantics |
| [Client semantics v1](spec/client-semantics-v1.md) | Portable errors, retry, timeouts, late responses |
| [TCP client conformance v1](spec/tcp-client-conformance-v1.md) | Official SDK checklist, bootstrap/retry pseudocode, fixtures |
| [Error taxonomy v1](spec/error-taxonomy-v1.md) | Cross-language status → category vectors |
| [Backup and restore v1](spec/backup-restore-v1.md) | Catalog snapshot boundary; offline and online fenced copy |
| [Persistence v1](spec/persistence-v1.md) | Complete durable namespace, binary formats, checksums, authority |
| [Recovery state-transition matrix v1](spec/recovery-state-matrix-v1.md) | Exact restart outcomes for bootstrap, commit/flush, rotation, and compaction |
| [Benchmark standard](spec/benchmark-standard.md) | Workload and result semantics |
| [Public C++ API](reference/cpp-api.md) | Supported API contracts and error behavior |
| [C++ TCP client API](reference/cpp-client-api.md) | Client routing, mutation outcomes, lifecycle, and SDK contract |

## Architecture and implementation guides

The documents in `docs/architecture/` explain rationale, implementation state, and detailed recovery
algorithms. Their stable rules are summarized by the specifications above. In particular:

- `durability-recovery.md` explains commit and recovery ordering;
- `platform-durability-evidence.md` defines E0–E4 evidence claims, native storage rows, and artifact
  provenance;
- `segment-format.md` and `manifest-format.md` provide detailed v1 field tables;
- `durable-runtime-catalog.md` describes the implemented durable runtime;
- `durable-compaction.md` describes whole-Worker durable compaction;
- `build-hardening.md` defines compiler, linker, and artifact verification;
- `server-model.md` explains ShardPair Reader/Writer ownership, SPSC lanes, and connection ownership;
- `paired-shard-volatile-prototype.md` records the lab-only volatile TCP prototype under
  `src/experimental/`; it is not the `glyphastored` runtime;
- `where-performance-matters.md` frames when engine speed helps real apps (and when it does not);
- `sdk-roadmap.md` prioritizes shared SDK contract work versus more languages.

## Official client implementations

The normative cross-language behavior is [client semantics v1](spec/client-semantics-v1.md);
conformance checklist: [tcp-client-conformance v1](spec/tcp-client-conformance-v1.md). Each
source package documents its language-specific API and concurrency contract:

| Client | Documentation |
| --- | --- |
| C++ | [C++ TCP client API](reference/cpp-client-api.md) |
| Python | [Python client README](../sdk/python/README.md) |
| Perl | [Perl client README](../sdk/perl/README.md) |
| Go | [Go client README](../sdk/go/README.md) |
| Erlang/OTP | [Erlang client README](../sdk/erlang/README.md) |
| Ruby | [Ruby client README](../sdk/ruby/README.md) |

## Decisions, development, and operations

- [Operations handbook](operations/handbook.md) — day-1/day-2 index and incident playbooks
- [Operations runbooks](operations/README.md) — graceful drain, overload, offline backup/restore, Worker reshard, corruption repair
- [Version lifecycle and compatibility](architecture/version-lifecycle.md) — 0.x upgrade/downgrade, ABI, Worker migrate
- [ADR index and lifecycle](adr/README.md)
- [Glossary](glossary.md)
- [Code tour](development/code-tour.md)
- [Test strategy](development/test-strategy.md)
- [Threat model](security/threat-model.md)
- [Security implementation roadmap](security/roadmap.md)
- [Secure-profile TLS reference](security/secure-profile.md)
- [TLS performance note](security/tls-performance.md)
- [Documentation roadmap](documentation-roadmap.md)
- [Production readiness](production-readiness.md) (generated from `engineering/gates/`)
- [Assurance baseline](assurance/engineering-baseline.md)
- [Persistence v1 roadmap](v1-production-roadmap.md)

## Review discipline

A change must update documentation when it changes any of these:

- a public API precondition, postcondition, error, lifetime, or thread-safety guarantee;
- a disk or wire byte;
- a commit, acknowledgement, visibility, or recovery point;
- Worker routing or ownership;
- a lock order, atomic synchronization edge, or shutdown transition;
- a benchmark workload or the meaning of one reported operation;
- a fixed architectural decision.

Every release should publish an immutable snapshot of these documents with the source tag. Benchmark
artifacts and roadmaps may expire; specifications for readable disk and wire versions must not.
