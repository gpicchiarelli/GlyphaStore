Status: descriptive
Applies to: repository version `0.1.x` (Phase B1)
Owner: maintainers
Last reviewed: 2026-08-01

# Atomics and memory-order inventory

Inventory of shared atomics that participate in correctness. Normative rules:
[`docs/spec/concurrency-memory-model.md`](../spec/concurrency-memory-model.md).
Requirement: `GS-CONCUR-MEM-001`.

## 1. Paired ReadGeneration publication

| Variable | W / R | Orders | HB / role | Proof |
| --- | --- | --- | --- | --- |
| `Lane::published_generation` | Writer / Readers | release / acquire | Publish ⇒ observe generation | linearizability tests, TLA+ |
| `Lane::published_catalog_revision` | Writer / Readers | release / acquire | Durable refresh coherence | durable paired paths |
| `Lane::reader_safe_epoch` | Reader / Writer reclaim | acq_rel | Min leased epoch before free | generation lease tests |
| `Lane::active_read_leases` | Readers / Writer | relaxed + seq_cst fence before adopt | Closes UAF window | ASan CI |
| `healthy_` | Writer fail-closed / observers | release / acquire | Terminal unavailability | fail-closed paths |

## 2. Admission and shutdown

| Variable | W / R | Orders | Role | Proof |
| --- | --- | --- | --- | --- |
| `admission_state_` | submitters / drain | acq_rel | Closed bit + count | close / LIVE-001 |
| `stopping_` / `Lane::stopping` | closer / Writer | acq_rel / release+acquire | Stop then wake | close tests |
| `active_writers_` | Writers / closer | acq_rel / acquire | Drain observes exit | stop_and_drain |
| `SyncMutation::done` | Writer / caller | release / acquire | Mutation completion | put/erase tests |
| Store `active_operations[]` | ops / close | relaxed admit; acq_rel finish | Close LP + drain | GS-CORE-CLOSE-001 |
| Store `lifecycle` | closer / waiters | acq_rel / release / acquire | One-way close | close tests |

## 3. Queues

| Variable | Orders | Role | Proof |
| --- | --- | --- | --- |
| SPSC `head`/`tail` | release publish; acquire consume | Cell ownership | unit tests + TSan |
| MPSC cell `sequence` | release / acquire | Connection handoff | bounded_mpsc tests |

## 4. Telemetry-only (relaxed by policy)

Latency histograms, high-water marks, admit/reject counters, merge stats. Must not
publish object contents; promoting a counter to a correctness edge requires updating
this inventory and the concurrency spec.

## 5. Residual gaps

- No machine-checked proof every `relaxed` use is telemetry-only.
- Multi-shard linearizability relies on routing independence.
- Durable Manifest publication uses mutex/CV domains (see concurrency spec).
