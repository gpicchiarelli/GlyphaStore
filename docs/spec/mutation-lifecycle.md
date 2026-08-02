Status: descriptive of as-implemented paired Writer mutation lifecycle (behavior-neutral refactor baseline)
Applies to: `ShardPairRuntime::run`, durable/volatile sync and async mutation paths
Owner: store/concurrency maintainers
Last reviewed: 2026-08-02
Requirement: `GS-PAIR-MUT-LIFECYCLE-001`

# Mutation lifecycle (as implemented)

This document maps the **current** paired Writer mutation lifecycle. It does not authorize
protocol, ACK polarity, or fail-closed changes. Structural refactoring must preserve every
outcome described here. When code and this text diverge, prefer the stricter fail-closed
reading and update this document in the same commit as the behavioral lock.

Authoritative wire polarity: [error-taxonomy-v1.md](error-taxonomy-v1.md),
[client-semantics-v1.md](client-semantics-v1.md).

## 1. Three authorities (must stay distinct)

| Authority | Meaning | Today encoded as |
| --- | --- | --- |
| **Persistent** | Mutation is part of catalog / Index / durable commit | `DurableMutationOutcome` on `DurableMutationResult` |
| **Reader visibility** | Mutation is observable via published generation | `generation_published`, drain snapshot, `publish_incremental` + `publish_read_generation` |
| **Client completion** | ACK polarity decided and delivered | `SyncMutation::status` / `MutationOutcome::error` + `done` / `deliver_outcome` |

Illegal conflations to avoid when extracting types:

- treating key presence alone as commit proof;
- rewriting post-commit failures to `resource_exhausted` (OVERLOADED);
- success-ACK before required visibility;
- changing a decided completion after a later sibling error (except documented sticky visibility upgrade).

## 2. Typed stages (target model; mirrors existing flags)

```text
CommitKnowledge  ↔  DurableMutationOutcome
  known_not_committed  ↔  not_committed
  committed            ↔  committed
  indeterminate        ↔  indeterminate

PublicationState
  not_required | required | staged | published | failed

MutationStage (orchestration)
  not_admitted → admitted → staged
       ↓              ↓
  expired_pre_store   durable_started
                           ↓
              authority_committed | known_not_committed | indeterminate
                           ↓
              publication_required → published | publication_failed
                           ↓
              completion_decided → completed
```

Source flags in `src/store/paired/shard_pair_runtime.cpp` (`run`):

| Conceptual stage | Representative flags |
| --- | --- |
| Admitted | `begin_submission`, `healthy_`, `started_`, `!stopping_` |
| Staged | sync LIFO→FIFO / async SPSC coalesce |
| Expired pre-Store | `expired[]`, `expire_remaining_`, merge/retire pressure |
| Durable started | `durable_mutate_entered`, `mutate_inflight` |
| Authority | `result.committed()`, `durable_committed`, `durable_commit_observed` |
| Publication | `publication_required`, `generation_published`, `sibling_snapshot_published`, `post_commit_publication_failure` |
| Completion locked | `status_resolved` |
| Fail-closed | `publish_fail_closed` → `healthy_=false`, `expire_remaining_` |

## 3. Linearization points

| Point | When | Effect |
| --- | --- | --- |
| Store authority | Durable mutate returns `committed` / Index publish on volatile | Persistent authority acquired |
| Reader visibility | `publish_read_generation` (or successful drain snapshot publish) | RAW for subsequent GET on published generation |
| ACK decision | Completion policy after visibility rules | Wire success / OVERLOADED / INTERNAL_ERROR |
| Fail-closed | Sticky arm after indeterminate / post-commit publication failure | No new mutations linearized |

Happy durable single-op (sync):

```text
admit → enqueue → put_durable(committed) → capture → publish_incremental
  → publish_read_generation → status={} → done
```

Sticky durable (committed+error or indeterminate):

```text
put_durable → try_drain_durable_snapshot → publish_fail_closed
  → ack_after_published_visibility? → done
```

## 4. Completion policy (characterization)

Inputs: `DurableDecision` × `PublicationDecision` → `CompletionDecision`.

| Durable knowledge | Publication | Completion | Fail-closed | Notes |
| --- | --- | --- | --- | --- |
| known_not_committed | any | known_not_committed | no | Rewrite to OVERLOADED bucket |
| committed (clean) | published | success | no | |
| committed (clean) | failed / not published | indeterminate | yes | Capture/publish fail after commit |
| committed (+error) | published | success if visibility matches | yes | Sticky then ACK-after-visibility |
| committed (+error) | failed | indeterminate | yes | |
| indeterminate | published or failed | indeterminate | yes | Drain attempted |
| mutate entered, throw before outcome | — | indeterminate | yes if Store entered | Placeholders → unavailable |
| expired pre-Store | not_required | known_not_committed | no | Queue wait / pressure |

Volatile: Store append success is not ACK until `generation_published`. Throw after `store_mutated` → fail-closed; if already published, preserve success.

## 5. Illegal transitions (must remain impossible)

| From | To | Why illegal |
| --- | --- | --- |
| not_admitted / unprocessed | success | Never ACK unprocessed batch items |
| published | known_not_committed | Visibility already required commit/publish |
| committed | overloaded (resource_exhausted) | Post-commit must stay reconcile / INTERNAL_ERROR |
| completion_decided | different outcome | `status_resolved` must stick |
| fail_closed | admit new linearizing mutate | `healthy_` gate |

## 6. Extraction map (behavior-neutral)

| Module | Responsibility |
| --- | --- |
| `mutation_state` | Typed stages + legal transitions |
| `mutation_execution` | Store put/erase / volatile publish |
| `mutation_batch` | FIFO, chunking, coalesce, group mutate |
| `publication_coordinator` | Incremental publish, drain snapshot, epochs |
| `mutation_recovery` | Catch → `plan_sync_durable_exception_*` (sync durable single-op wired) |
| `completion_policy` | `decide_completion` + `status_from_completion` / wire codes |
| `fail_closed_state` | Sticky arm, expire remaining, reject admit |

## 7. Related tests (golden lock)

- `tests/integration/paired_store_tests.cpp` — RAW, batch siblings, fail-closed, known-not-committed vs sticky
- `tests/integration/server_reactor_durable_tests.cpp` — wire OVERLOADED vs INTERNAL_ERROR, shutdown drain
- `tests/quality/allocation_fault_tests.cpp` — ACK-after-publish catch paths
- `tests/unit/mutation_state_tests.cpp` — transition table + completion characterization
- `tests/unit/completion_policy_recovery_tests.cpp` — Status mapping + sync durable exception plans
