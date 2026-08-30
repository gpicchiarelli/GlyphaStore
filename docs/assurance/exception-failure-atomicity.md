Status: descriptive audit; normative behavior remains in specifications and accepted ADRs
Applies to: repository version `0.1.x`
Owner: maintainers
Last reviewed: 2026-08-30

# Exception safety and failure atomicity audit

This audit classifies important mutation and lifecycle boundaries. It does not change persistence-v1,
wire-v2, acknowledgement, recovery, or visibility contracts, and it does not promote any assurance gate.
The terms below mean:

- **strong**: a reported pre-commit failure leaves the operation's observable state unchanged;
- **basic**: invariants and ownership remain valid, but non-authoritative temporary work may remain for
  documented recovery or cleanup;
- **fail-closed**: the component may enter a terminal unavailable state rather than continue with ambiguous
  authority;
- **no-throw boundary**: exceptions cannot cross the boundary; an invariant breach may still terminate.

## Classified boundaries

| Boundary | Guarantee and authority point | Failure behavior | Local checks |
| --- | --- | --- | --- |
| `Store::open` / durable bootstrap | strong until a valid Manifest becomes authoritative; recovery accepts only persistence-v1 transitions | allocation and filesystem errors return an error; partial bootstrap files are non-authoritative and namespace auditing/recovery decides cleanup or rejection | allocation-fault open campaigns; namespace, Manifest, truncated/corrupt recovery tests |
| durable PUT / ERASE | outcome follows the accepted durability mode's documented acknowledgement boundary; a post-write ambiguous failure is never relabeled known-not-committed | mutation state tracks not-entered, in-flight/indeterminate, committed, published, and completed states; irreconcilable publication failure arms fail-closed state | mutation-state characterization; durable completion-policy and crash-boundary matrices |
| paired immutable-generation publication | publication is one release edge after a complete immutable graph exists | capacity is reserved before Store entry; failure before publication retains the old generation, while failure after durable commit drains/snapshots or fails closed without issuing a false success | publication admission tests, linearizability histories, allocation failure campaigns, shutdown torture |
| incremental merge | unfinished merge is Writer-private; only a finished generation is publishable | abandonment destroys the owned cut/pins; allocation or processing failure keeps old Reader authority and records/retries or fails closed according to the caller's durable state | merge-allocation enumeration, continuous-Reader litmus, generation-reclamation checks |
| online compaction | Manifest remains sole authority until the staged segment, intent, replacement Manifest, rename, and required directory sync sequence reaches its specified boundary | every reached filesystem seam can fail; reopen must select one valid namespace/Manifest state and never treat staging files as authority | Nth-filesystem-failure enumeration plus existing FileIo, intent, crash, recovery, and namespace matrices |
| backup | source Manifest/catalog authority is immutable for the captured fence; destination publication is independent | incomplete destination state is not a successful backup and cannot change source authority | backup fault/crash matrices and fenced backup/compaction overlap tests |
| Reactor connection close | no-throw lifecycle boundary | poller removal errors are contained; socket/TLS/buffers are reset; cancellation epoch advances; the generation advances before reuse and exhausted identities retire | connection lifecycle, cold-read cancellation, close/drain, and token-wrap regression tests |
| disk-read worker completion | no-throw worker boundary with one completion per admitted slot | arbitrary task exceptions become a code-only error without allocation; the completion and recycle rings are preallocated and overflow is an invariant violation | disk executor tests, sanitizer runs, queue/resource terminal checks |
| paired runtime stop/drain/finalize | no-throw stop signal followed by explicit drain/finalization status | admission closes before joins; queued owned payloads receive terminal outcomes; Reader quiescence is required before final reclamation | 24-seed shutdown/reclamation torture across slot-pool and Writer modes |
| `Store` destruction | best effort, no exception may escape | callers that need error reporting must call `close`; destructor catches any unexpected close exception and releases owned implementation state | repeated open/close and sanitizer matrices |

## No-allocation recovery paths

The disk worker and paired shutdown abandonment paths cannot assume memory allocation remains available while
handling `std::bad_alloc`. They therefore default-construct an `Error` in already-owned optional storage and
set only its code. Preallocated free lists check capacity before every `push_back`; a violated ownership
invariant terminates rather than allocating, overwriting a live slot, or publishing partial authority.

## Filesystem failure enumeration

`NthFilesystemFailure` counts logical filesystem operations through the existing `FilesystemHooks` seam.
The online-compaction campaign resets a known durable store for every `N`, fails exactly the Nth reached
operation, closes, reopens, checks the namespace and model keys, and stops at the first N that is not reached.
The loop is capped at 64 attempts, so a missing terminal success cannot hang the suite. Existing campaigns
separately enumerate allocation sites and cover short I/O, `EINTR`, I/O errors, Manifest/intent publication,
backup, and recovery-specific boundaries.

## Resource terminal state

The shutdown torture test runs 24 deterministic seeds over embedded/dedicated Writer modes and mutation-slot
pool enabled/disabled. It overlaps PUT/GET/ERASE, merge pressure, and an active Reader lease, then checks after
drain/finalization that:

- Reader lifecycle is terminal and no active lease remains;
- retired generation debt is zero;
- no read merge remains active;
- mutation queues and owned payload slots are empty;
- no Writer/background thread survives the runtime object.

These checks detect logical ownership leaks even when a heap allocator would eventually free the enclosing
object.

## Residual correctness limits

- No local test can reproduce arbitrary physical power-loss timing, controller caches, or filesystem reorder
  behavior; platform-specific durability rows remain unproved until their retained CI evidence exists.
- Exhaustive allocation enumeration exists for selected construction/mutation families, and filesystem
  enumeration now covers online compaction, but not every operating-system call in every daemon/TLS path is
  routed through a universal Nth-failure seam.
- `std::terminate` paths protect impossible capacity/ownership states; dynamic tests demonstrate their
  preconditions for bounded campaigns but do not constitute a formal proof that every caller preserves them.
- Multi-hour starvation/fairness and operational exhaustion of 32/48/64-bit identities are not executable in
  this local campaign.
