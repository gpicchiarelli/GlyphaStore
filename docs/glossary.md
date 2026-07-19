# GlyphaStore glossary

Status: normative terminology
Applies to: current architecture, public API, persistence v1, wire protocol v2
Owner: GlyphaStore maintainers
Last reviewed: 2026-07-19

Terms in this glossary use their exact capitalization when they name an architectural entity.

| Term | Meaning |
|---|---|
| Store | One logical exact-key key-space exposed by the embedded API or daemon. |
| Worker | The unit of key ownership and mutation serialization. A key has exactly one owner Worker for a routing epoch. |
| executor | A server thread pairing one Reactor with the data path of one Worker. It is not a separate Store. |
| Reactor | The readiness-driven owner of a set of network sockets and their connection buffers. |
| Index | Derived acceleration state mapping a complete key to one `RecordRef`. It is partitioned physically by Worker. |
| Segment | A fixed 64 MiB append unit containing a 4 KiB header followed by immutable Records. |
| active Segment | The one Segment to which a Worker may append. |
| sealed Segment | A Segment whose committed extent will no longer grow. |
| retired Segment | A Segment removed from current authority but not necessarily safe to reuse or delete. |
| reclaimable Segment | A retired Segment with no reader, retention, or recovery obligation. |
| Record | One immutable, checksummed, canonical key/value or tombstone encoding. |
| RecordRef | A positional reference containing Segment identity, offset, size, sequence, and generation; never a process address. |
| tombstone | An erase Record whose sequence suppresses every older Record for the same complete key. |
| Manifest | The authoritative durable catalog, format versions, routing configuration, and active/sealed Segment roles. |
| commit slot | One of two independently checksummed Segment-header records that authorizes a committed byte extent. |
| committed extent | The Record byte range authorized by the selected valid commit slot. |
| crash tail | Bytes beyond the committed extent. Recovery never interprets them. |
| recovery authority | The durable objects allowed to determine recovered state: the authoritative Manifest and its committed Segment extents, plus a validated recovery intent where specified. |
| routing hash | Deterministic FNV-1a-64 over the complete binary key in routing algorithm v1. |
| routing epoch | Persisted identifier for one key-to-Worker ownership assignment. It changes only through an explicit migration. |
| owner-bound | A connection or operation executes on the Worker selected by the routing function. |
| worker-affine benchmark | A workload assigning each client thread keys owned by one Worker while using the public Store path. |
| owner-bound benchmark | A workload using an already owner-checked internal path; it must not be compared as if it measured the public locking path. |
| data plane | Exact-key get, put, and erase execution and their immediately required Index/Segment work. |
| control plane | Allocation, routing configuration, catalog publication, lifecycle, and global diagnostics. |
| maintenance plane | Explicit compaction, verification, retirement, and future vacuum scheduling. |
| visibility point | Transition after which a later admitted read may observe a mutation in the running process. |
| durable commit point | Transition after which recovery must include the mutation, subject to the documented platform storage contract. |
| acknowledgement point | Transition after which a caller may receive success. It depends on storage mode. |
| durable-sync | Per-mutation strict acknowledgement after durable Segment commit-slot synchronization. |
| durable-periodic | Immediate in-process publication with restart durability only after a successful explicit or background flush. |
| durable-group | Strict acknowledgement after one batch shares Record ordering and commit-slot synchronization. |
| fail-closed | State in which the instance rejects subsequent service because it cannot prove coherent behavior. |
| poisoned directory | A `DataDirectory` whose publication outcome became indeterminate; all handles sharing its health state reject further mutation. |
| not committed | Failure known to precede the persistent authority change; recovery retains the previous state. |
| indeterminate | The authority-changing operation was attempted and its durable result cannot be inferred in the running process; close and recover are required. |
| snapshot | An owning copy of diagnostic/catalog state or a specifically pinned historical read. A returned reference to a live container is not a snapshot. |
| hot-record cache | Durable-runtime RAM cache of recently published values. It is acceleration state and never recovery authority. |
| vacuum | Copy-build-validate-publish-reclaim model, currently represented by the volatile/offline builder foundations. |
| durable compaction | Crash-safe whole-Worker replacement of its complete sealed history under a persisted intent. |
| retirement | Removal of old physical objects after a newer authority is durable and readers cannot require them. |
| residency | Whether Segment bytes are resident, mapped, or require loading; persistence alone does not imply RAM residency. |
| pin | Ownership token preventing reclamation or unmapping of a specific Segment generation. Public pinned reads are not implemented. |
| write amplification | Physical bytes written divided by logical mutation bytes or, for the current compaction gate, preallocated replacement bytes divided by Segment bytes reclaimed. The chosen definition must accompany every metric. |
| space amplification | Physical bytes retained divided by bytes required for the currently visible logical state. |
| canonical encoding | Exactly one accepted byte representation for one logical object, including required zero padding and reserved fields. |
