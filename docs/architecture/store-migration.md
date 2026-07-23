# Offline durable Store migration (Worker reshard)

Status: implemented for stopped Stores (logical live-key copy)
Applies to: durable persistence v1
Owner: persistence maintainers
Last reviewed: 2026-07-23

## Contract

Worker-count changes are **offline** operations. Stop `glyphastored` / close every Store on the
source directory. Migration never mutates the source. It creates or resumes a **destination**
durable Store with the target Worker count and copies every live visible key (value + expiry).

Procedure:

1. Exclusive-lock and verify the source (`verify_durable_store`).
2. Open the source Store (`open_existing`).
3. Snapshot live Index keys under Worker locks; sort lexicographically for deterministic resume.
4. Create the destination (`create_new`) or resume an interrupted destination (`open_existing`) when
   a matching sibling checkpoint exists.
5. For each remaining key: `get` from source, `put` into destination (preserving `expire_at_ns`).
6. Persist a sibling checkpoint after each successful put (`<destination>.migrate-state`).
7. Flush and close both Stores; verify the destination; delete the checkpoint on success.

Live/hot migration and online resharding are unsupported ([ADR 0024](../adr/0024-offline-worker-migration.md)).

## Checkpoint and resume

```text
<source-data-dir>          # unchanged
<destination-data-dir>/    # new Store catalog
<destination-data-dir>.migrate-state
```

Resume requires the checkpoint's `source_store_id`, `source_worker_count`, and
`target_worker_count` to match the current invocation. Keys before `last_key` are skipped; the last
key may be rewritten idempotently.

A destination directory without a matching checkpoint is refused (fail closed).

## What is and is not preserved

| Preserved | Not preserved |
|---|---|
| Live key bytes | Segment files / physical layout |
| Live value bytes | Per-Worker sequences |
| Absolute expiry (`expire_at_ns`) | Source `store_id` / `routing_epoch` |
| Logical visibility (Index live set) | Tombstone history / sealed dead bytes |

## Tooling

```bash
glyphastore_migrate_store [--json] [--no-scan] --workers N -- /path/to/source /path/to/destination
```

## Explicit non-goals

- In-place Worker-count rewrite
- Online / dual-ownership reshard
- Preserving sequences or Store identity
- Copying crash temporaries or compaction intents
