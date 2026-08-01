# Durable backup and restore

Status: offline implemented; online fenced backup implemented for open Stores  
Applies to: durable data directories (`manifest.glypha` + catalog Segments)  
Owner: persistence maintainers  
Last reviewed: 2026-08-01

Normative consistency boundary: [backup-restore v1](../spec/backup-restore-v1.md). This page is the
architecture narrative; prefer the spec on conflict.

## Contract

### Offline (`glyphastore_backup_store` / `backup_durable_store`)

Backup and restore are **offline** operations when driven by the CLI. Stop `glyphastored` / close
every Store that holds the source directory before running them. The implementation takes the
exclusive Store lock and fails closed if the directory is already locked.

Procedure:

1. Exclusive-lock the source data directory.
2. Run `verify_durable_store` (Manifest, namespace audit, catalog Segment open + optional CRC scan).
3. Create an empty destination directory (`create_new`).
4. Copy only recovery-safe files: every Manifest catalog Segment (private `0600`), then
   `manifest.glypha` last. Sync each file and the destination directory.
5. Release locks and `verify_durable_store` the destination.

Restore is the same verified copy from a backup directory into a new empty destination. Ordinary
`Store::open(..., open_existing)` rebuilds Indexes via recovery.

### Online (`Store::backup_to`)

An open durable Store may copy its catalog into an empty destination without releasing the
data-directory lock:

1. Fence new Store admissions (in-flight ops drain; concurrent `put`/`get` see `unavailable` briefly
   during flush + catalog copy only).
2. Flush durable state.
3. Hold the catalog exclusive lock; structurally verify + copy Manifest catalog Segments (bounded
   parallel) then `manifest.glypha` last (no source CRC under the fence).
4. Resume admissions; verify the destination independently (optional CRC; promotion gate).

This is **online** (daemon/Store process stays up) with a **writer fence** during the copy window.
It is not a fully concurrent hot copy of active Segment tails under unpaced writers, and it is not
filesystem freeze / COW snapshot orchestration. Future zero-fence requirements:
[ADR 0034](../adr/0034-zero-fence-hot-backup-deferred.md).

## Explicit non-goals

- Fully concurrent hot backup with zero admission fencing
- Copying crash temporaries, compaction intents, or bootstrap intents into a backup
- In-place destructive repair
- Filesystem freeze / COW snapshot orchestration

Offline repair that quarantines non-catalog anomalies into an explicit workspace is provided by
`glyphastore_repair_store` (see [cli.md](../cli.md)); it never rewrites the source directory.
Operator procedures: [backup-restore runbook](../operations/backup-restore.md),
[corruption-repair runbook](../operations/corruption-repair.md).

## Failure modes

| Condition | Result |
| --- | --- |
| Source locked by Store/daemon (offline tool) | `io_error` (already locked) |
| Source fails verify | same error as `verify_durable_store` |
| Destination exists / non-empty | `sequence_conflict` or `invalid_argument` |
| Copy I/O failure mid-way | fail closed; destination may be incomplete and must not be opened for service without re-verify |
| Online backup on volatile Store | `invalid_argument` |

## Tooling

```bash
glyphastore_backup_store [--json] [--no-scan] -- /path/to/source /path/to/backup
glyphastore_backup_store [--json] [--no-scan] -- /path/to/backup /path/to/restored
```

Embedded / in-process:

```cpp
store->backup_to("/path/to/empty/backup");
server->backup_to("/path/to/empty/backup");
```

Live daemon (wire opcode `BACKUP` = 10; key = destination path; `admin` under secure authz).