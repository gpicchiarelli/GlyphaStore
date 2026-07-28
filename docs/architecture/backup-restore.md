# Offline durable backup and restore

Status: implemented for stopped Stores
Applies to: durable data directories (`manifest.glypha` + catalog Segments)
Owner: persistence maintainers
Last reviewed: 2026-07-21

## Contract

Backup and restore are **offline** operations. Stop `glyphastored` / close every Store that holds the
source directory before running them. The implementation takes the exclusive Store lock and fails
closed if the directory is already locked.

Procedure:

1. Exclusive-lock the source data directory.
2. Run `verify_durable_store` (Manifest, namespace audit, catalog Segment open + optional CRC scan).
3. Create an empty destination directory (`create_new`).
4. Copy only recovery-safe files: every Manifest catalog Segment (private `0600`), then
   `manifest.glypha` last. Sync each file and the destination directory.
5. Release locks and `verify_durable_store` the destination.

Restore is the same verified copy from a backup directory into a new empty destination. Ordinary
`Store::open(..., open_existing)` rebuilds Indexes via recovery.

## Explicit non-goals

- Live/hot backup while writers hold the Store lock
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
| Source locked by Store/daemon | `io_error` (already locked) |
| Source fails verify | same error as `verify_durable_store` |
| Destination exists / non-empty | `sequence_conflict` or `invalid_argument` |
| Copy I/O failure mid-way | fail closed; destination may be incomplete and must not be opened for service without re-verify |

## Tooling

```bash
glyphastore_backup_store [--json] [--no-scan] -- /path/to/source /path/to/backup
glyphastore_backup_store [--json] [--no-scan] -- /path/to/backup /path/to/restored
```
