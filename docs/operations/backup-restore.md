# Runbook: backup and restore

Status: descriptive
Applies to: durable data directories (`manifest.glypha` + catalog Segments)
Owner: persistence maintainers
Last reviewed: 2026-07-31

Architecture contract: [backup-restore](../architecture/backup-restore.md). CLI reference:
[cli.md § glyphastore_backup_store](../cli.md#glyphastore_backup_store).

## Purpose

Create a verified copy of a Store for migration, disaster recovery, or pre-change safety. Restore is
the same verified copy into a **new empty** destination.

Two paths:

| Path | When | Mechanism |
|---|---|---|
| Offline CLI | Stopped Store / stopped `glyphastored` | Exclusive data-dir lock via `glyphastore_backup_store` |
| Online API | Open durable `Store` / live `glyphastored` | `Store::backup_to` or wire `BACKUP` (opcode 10) |

## Hard requirements (fail closed)

- **Offline CLI:** stop all writers before backup or restore. The tool takes an exclusive Store lock
  and fails if `glyphastored` or another `Store` holds the directory.
- **Online API:** Store stays open and keeps the lock; mutations are **briefly fenced** (not a fully
  concurrent hot copy). Prefer offline CLI for cold release backups.
- **Destination must be empty** (`create_new`). Non-empty destinations fail with `sequence_conflict`
  or `invalid_argument`.
- **No in-place restore** over the production directory; always copy into a new path, verify, then
  swap at the orchestration layer (rename/mount/service pointer), not by overwriting open files.
- Copies include **only** recovery-safe catalog Segments and `manifest.glypha` (last). Crash
  temporaries, compaction intents, and bootstrap intents are **not** copied.

## Backup procedure

### Offline (preferred for release artifacts)

#### 1. Quiesce writers

```bash
systemctl stop glyphastored
# confirm no process holds the data dir
```

#### 2. Optional: structural verify (recommended)

```bash
glyphastore_verify_store -- /var/lib/glyphastore
glyphastore_verify_store --json -- /var/lib/glyphastore
```

Exit `1` means do **not** proceed to backup until corruption runbook is followed
([corruption-repair.md](corruption-repair.md)).

#### 3. Run backup into a new empty directory

```bash
install -d -m 700 /backup/glyphastore-2026-07-23
glyphastore_backup_store -- /var/lib/glyphastore /backup/glyphastore-2026-07-23
```

JSON audit trail:

```bash
glyphastore_backup_store --json -- /var/lib/glyphastore /backup/glyphastore-2026-07-23
```

Faster header-only pass (skips committed Record CRC scan):

```bash
glyphastore_backup_store --no-scan -- /var/lib/glyphastore /backup/glyphastore-2026-07-23
```

Prefer full scan for release backups; `--no-scan` is for repeated operator checks when header/commit
validation is sufficient.

#### 4. Confirm success

- Exit code `0`.
- Tool verifies the destination after copy (lock → verify source → copy catalog files → sync → verify
  destination).
- Store backup artifacts outside the live data directory (separate volume or object storage).

### Online (embedded Store / live daemon)

Use `Store::backup_to(destination)` against an open durable Store, or wire opcode `BACKUP` (10) against
a live `glyphastored` (key = UTF-8 destination path; requires `admin` under `--authz-map`). Expect a
short admission fence (in-flight ops drain; new ops return `unavailable` until copy completes).
External `glyphastore_backup_store` against the same path still fails with `io_error` while the Store
holds the lock. Official SDKs may not wrap `BACKUP` yet.

## Restore procedure

### 1. Prepare a new empty data directory

```bash
install -d -m 700 /var/lib/glyphastore-restored
```

### 2. Copy from backup (same tool, reversed paths)

```bash
glyphastore_backup_store -- /backup/glyphastore-2026-07-23 /var/lib/glyphastore-restored
```

### 3. Open via recovery

Point `glyphastored` or embedded `Store::open(..., open_existing)` at the restored directory.
Recovery rebuilds Indexes from the Manifest and committed Records; no separate index rebuild tool is
required.

```bash
glyphastored --profile production --data-dir /var/lib/glyphastore-restored --bind 127.0.0.1 --port 7379
```

Confirm `READY` before serving traffic.

### 4. Cut over

Stop the old instance, atomically switch the service to the restored path (symlink, mount, or config
update), start the daemon, re-verify `READY`.

## Failure modes

| Condition | Result | Operator action |
|---|---|---|
| Source locked (offline CLI) | `io_error` (already locked) | Stop writers; retry, or use `Store::backup_to` |
| Source fails verify | same error as `verify_store` | Follow corruption runbook; do not treat backup as valid |
| Destination exists / non-empty | `sequence_conflict` / `invalid_argument` | Choose a new empty path |
| Copy I/O failure mid-way | fail closed; destination may be incomplete | Delete incomplete destination; retry |

## What NOT to do

- Do **not** run `glyphastore_backup_store` against a live data directory “for convenience”.
- Do **not** treat online fenced backup as zero-impact under load; schedule it or use offline CLI.
- Do **not** restore by copying files manually without verify/sync ordering; use the tool so catalog
  Segments and Manifest publication order match the implementation contract.
