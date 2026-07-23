# Runbook: corruption detection and offline repair

Status: descriptive
Applies to: durable v1 data directories and Segment files
Owner: persistence maintainers
Last reviewed: 2026-07-23

Recovery on ordinary `Store::open` **never** mutates, truncates, or rewrites source files
([durability-recovery](../architecture/durability-recovery.md)). Suspected corruption is diagnosed
with read-only tools; salvage uses `glyphastore_repair_store` into an explicit **empty workspace**
with quarantine **outside** the live store tree.

## Purpose

Classify storage faults, preserve evidence, and produce a clean verified store copy when salvage is
possible — without in-place destructive repair.

## Symptom → first command

| Symptom | First step |
|---|---|
| Daemon fails open / `READY` sticky fault | Stop daemon; `glyphastore_verify_store` on `--data-dir` |
| Known bad Segment file | `glyphastore_inspect_segment` on the file |
| Extra files after crash (orphan Segments, notes) | Verify, then repair into workspace if verify passes except quarantinable anomalies |
| Missing catalog Segment, symlink, hard link | **Fail closed** — repair refuses; restore from backup |

## Step 1 — Stop writers and verify (read-only)

```bash
systemctl stop glyphastored
glyphastore_verify_store --json -- /var/lib/glyphastore
```

`glyphastore_verify_store`:

- takes the **exclusive Store lock** (fails if daemon still running);
- decodes `manifest.glypha` and audits the namespace;
- opens each catalog Segment read-only against Manifest identity;
- scans committed Record extents (unless `--no-scan`).

Exit codes: `0` validated, `1` validation/I/O failure, `2` usage. Exit `1` is fail closed — do not
open for service until classified.

Header-only check when iterating:

```bash
glyphastore_verify_store --no-scan -- /var/lib/glyphastore
```

## Step 2 — Inspect individual Segments (optional)

When verify points at a specific catalog file or you have a detached Segment copy:

```bash
glyphastore_inspect_segment --json -- segment-0123456789abcdef-00000001.glypha
glyphastore_inspect_segment --no-scan -- segment-0123456789abcdef-00000001.glypha
```

Does **not** take the Store directory lock. A concurrent writer can cause a torn commit observation;
that is reported as validation failure, not success.

## Step 3 — Offline repair (quarantine outside store)

Use when verify fails because of **quarantinable** namespace anomalies (unlisted Segments, crash
temporaries, compaction intents, unknown regular files) but catalog Segments listed in the Manifest
are present and safe.

**Requirements:**

- Source directory is unchanged (repair **never mutates** source).
- Workspace directory is **completely empty**.
- Writable space for `<workspace>/store` and `<workspace>/quarantine`.

```bash
install -d -m 700 /srv/glyphastore-repair-2026-07-23
glyphastore_repair_store --json -- /var/lib/glyphastore /srv/glyphastore-repair-2026-07-23
```

On success the workspace contains:

| Path | Contents |
|---|---|
| `<workspace>/store/` | Clean Manifest + catalog Segments (verified after copy) |
| `<workspace>/quarantine/` | Non-catalog anomalies moved out of the catalog view |
| `<workspace>/quarantine/audit.txt` | Operator audit trail |

Open the repaired store only from `<workspace>/store`:

```bash
glyphastore_verify_store -- /srv/glyphastore-repair-2026-07-23/store
glyphastored --data-dir /srv/glyphastore-repair-2026-07-23/store ...
```

Preserve `<workspace>/quarantine/` for forensics; do **not** copy quarantined files back into the
catalog namespace.

## Fail-closed conditions (repair refuses)

Repair exits `1` and does **not** produce a usable `<workspace>/store` when:

- a Manifest-listed catalog Segment is **missing**;
- the namespace contains **unsafe** entries (symlinks, hard links, non-regular objects);
- the workspace is **non-empty**;
- structural verify of the salvageable catalog fails.

In these cases restore from the last good **offline backup** ([backup-restore.md](backup-restore.md))
or engage maintainers; do not attempt manual catalog surgery in the production directory.

## Corruption vs incompatibility

| Class | Examples | Tool behavior |
|---|---|---|
| Corruption | bad CRC in committed extent, missing listed Segment, inconsistent sequences | verify/repair fail closed |
| Incompatibility | unknown required format version | fail closed as incompatibility, not generic repair |
| Benign crash debris | unlisted Segment file, recognized temporary | quarantine via repair; source untouched |

## What NOT to do

- Do **not** edit, truncate, or `rm` files in the production data directory to “fix” recovery.
- Do **not** run repair while `glyphastored` holds the lock.
- Do **not** run repair with a non-empty workspace.
- Do **not** use live/hot repair — not supported.
- Do **not** invoke `glyphastore_rebuild_index` for durable v1; durable Indexes rebuild through
  ordinary Store recovery. The tool exits `1` with an explicit error.
- Do **not** place quarantine directories **inside** the live store catalog path; use the explicit
  workspace layout above.

## Post-incident checklist

1. Retain original source directory read-only for evidence.
2. Retain `<workspace>/quarantine/` and verify JSON/text tool output.
3. Verify repaired or restored store before traffic: `glyphastore_verify_store` then `READY`.
4. Root-cause: filesystem, hardware, abrupt power loss, or operator error — see
   [platform durability evidence](../architecture/platform-durability-evidence.md).

## Command summary

```bash
glyphastore_verify_store   [--json] [--no-scan] -- <DATA-DIR>
glyphastore_inspect_segment [--json] [--no-scan] -- <SEGMENT-FILE>
glyphastore_repair_store   [--json] [--no-scan] -- <SOURCE-DIR> <EMPTY-WORKSPACE>
glyphastore_backup_store   [--json] [--no-scan] -- <SOURCE-DIR> <EMPTY-DEST>   # when salvage impossible
```
