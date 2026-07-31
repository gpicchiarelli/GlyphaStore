# Worker count change (offline reshard)

Status: descriptive
Applies to: durable data directories
Owner: persistence maintainers
Last reviewed: 2026-07-31

Use this runbook when you must change the persisted Worker count of a durable Store. Ordinary
reopen cannot change Worker count. Policy:
[version lifecycle](../architecture/version-lifecycle.md),
[store migration](../architecture/store-migration.md), [ADR 0024](../adr/0024-offline-worker-migration.md).

## Prerequisites

- Disk space for a second full logical copy of live data.
- Source Store fully stopped.
- Known target Worker / shard-pair count `N` in `[1, 256]`, matching future daemon `--shard-pairs`
  (`--workers` is the 0.1.x alias for the same setting).

## Steps

1. `glyphastore_verify_store -- /var/lib/glyphastore`
2. Optional: `glyphastore_backup_store -- /var/lib/glyphastore /var/backups/glyphastore-$(date +%Y%m%d)`
3. `glyphastore_migrate_store --workers 4 -- /var/lib/glyphastore /var/lib/glyphastore-w4`
4. On interrupt, re-run the same command (resumes from `/var/lib/glyphastore-w4.migrate-state`).
5. Point the daemon at the new directory: `glyphastored --data-dir /var/lib/glyphastore-w4 --shard-pairs 4 ...`
6. After soak, retain or delete the old directory deliberately.

## Upgrade without reshard

Stop writers, verify, start the newer binary with the **same** `--shard-pairs` (or `--workers` alias).

## Downgrade without format bump

If the older binary still implements persistence v1 and the Store encodes only v1, verify then start
the older binary with the same Worker count. Newer required format versions fail closed.
