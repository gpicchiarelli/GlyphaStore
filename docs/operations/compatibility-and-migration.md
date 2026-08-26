Status: normative procedures for 0.1.x; descriptive for unreleased majors
Applies to: persistence v1 reopen, wire v2, offline Worker migrate, released fixtures
Owner: persistence / release maintainers
Last reviewed: 2026-08-01

# Compatibility and migration manual

Operator-facing upgrade, downgrade, and Worker-count procedures for GlyphaStore **before 1.0**.
Machine-readable policy: [`engineering/compatibility/n-n1-matrix.yaml`](../../engineering/compatibility/n-n1-matrix.yaml).
Normative lifecycle: [version-lifecycle](../architecture/version-lifecycle.md),
[n-n1-compatibility](../architecture/n-n1-compatibility.md). Claim ceiling remains
**architectural prototype** until alpha gates close with retained evidence.

## Quick decision table

| Goal | Supported? | Procedure |
| --- | --- | --- |
| Reopen same data dir with same Worker count on 0.1.x | Yes | Stop writers → verify → open |
| Change Worker count | Offline only | [`glyphastore_migrate_store`](worker-resharding.md) into a **new** directory |
| Open pre-v1 / unknown required format | No | Fail closed |
| Silent downgrade rewrite of a newer required version | No | Not promised |
| Wire non-v2 client against 0.1.x server | No | Intentionally rejected |
| C++ ABI stable across 0.x builds | No | Rebuild consumers (`SameMinorVersion`) |
| Live/hot migrate or online reshard | No | Out of scope for v1 |

## 1. Same-line reopen (0.1.x → 0.1.x)

Preconditions: persistence format v1, unchanged Worker count, every writer stopped.

```bash
# 1. Stop glyphastored / close embedded Stores (directory lock must be free).
# 2. Read-only verify
glyphastore_verify_store -- /path/to/data

# 3. Start the new binary with the same Worker count (omit override, or pass the persisted count).
glyphastored --profile production --data-dir /path/to/data ...
```

Fail closed if the binary requires a higher format version than the Store encodes. Reopen never
rewrites Manifest/Segments, changes Worker count, or repairs corruption in place.

Matrix row: `STORE-SAME-LINE`.

## 2. Offline Worker-count change

Use [worker-resharding](worker-resharding.md) / [store-migration](../architecture/store-migration.md).

```bash
glyphastore_verify_store -- /path/to/source
glyphastore_migrate_store --workers N -- /path/to/source /path/to/destination
glyphastore_verify_store -- /path/to/destination
# Cut traffic to destination only after verify succeeds; keep source until rollback window ends.
```

Source is never mutated. Resume uses `<destination>.migrate-state`. Matrix row:
`STORE-WORKER-RESHARD` (`supported_offline_only`).

## 3. Downgrade and future formats

| Situation | Operator action |
| --- | --- |
| Older binary, Store still pure persistence v1 | May reopen if every required version is implemented; else fail closed |
| Store encodes a **newer required** version | Fail closed — do not force open; restore from backup taken before upgrade |
| Need an older line for salvage | Offline backup/export workflows only — not silent reopen (`STORE-DOWNGRADE-REWRITE` is `not_promised`) |

## 4. Wire clients

Only wire protocol **v2** is supported on 0.1.x (`WIRE-V2-SAME`). Non-v2 frames are rejected
before service (`WIRE-V1-OR-UNKNOWN`). Cross-major wire compatibility is `not_promised` until an
explicit matrix row and fixtures exist.

Official SDKs in-tree speak v2. After a server upgrade, roll clients only when release notes and
fixtures say the wire row remains supported.

## 5. Released fixture drops (release process)

In-tree `tests/fixtures/released/self-v1/` proves the harness. For each version tag:

```bash
./scripts/package-release-compatibility-artifacts.sh 0.1.0
./scripts/package-release-claim.sh 0.1.0 "$(git rev-parse HEAD)"
# Commit tests/fixtures/released/0.1.0/ when promoting a permanent N−1 drop (release decision).
```

CI (`.github/workflows/release-compat.yml`) packages self/tag artifacts; permanent prior-release
trees under `tests/fixtures/released/<label>/` remain a **maintainer release step**. See
[released fixtures README](../../tests/fixtures/released/README.md).

A complete Store baseline is separate from those codec vectors. After a tagged release has created
and verified a stopped Store through its installed daemon, package it with
`engineering/tools/persistence_fixture.py create` under
`tests/fixtures/released-stores/<semver>/`; follow the exact metadata and digest procedure in that
directory's README. The next release's artifact-only persistence job requires this baseline and
will not accept a self-generated candidate Store.

## 6. Backup before upgrade

Always take a verified offline or online-fenced backup before binary upgrades that touch
persistence:

- Offline: [backup-restore runbook](backup-restore.md)
- Online fenced: `Store::backup_to` / wire `BACKUP` (admission pause; not zero-fence hot copy)

Zero-fence hot backup remains unsupported.

## Related

- [Operations index](README.md)
- [Release checklist](../assurance/release-checklist.md)
- ADR [0024 offline Worker migration](../adr/0024-offline-worker-migration.md)
