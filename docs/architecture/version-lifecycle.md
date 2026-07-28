# Version lifecycle and compatibility

Status: normative for 0.x promises; descriptive for future 1.0 support windows
Applies to: persistence v1, wire protocol v2, public C++ API before 1.0
Owner: persistence maintainers
Last reviewed: 2026-07-23

This is the published upgrade, downgrade, Worker-count, ABI/API, and release-artifact policy for
GlyphaStore before and at alpha. It amends the development-only matrix in
[format-compatibility.md](format-compatibility.md) with operator-facing promises. Architectural
decision: [ADR 0024](../adr/0024-offline-worker-migration.md).

## Release levels and what is promised

| Level | Disk / wire | Embedded C++ API | C++ ABI | Worker count change |
|---|---|---|---|---|
| Prototype | No promise | Unstable | No promise | Unsupported |
| Alpha (0.x durable) | Persistence **v1** reopen within declared codec rows; wire **v2** exact | Source may break across minors with changelog notes; patches preserve documented behavior | **No ABI promise**; rebuild required | Offline migrate only ([store-migration](store-migration.md)) |
| Beta | Same plus tested upgrade/downgrade notes per release | Feature-complete contracts | Still no ABI promise unless announced | Offline migrate; online still out of scope |
| RC / Stable | Supported upgrade paths and support lifetime | SemVer API policy | Declared only at 1.0+ | Separate project for online reshard |

## Persistence v1: upgrade and downgrade

### Upgrade (newer binary, existing Store)

1. Stop every writer that holds the data-directory lock.
2. Run `glyphastore_verify_store` on the data directory.
3. Open with the newer binary using the **same** Worker count (omit override or pass the persisted
   count). Reopen is reopen-only: no silent rewrite.
4. If the newer binary requires a higher format version than the Store encodes, open fails closed.

### Downgrade (older binary, existing Store)

1. An older binary may reopen a Store only when every required Manifest/Segment/header/commit/Record
   version is one it implements.
2. A correctly checksummed **newer** required version is rejected fail-closed.
3. For pure persistence v1 (no format bump), byte-identical [backup/restore](backup-restore.md) plus
   opening with the older binary is sufficient when the older binary still implements v1.

### What reopen never does

- Change Worker count or routing algorithm
- Rewrite Manifest or Segments to a new format version
- Adopt unlisted namespace files
- Repair corruption in place

## Worker count and resharding

| Operation | Supported? |
|---|---|
| Create Store with chosen Worker count | Yes (creation-time only) |
| Reopen with matching count | Yes |
| Reopen with different count | **No** — fail closed |
| Change count via backup/restore | **No** — restore preserves catalog bytes |
| Change count via offline migrate | **Yes** — `glyphastore_migrate_store` |
| Online / live reshard | **No** (deferred; not required for persistence v1) |

See [store-migration](store-migration.md) and [worker-resharding](../operations/worker-resharding.md).

## Public C++ API and ABI

Before `1.0`: no C++ ABI stability; consumers rebuild; patch releases preserve documented source
behavior; minors may break source with changelog notes. Disk/wire follow encoded versions.

## Released-artifact evidence

| Layout | Role |
|---|---|
| `tests/fixtures/*.hex` | Current-tree canonical codecs |
| `tests/fixtures/released/<label>/` | Optional dropped fixture trees from a tag or packaging script |
| `scripts/package-release-compatibility-artifacts.sh` | Packages current fixtures for a release label |

Until tagged trees are committed into `tests/fixtures/released/` as a release-process step, permanent
cross-release binary evidence still depends on those drops; CI already packages and decodes
self/tag artifacts on every push/PR and tag.

## Explicit non-goals (persistence v1)

- Online Worker resharding or dual-ownership routing tables
- In-place Worker-count rewrite on open
- C++ ABI guarantees before 1.0
- Automatic downgrade of future format versions
- Preserving sequence numbers or Store identity across migrate
