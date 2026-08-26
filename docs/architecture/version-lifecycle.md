# Version lifecycle and compatibility

Status: normative for 0.x promises; descriptive for future 1.0 support windows
Applies to: persistence v1, wire protocol v2, C ABI v1, public C++ API before 1.0
Owner: persistence maintainers
Last reviewed: 2026-08-01

This is the published upgrade, downgrade, Worker-count, ABI/API, and release-artifact policy for
GlyphaStore before and at alpha. It amends the development-only matrix in
[format-compatibility.md](format-compatibility.md) with operator-facing promises. Architectural
decision: [ADR 0024](../adr/0024-offline-worker-migration.md). Machine-readable N↔N-1 rows:
[n-n1-compatibility.md](n-n1-compatibility.md) and
[`engineering/compatibility/n-n1-matrix.yaml`](../../engineering/compatibility/n-n1-matrix.yaml).
Operator procedures: [compatibility-and-migration](../operations/compatibility-and-migration.md).

## Release levels and what is promised

| Level | Disk / wire | C ABI | Embedded C++ API / ABI | Worker count change |
|---|---|---|---|---|
| Prototype | No release claim | ABI 1 contract may be implemented; evidence gates still apply | Unstable / no ABI promise | Unsupported |
| Alpha (0.x durable) | Persistence **v1** reopen within declared codec rows; wire **v2** exact | ABI major 1 symbols/layout/semantics preserved after first publication | Source may break across minors; no C++ ABI promise | Offline migrate only ([store-migration](store-migration.md)) |
| Beta | Same plus tested upgrade/downgrade notes per release | Old-binary/new-library matrix required | Feature-complete contracts; C++ ABI still not promised | Offline migrate; online still out of scope |
| RC / Stable | Supported upgrade paths and support lifetime | Declared ABI-major support window | SemVer API; C++ ABI only if separately declared | Separate project for online reshard |

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
2. A correctly checksummed **newer** required version is rejected fail-closed. Synthetic coverage:
   checksum-valid Manifest format v2 and a future Record version pinned in the Manifest refuse
   `verify_durable_store_path`, `restore_durable_store`, and `Store::open`
   (`tests/unit/store_backup_tests.cpp`, HAZ-022 / `STORE-FUTURE-REQUIRED`).
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
| Online / live reshard | **No** (deferred; design constraints in [ADR 0033](../adr/0033-online-rebalance-deferred.md)) |

See [store-migration](store-migration.md) and [worker-resharding](../operations/worker-resharding.md).

## Public APIs and ABIs

Before `1.0`: no C++ ABI stability; consumers rebuild; patch releases preserve documented source
behavior; minors may break source with changelog notes. Disk/wire follow encoded versions.

The C facade is separately versioned by `ABI_VERSION`. ABI major 1 is governed by ADR 0038 and
[`docs/spec/c-abi-v1.md`](../spec/c-abi-v1.md); product `0.x` does not cancel or infer that ABI.
Until a complete prior official ABI-1 release is retained, same-build tests and the implemented
producer establish mechanism but no release note may claim demonstrated N−1 binary compatibility.

## Released-artifact evidence

| Layout | Role |
|---|---|
| `tests/fixtures/*.hex` | Current-tree canonical codecs |
| `tests/fixtures/released/<label>/` | Optional dropped fixture trees from a tag or packaging script |
| `scripts/package-release-compatibility-artifacts.sh` | Packages current fixtures for a release label |
| `tests/fixtures/released-stores/<semver>/` | Complete stopped Store from a prior tagged release |
| `engineering/tools/persistence_fixture.py` | Creates, validates, and strictly selects complete Store drops |
| `glyphastore-abi-v<major>-consumer-<version>-linux-<arch>.tar.xz` | Sealed compiled consumer used by the next ABI release matrix |
| `engineering/tools/prior_release.py` | Validates a complete official prior release before cross-version use |
| `scripts/package-release-claim.sh` | Writes `engineering/claims/<tag>.yaml` for a version tag |
| `engineering/claims/` | Claim ceiling + gate/evidence pointers per tag |

Until tagged trees are committed into `tests/fixtures/released/` as a release-process step, permanent
cross-release binary evidence still depends on those drops; CI already packages and decodes
self/tag artifacts on every push/PR and tag, and packages a claim YAML on version tags.

## Explicit non-goals (persistence v1)

- Online Worker resharding or dual-ownership routing tables
- In-place Worker-count rewrite on open
- C++ ABI guarantees before 1.0 (the independent C ABI v1 is the supported binary boundary)
- Automatic downgrade of future format versions
- Preserving sequence numbers or Store identity across migrate
