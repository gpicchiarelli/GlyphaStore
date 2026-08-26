Status: normative for 0.1.x policy rows; descriptive for future releases
Applies to: store open, wire server/client, ABI promises across N↔N-1
Owner: maintainers
Last reviewed: 2026-08-01

# N↔N-1 compatibility matrix

Machine-readable authority: [`engineering/compatibility/n-n1-matrix.yaml`](../../engineering/compatibility/n-n1-matrix.yaml).
Validator: `python3 engineering/tools/validate_compat_matrix.py`.

This matrix states what GlyphaStore **supports**, **intentionally rejects**, or **does not promise**
when a writer/server at version N meets a reader/client at N−1 (or the reverse). It does not
advance the release claim ceiling beyond *architectural prototype*.

## Store open (durable directory)

| ID | Writer | Reader | Status |
| --- | --- | --- | --- |
| `STORE-SAME-LINE` | 0.1.x | 0.1.x | supported (same Worker count, persistence v1) |
| `STORE-WORKER-RESHARD` | 0.1.x | 0.1.x | supported offline only (`glyphastore_migrate_store`) |
| `STORE-PRE-V1` | pre-v1 | 0.1.x | unsupported |
| `STORE-FUTURE-REQUIRED` | future required version | 0.1.x | intentionally rejected (fail closed) |
| `STORE-DOWNGRADE-REWRITE` | 0.1.x | older required | not promised |

## Wire (server ↔ client)

| ID | Pair | Status |
| --- | --- | --- |
| `WIRE-V2-SAME` | 0.1.x ↔ 0.1.x on wire v2 | supported |
| `WIRE-V1-OR-UNKNOWN` | non-v2 client/server | intentionally rejected |
| `WIRE-CROSS-MAJOR-FUTURE` | future major ↔ 0.1.x | not promised |

## ABI

`ABI-0X-NO-STABLE`: no ABI stability before 1.0 (CMake package uses SameMinorVersion on 0.x).

## Released fixtures

In-tree `tests/fixtures/released/self-v1/` plus CI packaging via
[`release-compat.yml`](../../.github/workflows/release-compat.yml) prove the harness. Committing a
prior tagged N−1 tree into `tests/fixtures/released/<label>/` remains a **release-process** step
when tags exist; until then the YAML matrix is the policy record. Schema-2 drops use a flat,
validated label and bind their exact persistence/wire versions, fixture count and SHA-256 manifest;
existing labels are never overwritten by the packaging script.
