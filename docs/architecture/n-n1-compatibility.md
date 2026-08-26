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

Each release candidate retains its compiled reference wire-v2 client as a deterministic sealed
asset. On a later tag, the release producer revalidates the complete prior release and its Sigstore
attestation, then runs new/new, old-client/new-server, and new-client/old-server operations without
rebuilding either daemon. The mechanism is implemented; no N−1 wire evidence exists until a
complete prior release exists, and a same-build local run is only a harness proof.

## ABI

The installed C++ ABI remains not promised before product 1.0
(`ABI-CPP-0X-NO-STABLE`; the CMake package uses SameMinorVersion on 0.x).

The shared C facade has its own version line. `ABI-C1-CURRENT` supports an ABI-1 consumer against the
same ABI-1 build. `ABI-C1-N-MINUS-1` remains `not_promised` as an evidence statement until a complete
prior official release exists. Candidates now package a deterministic compiled consumer, and the
same-run producer revalidates the old seal, release policy, annotated tag and Sigstore attestation
before proving both dynamic-library directions. The matrix deliberately does not confuse this
implemented mechanism with cross-release proof that cannot yet exist.

## Released fixtures

In-tree `tests/fixtures/released/self-v1/` plus CI packaging via
[`release-compat.yml`](../../.github/workflows/release-compat.yml) prove the harness. Committing a
prior tagged N−1 tree into `tests/fixtures/released/<label>/` remains a **release-process** step
when tags exist; until then the YAML matrix is the policy record. Schema-2 drops use a flat,
validated label and bind their exact persistence/wire versions, fixture count and SHA-256 manifest;
existing labels are never overwritten by the packaging script.

Complete durable Stores use the separate
`tests/fixtures/released-stores/<semver>/` contract. Each drop binds tag, commit, producer artifact
digest, Worker count, probe bytes, and every Store file in a closed checksum inventory. The release
producer selects only the newest valid version strictly older than the candidate, then runs the
installed candidate daemon and offline tools without rebuilding the engine. The directory is empty
until the first tag can become a genuine prior baseline; `self-v1` codec vectors never satisfy this
gate.
