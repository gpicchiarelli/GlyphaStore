<!-- GENERATED FILE. Authority: engineering/gates/*.yaml -->

# Quality gates

Generated from `engineering/gates/`. See also the derived
[production-readiness](../production-readiness.md) view.

| ID | Section | State | Release | Requirements |
| --- | --- | --- | --- | --- |
| `GATE-API-ABI-POLICY` | public_contract | `SPECIFICATA` | `alpha` | `GS-COMPAT-FIXTURE-001` |
| `GATE-AUTH-TRANSPORT` | operations_security | `PROVATA_IN_CI` | `beta` | `GS-SEC-PROFILE-001` |
| `GATE-BACKUP-RESTORE` | durability_recovery | `IMPLEMENTATA` | `beta` | `GS-OPS-BACKUP-001` |
| `GATE-CMAKE-INSTALL` | distribution_lifecycle | `PROVATA_IN_CI` | `alpha` | `GS-CORE-API-001` |
| `GATE-CONCURRENCY-SPEC` | public_contract | `IMPLEMENTATA` | `alpha` | `GS-CONCUR-PAIR-001`, `GS-PROTO-WIRE-001`, `GS-CORE-CLOSE-001` |
| `GATE-CONFIG` | operations_security | `PROVATA_IN_CI` | `alpha` | `GS-OPS-CONFIG-001` |
| `GATE-DISK-WIRE-VERSIONS` | public_contract | `PROVATA_IN_CI` | `alpha` | `GS-COMPAT-FIXTURE-001`, `GS-PROTO-WIRE-001` |
| `GATE-DURABLE-ACK` | durability_recovery | `PROVATA_IN_CI` | `beta` | `GS-PERSIST-ACK-001` |
| `GATE-FAIL-CLOSED-IO` | durability_recovery | `PROVATA_LOCALMENTE` | `beta` | `GS-RECOVERY-FAILCLOSED-001` |
| `GATE-FAULT-INJECTION` | verification | `IMPLEMENTATA` | `beta` | `GS-RECOVERY-FAILCLOSED-001` |
| `GATE-FUZZ-CI` | verification | `PROVATA_IN_CI` | `alpha` | `GS-RECOVERY-FAILCLOSED-001` |
| `GATE-INSTALL-CONSUMER` | distribution_lifecycle | `PROVATA_IN_CI` | `alpha` | `GS-CORE-API-001` |
| `GATE-OPS-RUNBOOKS` | operations_security | `PROVATA_IN_CI` | `alpha` | `GS-OPS-BACKUP-001`, `GS-OPS-CONFIG-001` |
| `GATE-PERFORMANCE` | verification | `PROVATA_IN_CI` | `beta` | `GS-PERF-REGRESSION-001` |
| `GATE-PUBLIC-API-OWNERSHIP` | public_contract | `PROVATA_IN_CI` | `alpha` | `GS-CORE-API-001` |
| `GATE-RECOVERY-DETERMINISTIC` | durability_recovery | `PROVATA_IN_CI` | `beta` | `GS-RECOVERY-DET-001` |
| `GATE-RELEASE-MATRIX` | distribution_lifecycle | `IMPLEMENTATA` | `rc` | `GS-COMPAT-FIXTURE-001` |
| `GATE-REPRO-SBOM` | distribution_lifecycle | `IMPLEMENTATA` | `rc` | `GS-COMPAT-FIXTURE-001` |
| `GATE-SOAK` | verification | `PROVATA_IN_CI` | `beta` | `GS-OPS-CONFIG-001` |
| `GATE-TELEMETRY` | operations_security | `PROVATA_IN_CI` | `alpha` | `GS-OPS-CONFIG-001` |
| `GATE-TEST-SUITES` | verification | `PROVATA_IN_CI` | `alpha` | `GS-RECOVERY-DET-001`, `GS-COMPAT-FIXTURE-001` |
| `GATE-THREAT-SUPPLY` | operations_security | `IMPLEMENTATA` | `beta` | `GS-SEC-PROFILE-001` |
| `GATE-VERSION-LIFECYCLE` | distribution_lifecycle | `PROVATA_IN_CI` | `alpha` | `GS-COMPAT-FIXTURE-001` |
| `GATE-WRITE-ORDER` | durability_recovery | `PROVATA_IN_CI` | `beta` | `GS-PERSIST-ORDER-001` |
