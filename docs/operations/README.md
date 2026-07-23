# Operations runbooks

Status: descriptive
Applies to: durable `glyphastored` deployments and offline maintenance tools
Owner: persistence and platform maintainers
Last reviewed: 2026-07-23

Concise operator procedures for production incidents and planned maintenance. These runbooks
describe **implemented** behavior; they do not redefine wire or disk contracts. Normative rules live
in [cli.md](../cli.md), [wire protocol v2](../spec/wire-protocol-v2.md), and
[backup-restore](../architecture/backup-restore.md).

| Runbook | When to use |
|---|---|
| [Graceful drain and overload](graceful-drain-and-overload.md) | Rolling restart, deploy, capacity pressure, `OVERLOADED` responses |
| [Backup and restore](backup-restore.md) | Planned copy, migration to new host, disaster recovery from verified backup |
| [Corruption detection and repair](corruption-repair.md) | Startup failure, verify errors, namespace anomalies, post-incident salvage |

## Tool and exit-code contract

All maintenance tools share the CLI contract in [cli.md](../cli.md):

- exit `0` — success
- exit `1` — validation, I/O, or repair failure (fail closed)
- exit `2` — usage error

Stop every writer (`glyphastored` or embedded `Store`) that holds the data-directory lock before
offline verify, backup, or repair. Live/hot backup and in-place rewrite are **not** supported.

## Related architecture

- [Server model](../architecture/server-model.md) — bounded queues, drain-before-close, probe semantics
- [Durability and recovery](../architecture/durability-recovery.md) — recovery never mutates source files
- [Namespace policy](../architecture/namespace-policy.md) — quarantine rules and unsafe entries
