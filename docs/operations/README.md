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
| [Durable TCP daemon](durable-tcp-daemon.md) | End-to-end durable `glyphastored` setup: profile/mode, flags, probes, drain, offline ops |
| [Graceful drain and overload](graceful-drain-and-overload.md) | Rolling restart, deploy, capacity pressure, `OVERLOADED` responses |
| [Backup and restore](backup-restore.md) | Planned copy, migration to new host, disaster recovery from verified backup |
| [Worker count change](worker-resharding.md) | Offline reshard / logical rewrite via `glyphastore_migrate_store` |
| [Corruption detection and repair](corruption-repair.md) | Startup failure, verify errors, namespace anomalies, post-incident salvage |
| [Soak profiles](soak.md) | Smoke / 30m / 1h / 4h durable soak; CI vs multi-hour honesty |
| [E3 pinned-row campaign](e3-campaign.md) | Operator E0→E2→E3 campaign-prep tarball; promotion gate (no false E3 cert) |
| [Secure profile certs](secure-profile-certs.md) | Rotate TLS/mTLS material; revoke via authz map and/or CRL |

## Tool and exit-code contract

All maintenance tools share the CLI contract in [cli.md](../cli.md):

- exit `0` — success
- exit `1` — validation, I/O, or repair failure (fail closed)
- exit `2` — usage error

Stop every writer (`glyphastored` or embedded `Store`) that holds the data-directory lock before
offline verify, backup, repair, or Worker migrate. Live/hot backup, in-place rewrite, and online
reshard are **not** supported.

## CI / staging exercise

Automated smoke (not multi-hour certification):

```bash
./scripts/exercise_ops_runbooks.sh
./scripts/soak_daemon.sh                 # profile smoke ~45s
./scripts/soak_daemon.sh --profile long  # 30m
./scripts/soak_daemon.sh --profile 1h    # multi-hour path + RSS/STATS samples
```

Profiles and CI wiring: [soak.md](soak.md). GitHub Actions:
`.github/workflows/ops-runbooks.yml` runs the runbook exercise on every PR/push and a weekly
30-minute soak; `.github/workflows/soak-extended.yml` is **workflow_dispatch / monthly 1h only**
(not every PR). `.github/workflows/durability-evidence.yml` archives E2 collector metadata (and
scheduled process-kill) plus linux-ext4 E3 harness smoke; those uploads are rehearsal artifacts,
not E3/E4 certification. For a maintainer-run pinned campaign (still `e3_certified=no` until human
promotion), use [e3-campaign.md](e3-campaign.md) and `scripts/run-e3-campaign.sh`.

## Related architecture

- [Server model](../architecture/server-model.md) — bounded queues, drain-before-close, probe semantics
- [Durability and recovery](../architecture/durability-recovery.md) — recovery never mutates source files
- [Namespace policy](../architecture/namespace-policy.md) — quarantine rules and unsafe entries
