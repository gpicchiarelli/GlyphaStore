Status: descriptive operator handbook (index + incident playbooks)
Applies to: durable `glyphastored` and offline maintenance tools
Owner: platform / ops maintainers
Last reviewed: 2026-08-01

# Operations handbook

Single entry point for running GlyphaStore in production-like deployments. Detailed procedures stay
in the linked runbooks; this handbook orders them for day-2 operations and incidents. Normative
contracts live in [cli.md](../cli.md), [wire protocol v2](../spec/wire-protocol-v2.md), and the
specs under `docs/spec/`. Claim ceiling remains *architectural prototype*.

## 1. Day-1: bring a durable daemon up

1. Choose profile (`production` / `embedded`) and an absolute `--data-dir`.
2. `glyphastored … --dump-config` and review abuse, maintenance, and queue caps.
3. Start the process; confirm log `listen` / `ready` (or `--log-format json` equivalents).
4. Probe `READY` before adding to a load balancer; never use `HEALTH` alone.

Guide: [durable-tcp-daemon](durable-tcp-daemon.md). Observability catalog:
[observability](observability.md).

## 2. Day-2: steady state

| Task | Where |
| --- | --- |
| Readiness / overload | [graceful-drain-and-overload](graceful-drain-and-overload.md), [observability](observability.md) |
| Soak / RSS growth | [soak](soak.md) |
| Secure TLS/mTLS/CRL | [secure-profile-certs](secure-profile-certs.md), [secure-profile](../security/secure-profile.md) |
| Compatibility / upgrades | [compatibility-and-migration](compatibility-and-migration.md) |
| Release packaging | [release-checklist](../assurance/release-checklist.md) |

## 3. Rolling restart / deploy

1. Remove instance from LB (or wait until `READY` fails in preStop).
2. `SIGTERM` / `SIGINT`; honor `--shutdown-drain-ms`.
3. Confirm process exit; start new binary with same Worker/shard-pair count.
4. Wait for `READY` before returning traffic.

Details: [graceful-drain-and-overload](graceful-drain-and-overload.md).

## 4. Backup, restore, Worker count

| Goal | Path |
| --- | --- |
| Cold verified copy | Offline [backup-restore](backup-restore.md) (preferred for release artifacts) |
| Live fenced copy | Online `BACKUP` / `Store::backup_to` (admission fence; not zero-fence hot) |
| Change Worker count | Offline [worker-resharding](worker-resharding.md) into a **new** directory |

Normative snapshot boundary: [backup-restore v1](../spec/backup-restore-v1.md). Online reshard is
**not** supported (ADR 0024; design constraints in [ADR 0033](../adr/0033-online-rebalance-deferred.md)).

## 5. Incident playbooks

### 5.1 Traffic refused / `OVERLOADED`

1. Check `READY` and STATS `lane[N].rejected`, `abuse_*`, queue depths.
2. If `mutations_rejected` / maintenance emergency: shed load; inspect maintenance needles.
3. Scale or reduce admit rates; do not blindly retry overloaded mutations (client semantics).

### 5.2 Startup / verify failure

1. Stop writers; run `glyphastore_verify_store`.
2. Follow [corruption-repair](corruption-repair.md); never in-place rewrite production dirs.
3. Restore from last verified backup into a new path; cut over only after verify.

### 5.3 Suspected data loss after crash

1. Do not delete the data directory.
2. Capture E2/E3 rehearsal artifacts only with the honesty labels in [e3-campaign](e3-campaign.md)
   (`e3_certified=no` until human promotion).
3. Prefer verify → repair workspace → restore from backup over ad-hoc file edits.

### 5.4 Certificate / auth incidents

Rotate material and CRL per [secure-profile-certs](secure-profile-certs.md). Prefix principals need
`admin` for STATS (ADR 0027).

## 6. Signals cheat sheet

| Intent | Signal |
| --- | --- |
| Liveness | `HEALTH` / log `listen` |
| Traffic gate | `READY` / log `ready` |
| Admin snapshot | `STATS` (see [observability](observability.md)) |
| Effective config | `--dump-config` |

## 7. Tool exit codes

Shared by maintenance CLIs ([cli.md](../cli.md)): `0` success, `1` fail closed, `2` usage.

## 8. Related index

Full runbook table: [operations README](README.md). Assurance posture:
[final-engineering-report](../assurance/final-engineering-report.md).
