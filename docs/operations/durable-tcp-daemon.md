# Operator guide: durable TCP daemon

Status: descriptive
Applies to: `glyphastored` wire protocol v2 with durable storage
Owner: platform and persistence maintainers
Last reviewed: 2026-07-23

End-to-end checklist for running a durable `glyphastored` instance. Normative CLI and wire rules
live in [cli.md](../cli.md) and [wire protocol v2](../spec/wire-protocol-v2.md); this guide ties
configuration, probes, drain, and offline maintenance into one completeness story.

## 1. Choose storage mode and open policy

### Deployment profile (recommended)

Profiles apply preset values between hardcoded defaults and file/env/CLI overrides. Unknown profile
names fail closed before listen.

| Profile | Storage | Maintenance | Typical use |
|---|---|---|---|
| `dev` | `volatile` | disabled | local iteration only — **not** for durable data |
| `embedded` | `durable-periodic` | background | sidecar / single-node with tighter caps |
| `production` | `durable-periodic` | background | standard durable deployment |

Every durable profile still requires an explicit data directory. Profiles do **not** invent a path.

```bash
glyphastored --profile production --data-dir /var/lib/glyphastore \
  --bind 0.0.0.0 --port 7379
```

Equivalent config file:

```ini
profile = production
data-dir = /var/lib/glyphastore
bind = 0.0.0.0
port = 7379
```

Precedence: **defaults < profile < config file < environment < CLI**. Validate the resolved
settings without listening:

```bash
glyphastored --profile production --data-dir /var/lib/glyphastore --dump-config
```

### Explicit storage mode

When a profile is wrong for your durability contract, set `--storage-mode` explicitly. All durable
modes require `--data-dir` (or `data-dir` / `GLYPHASTORE_DATA_DIR`).

| Mode | Acknowledgement contract (summary) |
|---|---|
| `durable-sync` | each mutation durable before wire success |
| `durable-periodic` | batched flush on interval and size limits (profile default) |
| `durable-group` | strict group commit with bounded concurrent producers |

```bash
glyphastored --storage-mode durable-sync --data-dir /var/lib/glyphastore ...
```

Durable-only flags (`--open-mode`, batch limits, resource caps) are rejected with volatile storage.

### Open policy (`--open-mode`)

| Mode | Behavior |
|---|---|
| `open-or-create` | default — create an empty Store or open an existing one |
| `create-new` | fail if the directory already contains a Store |
| `open-existing` | fail if no Store exists (restore/migration cutover) |

Invalid combinations and missing `--data-dir` for durable modes fail before the process binds a
socket.

## 2. Resource, batch, and maintenance flags that matter

Tune these after `--dump-config` on representative hardware. Defaults are safe starting points;
embedded profile lowers several caps automatically.

### Durable resource limits

| Flag | Default (production) | Purpose |
|---|---|---|
| `--max-store-bytes` | 8 GiB | cap catalog Segment bytes |
| `--reserved-free-bytes` | 256 MiB | keep filesystem headroom unused |
| `--max-segments` | 127 | cap Segment count |
| `--max-hot-cache-bytes` | 256 MiB | cap cross-Worker hot cache (`0` disables) |
| `--max-temporary-compaction-bytes` | 1 GiB | cap compaction temporary peak |

Embedded profile preset: 1 GiB store, 64 MiB reserved, 32 segments, 64 MiB hot cache, 256 MiB
temporary compaction.

### Batch / flush tuning

For `durable-periodic` (and periodic batch fields shared with group limits):

| Flag | Default | Purpose |
|---|---|---|
| `--sync-interval-ms` | 1000 | periodic flush interval |
| `--group-max-records` | 4096 (periodic) / 32 (group mode) | batch record limit |
| `--group-max-bytes` | 4 MiB / 64 KiB | batch byte limit |
| `--group-max-wait-ms` | 1000 / 10 | batch close wait |

For `durable-group`, `--durable-group-concurrency` (default 4) bounds strict-group producers per
Worker.

### Admission and overload bounds

| Flag | Default | Purpose |
|---|---|---|
| `--durable-mutation-queue-capacity` | 256 | admitted mutations per Worker |
| `--durable-mutation-queue-bytes` | 16 MiB | owned mutation bytes per Worker |
| `--durable-mutation-queue-wait-ms` | 1000 | expire queued work before Store entry (`0` disables) |

When limits or maintenance emergency are hit, clients see `OVERLOADED` — treat as **not committed**
without application reconciliation ([client semantics v1 §5](../spec/client-semantics-v1.md)).

### Maintenance

| Flag | Default | Purpose |
|---|---|---|
| `--maintenance-mode` | `background` | `cooperative`, `background`, or `disabled` |
| `--maintenance-max-copy-bytes-per-cycle` | 128 MiB | normal compaction copy limit (`0` = unlimited) |
| `--maintenance-max-copy-bytes-per-sec` | 0 | normal one-second copy rate (`0` = unlimited) |
| `--maintenance-max-cpu-ms-per-window` | 0 | normal one-second compact CPU budget (`0` = unlimited) |
| `--maintenance-suspend-on-p99-latency-ms` | 0 | defer normal compaction when the latest durable mutation p99 reaches this threshold (`0` = disabled; pressure/emergency bypass) |
| `--maintenance-suspend-on-p99-min-samples` | 32 | minimum completed mutations required for a latency decision |
| `--maintenance-max-latency-deferral-ms` | 30000 | admit one normal reclaim attempt after continuous latency deferral (`0` = wait until pressure) |
| `--maintenance-unread-ttl-pressure-probe` | true | probe unread expired puts under pressure |
| `--maintenance-unread-ttl-normal-scheduling` | false | opt-in: count unread expired bytes in normal dead-byte threshold |

Pressure/emergency maintenance bypass the normal copy and rate limits to recover capacity. Sticky
maintenance faults fail `READY` until resolved or the Store is replaced from backup.

## 3. HEALTH / READY / STATS expectations

Wire opcodes 7–9 are accepted before `INIT`/`BIND_WORKER` and do not mutate Store state. Failed
probes return `INTERNAL_ERROR` with an empty value.

| Probe | Success value | Use for |
|---|---|---|
| `HEALTH` (7) | `GlyphaStore/live` | process / executor liveness only |
| `READY` (8) | `GlyphaStore/ready` | load balancer readiness — admission open, not shutting down, catalog healthy, maintenance not in emergency or sticky fault |
| `STATS` (9) | bounded ASCII report | admin snapshot: version, live/ready, connections, durable lane/batch counters, per-lane latency histograms (`queue_wait_ns` / `service_ns`), maintenance fields |

**Fail closed for traffic:** orchestrators must gate on `READY`, not `HEALTH` alone. During
graceful drain or sticky storage faults, `HEALTH` may succeed while `READY` fails.

`STATS` requires the `read` capability under `--authz-map` / `--secure-profile` for unscoped
principals; prefix-scoped principals need `admin` ([ADR 0027](../adr/0027-stats-isolation-prefix-principals.md),
[secure-profile reference](../security/secure-profile.md)). On cleartext trusted binds it remains
an unauthenticated admin surface — restrict network access accordingly. Phase 5 exports `abuse_*`
reject/close counters when limits are enabled.

Structured lifecycle logs (`--log-format json`) emit `ready`, `maintenance_emergency`,
`maintenance_fault`, and shutdown drain events on stderr for log aggregation.

Full probe and overload detail: [graceful drain and overload runbook](graceful-drain-and-overload.md).

## 4. Shutdown drain

`SIGINT` and `SIGTERM` trigger orderly shutdown:

1. stop accepting new TCP connections;
2. drain existing connections (idle close after in-flight responses flush);
3. drain durable mutations already admitted to Store execution;
4. close the Store and release the data-directory lock;
5. exit.

The drain deadline is `--shutdown-drain-ms` (default **30s**; **`0` waits unbounded**). Queued
mutations that never enter Store execution before the deadline complete as `unavailable` on the
wire. In-flight Store work is **never** cancelled. A timed-out drain makes process exit fail closed
(non-zero exit).

```bash
glyphastored --shutdown-drain-ms 120000 --data-dir /var/lib/glyphastore ...
# GLYPHASTORE_SHUTDOWN_DRAIN_MS=120000
```

Procedure: remove from load balancing → confirm `READY` fails → `SIGTERM` → verify clean exit and
lock release → restart → confirm `READY` before returning traffic. Do **not** use `SIGKILL` for
routine restarts.

## 5. Backup, verify, and repair (offline)

All maintenance tools require **stopped writers**. The daemon must not hold the data-directory lock.

| Task | Tool / runbook |
|---|---|
| Structural verify | `glyphastore_verify_store` — [cli.md § verify](../cli.md#glyphastore_verify_store), [corruption-repair runbook](corruption-repair.md) |
| Offline backup / restore | `glyphastore_backup_store` — [backup-restore runbook](backup-restore.md) |
| Salvage with quarantine | `glyphastore_repair_store` — [corruption-repair runbook](corruption-repair.md) |
| Segment forensics | `glyphastore_inspect_segment` — [corruption-repair runbook](corruption-repair.md) |

Typical planned maintenance:

```bash
systemctl stop glyphastored
glyphastore_verify_store -- /var/lib/glyphastore
install -d -m 700 /backup/glyphastore-$(date +%F)
glyphastore_backup_store -- /var/lib/glyphastore /backup/glyphastore-$(date +%F)
systemctl start glyphastored
```

Restore always targets a **new empty** directory; cut over at the orchestration layer (symlink,
mount, or config), not by overwriting the live path in place.

## 6. What is NOT supported (fail closed)

| Claim or shortcut | Status |
|---|---|
| Live / hot backup while `glyphastored` holds the lock | **Not supported** — backup tools fail closed if the directory is locked |
| In-place restore over production data | **Forbidden** — copy into a new path, verify, then swap |
| Filesystem snapshot without stopped writers | **Insufficient** — freeze writers and run `glyphastore_verify_store` on the image |
| `glyphastore_rebuild_index` for durable v1 | **Permanently refused** — Indexes rebuild via Store recovery or `glyphastore_repair_store` |
| E3/E4 sudden power-loss certification | **Pending** — process-kill (E2), E3 harness, and operator campaign-prep (`scripts/run-e3-campaign.sh`) exist; no pinned native filesystem row is certified ([platform durability evidence](../architecture/platform-durability-evidence.md), [E3 campaign](e3-campaign.md)) |
| `HEALTH` as readiness during deploys | **Wrong signal** — use `READY` |
| Blind retry after `OVERLOADED` | **Unsafe** — same logical mutation may be uncommitted; reconcile or start a new attempt |

NFS, SMB, FUSE, overlay, and remote/user-space storage are outside the local-filesystem contract
([persistence v1](../spec/persistence-v1.md)).

## Quick reference

```bash
# Start (production durable)
glyphastored --profile production --data-dir /var/lib/glyphastore --bind 0.0.0.0 --port 7379

# Validate config
glyphastored --profile production --data-dir /var/lib/glyphastore --dump-config

# Graceful stop (orchestrator preStop)
kill -TERM "$(pidof glyphastored)"

# Offline verify + backup (daemon stopped)
glyphastore_verify_store -- /var/lib/glyphastore
glyphastore_backup_store -- /var/lib/glyphastore /backup/empty-dest
```

## Related documentation

- [CLI reference](../cli.md) — full flag list, config precedence, maintenance tools
- [Operations runbooks](README.md) — drain, backup, corruption procedures
- [Server model](../architecture/server-model.md) — bounded queues, reactor/durable lanes
- [Production readiness](../production-readiness.md) — release gates and evidence levels
- [v1 production roadmap](../v1-production-roadmap.md) — P0-01 daemon durability backlog
