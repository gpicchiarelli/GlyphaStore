# Runbook: graceful drain and overload

Status: descriptive
Applies to: `glyphastored` wire protocol v2
Owner: platform maintainers
Last reviewed: 2026-08-01

## Purpose

Stop or restart a daemon without abandoning admitted durable work, and recognize overload before it
becomes unbounded memory growth or silent data loss.

Assurance linkage: requirement `GS-OPS-SOAK-001`, gate `GATE-OPS-RUNBOOKS`, budget class
`OVERLOAD-RUNBOOK` in
[`engineering/performance/budgets.yaml`](../../engineering/performance/budgets.yaml).

## Preconditions

- Durable deployments use a persistent `--data-dir` (or config/env equivalent).
- Load balancers or orchestrators should treat `READY` failure as **not ready for traffic**; `HEALTH`
  alone is insufficient during drain or sticky storage faults.

## Probe semantics (fail closed)

Wire opcodes 7–9 are accepted before `INIT`/`BIND_WORKER` and do not mutate Store state. See
[wire protocol v2 §4](../spec/wire-protocol-v2.md).

| Probe | Success value | Meaning |
|---|---|---|
| `HEALTH` (7) | `GlyphaStore/live` | Process and executors are live |
| `READY` (8) | `GlyphaStore/ready` | Safe for traffic: Store admission open, not shutting down, catalog healthy, maintenance not in emergency or sticky fault |
| `STATS` (9) | bounded ASCII report | Admin snapshot: version, live/ready, connections, durable lane/batch counters, maintenance fields |

Failed probes return `INTERNAL_ERROR` with an empty value. During shutdown, expect `READY` to fail
while `HEALTH` may still succeed briefly.

Use any v2 client that can send raw opcodes (official SDKs encode `HEALTH`/`READY`/`STATS`). There
is no separate probe binary; treat `STATS` as a private-admin surface until authentication is enforced
([ADR 0021](../adr/0021-secure-profile-authentication.md)).

## Graceful shutdown procedure

### 1. Stop new traffic

Remove the instance from load balancing or scale the service to zero **before** sending a stop
signal. Confirm `READY` no longer returns `OK` on the draining instance if your orchestrator supports
preStop hooks.

### 2. Send SIGTERM or SIGINT

```bash
kill -TERM "$(pidof glyphastored)"
# or: systemctl stop glyphastored
```

The daemon:

1. stops accepting new TCP connections;
2. drains existing connections (idle connections close after in-flight responses flush; new requests on
   draining connections are refused);
3. drains durable mutations already admitted to Store execution (bounded by `--shutdown-drain-ms`,
   default **30s**; **`0` waits unbounded**);
4. closes the Store and releases the data-directory lock;
5. exits.

Queued mutations that have **not** entered Store execution when the drain deadline expires complete as
`unavailable` on the wire. In-flight Store work is **never** cancelled. A timed-out drain makes
process exit **fail closed** (`join` returns an error; non-zero exit).

### 3. Tune drain deadline when needed

```bash
glyphastored --shutdown-drain-ms 120000 --data-dir /var/lib/glyphastore ...
# or in daemon.conf:
# shutdown-drain-ms = 120000
# or: GLYPHASTORE_SHUTDOWN_DRAIN_MS=120000
```

Increase the deadline when large durable batches or slow storage can exceed 30s. Use `0` only when an
orchestrator can wait indefinitely and you accept unbounded shutdown time.

### 4. Verify shutdown

- Process exited (check service manager or `wait`).
- Data directory lock released (offline tools can exclusive-lock the directory).
- If exit was non-zero after timeout, treat in-flight client mutations as **indeterminate** until
  applications reconcile ([client semantics v1 §5](../spec/client-semantics-v1.md)).

### 5. Restart

```bash
glyphastored --profile production --data-dir /var/lib/glyphastore --bind 0.0.0.0 --port 7379
glyphastored --dump-config   # optional: print resolved settings without listening
```

Confirm `READY` returns `OK` before returning the instance to load balancing.

## Overload behavior

Overload is **bounded** and **fail closed**. The server does not grow unbounded queues.

| Condition | Client-visible result | Committed? |
|---|---|---|
| Connection/handoff/input/output buffer limit | connection closed or `OVERLOADED` | not committed if rejected before Store entry |
| Durable lane count or owned-byte limit | `OVERLOADED` | known not committed |
| Queue wait expires before Store entry | `OVERLOADED` | known not committed |
| Maintenance emergency / `storage_exhausted` | `OVERLOADED` (wire cannot distinguish from admission pressure) | known not committed |
| Task already inside Store | completes or fails normally | never cancelled mid-commit |

Clients must treat `OVERLOADED` as **`retryability=never`** for the same logical mutation without
application reconciliation ([client semantics v1](../spec/client-semantics-v1.md),
[ADR 0019](../adr/0019-client-error-retry-timeout.md)). Backoff and retry only as a **new** attempt
after fixing capacity or shedding load.

### Operator response checklist

1. **`STATS`**: inspect per-shard-pair mutation lane `admitted`/`rejected`/`expired`, queue depth,
   maintenance emergency/skip reason, and rotation phase timings (`pair_writer_stats` / wire Worker
   lane counters name the same topology).
2. **Capacity**: check `--max-store-bytes`, `--reserved-free-bytes`, `--max-segments`,
   `--max-hot-cache-bytes`, connection and buffer limits via `--dump-config`.
3. **Shed load**: stop new clients, reduce shard-pair hot spots, or add instances (shard-pair /
   Worker count changes require offline migration — not an in-place rewrite).
4. **Do not** rely on killing the process to “clear queues”; use graceful drain so admitted work can
   finish within `--shutdown-drain-ms`.

## What NOT to do

- Do **not** use `SIGKILL` for routine restarts; it skips drain and may leave client mutations
  indeterminate after commit.
- Do **not** treat `HEALTH` alone as readiness during deploys or incident traffic cuts.
- Do **not** assume `OVERLOADED` will succeed on immediate blind retry; distinguish admission
  pressure from capacity exhaustion only via operator metrics, not the wire status alone.

## Related tests and evidence

Integration coverage includes graceful stop, drain timeout, and real-daemon SIGKILL paths
(`glyphastore_crash_daemon`). CI/staging runbook smoke:
`scripts/exercise_ops_runbooks.sh` (backup/restore, corruption repair, graceful drain + `STATS`)
via `.github/workflows/ops-runbooks.yml`. See [production readiness](../production-readiness.md).
