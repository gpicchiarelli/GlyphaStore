# Soak profiles (durable daemon)

Status: descriptive  
Applies to: `scripts/soak_daemon.sh` and GitHub Actions soak jobs  
Owner: platform / ops maintainers  
Last reviewed: 2026-08-27

CI-friendly and multi-hour soak entry points for `glyphastored`. These runs are **software
path evidence** (PUT/GET, reconnect, overwrite churn, graceful drain, optional RSS/STATS
sampling, plus adversarial software stubs). They are **not** E3/E4 power-loss certification
and not a substitute for a controlled multi-hour hardware matrix on release hardware.

Assurance linkage: requirement `GS-OPS-SOAK-001`, gate `GATE-SOAK`, budget classes
`SOAK-SOFTWARE-SMOKE` / `SOAK-SOFTWARE-EXTENDED` in
[`engineering/performance/budgets.yaml`](../../engineering/performance/budgets.yaml).

## Profiles

| Profile | Duration | Typical use | RSS / STATS sampling |
| --- | --- | --- | --- |
| `smoke` | ~45s | Every PR/push via `ops-runbooks.yml` | off |
| `long` | 30m | Weekly schedule / local | every 60s |
| `1h` | 1h | Manual `soak-extended.yml` or monthly schedule | every 60s |
| `4h` | 4h | Manual `soak-extended.yml` only | every 120s |
| `hot-key` | ~45s | Wave 4 software stub: hammer one key | off |
| `connection-churn` | ~45s | Wave 4 software stub: reconnect every op | off |
| `queue-saturation` | ~45s | Wave 4 stub: tiny `--durable-mutation-queue-capacity` | off |
| `adversarial-reclaim` | ~45s | Wave 4 software stub: heavy overwrite churn | off |

Override duration with `SOAK_SECONDS` / `--seconds` without changing the profile label
(RSS fail-closed still applies when duration ≥ 3600s or profile is `1h`/`4h`).

Adversarial profiles (`hot-key`, `connection-churn`, `queue-saturation`,
`adversarial-reclaim`) are **software stubs**: they exercise shape, OVERLOADED under a
bounded queue, and SIGTERM drain. They do **not** close multi-hour reclaim-fairness
residuals (`HAZ-026` / `GATE-CONFIG`) or absolute hardware budgets.

## How to run

```bash
# Smoke (default)
./scripts/soak_daemon.sh
./scripts/soak_daemon.sh --profile smoke

# Weekly-style local long soak
./scripts/soak_daemon.sh --profile long
# or
SOAK_SECONDS=1800 ./scripts/soak_daemon.sh

# Multi-hour path (RSS + STATS samples; rotation counters may remain 0)
./scripts/soak_daemon.sh --profile 1h
./scripts/soak_daemon.sh --profile 4h

# Adversarial software stubs (default ~45s)
./scripts/soak_daemon.sh --profile hot-key
./scripts/soak_daemon.sh --profile connection-churn
./scripts/soak_daemon.sh --profile queue-saturation
./scripts/soak_daemon.sh --profile adversarial-reclaim
```

Requires built `glyphastored` and `glyphastore_interop_client` (or set `GLYPHASTORED` /
`GLYPHASTORE_INTEROP_CLIENT`), plus `lsof` and the Python SDK on `PYTHONPATH` for `STATS`.

## What is checked

- Continuous PUT/GET against a durable-periodic daemon (`--open-mode create-new`)
- Reconnect every 25 ops on smoke/long/1h/4h (fresh TCP client process)
- Overwrite churn every 7 ops (reclaimable sealed history for background vacuum)
- Adversarial stubs as above (`queue-saturation` requires at least one OVERLOADED)
- Final `STATS` needles including `useful_compactions=` and `durable_rotations_committed=`
- Graceful `SIGTERM` drain (`--shutdown-drain-ms`); soak teardown never uses `SIGKILL`
- Multi-hour only: periodic RSS + STATS samples; fail if RSS grows by both
  `SOAK_RSS_FAIL_FACTOR` (default 3×) **and** more than `SOAK_RSS_FAIL_DELTA_KB`
  (default 256 MiB)

### SIGTERM durability-boundary note

End-of-soak always sends `SIGTERM` so the daemon takes the graceful drain path. That is
the intended durability boundary for operator restarts. A `SIGTERM` that races an
in-flight mutation still leaves the client ACK indeterminate after commit — the same
rule as production drain ([graceful-drain-and-overload](graceful-drain-and-overload.md)).
Software soaks do **not** certify power-loss or `SIGKILL` recovery.

Rotation / useful compaction counters are **reported**, not required to be non-zero: short
and even multi-hour software soaks may not fill enough segments to rotate.

## CI wiring

| Workflow | When | Profile / duration |
| --- | --- | --- |
| `.github/workflows/ops-runbooks.yml` | push/PR | smoke ~45s |
| `.github/workflows/ops-runbooks.yml` | weekly Monday | long 30m |
| `.github/workflows/ops-runbooks.yml` | `workflow_dispatch` | caller `soak_seconds` |
| `.github/workflows/soak-extended.yml` | `workflow_dispatch` | `smoke`/`long`/`1h`/`4h` |
| `.github/workflows/soak-extended.yml` | monthly schedule | `1h` |

Multi-hour jobs are **not** on every PR. Adversarial stub profiles are local / optional
until a dedicated CI matrix owns them.

## Related

- [Production readiness](../production-readiness.md) — smoke vs multi-hour honesty
- [Operations index](README.md)
- [Durable TCP daemon](durable-tcp-daemon.md)
- [Graceful drain and overload](graceful-drain-and-overload.md)
