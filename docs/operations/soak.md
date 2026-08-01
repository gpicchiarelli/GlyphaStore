# Soak profiles (durable daemon)

Status: descriptive  
Applies to: `scripts/soak_daemon.sh` and GitHub Actions soak jobs  
Owner: platform / ops maintainers  
Last reviewed: 2026-08-01

CI-friendly and multi-hour soak entry points for `glyphastored`. These runs are **software
path evidence** (PUT/GET, reconnect, overwrite churn, graceful drain, optional RSS/STATS
sampling). They are **not** E3/E4 power-loss certification and not a substitute for a
controlled multi-hour hardware matrix on release hardware.

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

Override duration with `SOAK_SECONDS` / `--seconds` without changing the profile label
(RSS fail-closed still applies when duration ≥ 3600s or profile is `1h`/`4h`).

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
```

Requires built `glyphastored` and `glyphastore_interop_client` (or set `GLYPHASTORED` /
`GLYPHASTORE_INTEROP_CLIENT`), plus `lsof` and the Python SDK on `PYTHONPATH` for `STATS`.

## What is checked

- Continuous PUT/GET against a durable-periodic daemon (`--open-mode create-new`)
- Reconnect every 25 ops (fresh TCP client process)
- Overwrite churn every 7 ops (reclaimable sealed history for background vacuum)
- Final `STATS` needles including `useful_compactions=` and `durable_rotations_committed=`
- Graceful `SIGTERM` drain
- Multi-hour only: periodic RSS + STATS samples; fail if RSS grows by both
  `SOAK_RSS_FAIL_FACTOR` (default 3×) **and** more than `SOAK_RSS_FAIL_DELTA_KB`
  (default 256 MiB)

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

Multi-hour jobs are **not** on every PR.

## Related

- [Production readiness](../production-readiness.md) — smoke vs multi-hour honesty
- [Operations index](README.md)
- [Durable TCP daemon](durable-tcp-daemon.md)
