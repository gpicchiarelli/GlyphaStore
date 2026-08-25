Status: descriptive operator reference for implemented surfaces
Applies to: `glyphastored` wire v2 probes, STATS needles, JSON lifecycle/audit logs, `--dump-config`
Owner: platform / ops maintainers
Last reviewed: 2026-08-01

# Observability reference

Inventory of **implemented** GlyphaStore operator signals. Normative probe/opcode contracts live in
[wire protocol v2](../spec/wire-protocol-v2.md). Flag defaults and dump-config keys live in
[cli.md](../cli.md). This document names units, cardinality, and how to use the surfaces without
claiming Prometheus/OpenTelemetry exporters (none exist yet).

Claim ceiling remains *architectural prototype*. Histogram approximations are **not** SLOs
(`GATE-TELEMETRY` residual).

## 1. Probe model (load balancer)

| Opcode | Success value | Use |
| --- | --- | --- |
| `HEALTH` (7) | `GlyphaStore/live` | Process/executors alive |
| `READY` (8) | `GlyphaStore/ready` | Safe for **new traffic** |
| `STATS` (9) | Bounded ASCII `GlyphaStore/stats` + `key=value` lines | Admin snapshot |

Rules:

- Orchestrators must gate traffic on **READY**, not HEALTH alone.
- During drain, READY fails while HEALTH may still succeed briefly.
- Failed probes return `INTERNAL_ERROR` with empty value.
- Under secure authz, prefix principals need `admin` for STATS ([ADR 0027](../adr/0027-stats-isolation-prefix-principals.md)).
- STATS may return `OVERLOADED` if the report would exceed the size budget.

Operator triage: [graceful-drain-and-overload](graceful-drain-and-overload.md),
[durable-tcp-daemon](durable-tcp-daemon.md) §3.

## 2. Units and cardinality

| Suffix / shape | Unit | Notes |
| --- | --- | --- |
| (no suffix) | dimensionless counter | Monotonic unless documented otherwise |
| `*_ns` | nanoseconds | Wall/monotonic as implemented per needle |
| `*_bytes` | bytes | |
| `*_bp` | basis points | Ratio × 10000 |
| boolean fields | `0` / `1` | |
| `lane[N].*` | per shard-pair Writer lane (`pair_writer_stats`) | `N` = shard-pair / executor index |
| `batch[W].*` | per durable batch shard pair | Thin subset of batch stats; wire still labels owner ids as Workers |

Cardinality is bounded by configured shard pairs (STATS `executors`) / connections — do not scrape
unbound label sets. Manifest and wire still expose `worker_count` as the 0.1.x name for that same
pair count. There is no remote metrics port; scrape via authenticated STATS or logs.

## 3. STATS field catalog

Authority for emission order: `src/server/server_stats.cpp`. Fields may be added in minor releases;
treat unknown keys as forward-compatible.

### 3.1 Process / security

`version`, `live`, `ready`, `executors`, `connections_active`, `connections_adopted`,
`output_scatter_responses`, `output_scatter_bytes`, `output_scatter_partial_writes`,
`output_scatter_completions`, `input_buffer_compactions`, `input_buffer_bytes_moved`, `tls_enabled`,
`tls_mtls`, `tls_crl`, `tls_ocsp_fail_closed`,
`authz_enabled`, `authz_principals`, `auth_accepts`, `auth_denies`, `authz_denies`, `tls_errors`

Abuse (Phase 5): `abuse_accepts_rejected`, `abuse_idle_closed`, `abuse_request_timeout_closed`,
`abuse_connection_rate_rejected`, `abuse_principal_request_rejected`,
`abuse_principal_bandwidth_rejected`

### 3.2 Maintenance

`maintenance_state`, `maintenance_pressure`, `mutations_rejected`, `compact_attempts`,
`compact_completed`, `useful_compactions`, `maintenance_skips`, `maintenance_consecutive_no_gain`,
`maintenance_last_skip_reason`, `maintenance_last_activation_reason`,
`maintenance_last_no_gain_source_records_verified`, `maintenance_last_no_gain_source_bytes_verified`,
`maintenance_last_no_gain_expired_records_dropped`, `maintenance_total_no_gain_*` (same three),
`maintenance_sequence_conflicts`, `maintenance_candidate_worker`,
`maintenance_candidate_sealed_record_bytes`, `maintenance_candidate_live_record_bytes`,
`maintenance_candidate_dead_record_bytes`, `maintenance_candidate_dead_byte_ratio_bp`,
`maintenance_candidate_scheduling_dead_byte_ratio_bp`, `maintenance_unread_ttl_probe_performed`,
`maintenance_candidate_unread_expired_sealed_record_count`,
`maintenance_candidate_unread_expired_sealed_record_bytes`,
`maintenance_rate_window_bytes_copied`, `maintenance_rate_window_cpu_ns`,
`maintenance_foreground_latency_samples`, `maintenance_last_foreground_p99_ns`,
`maintenance_latency_suspends`, `maintenance_latency_guard_active`,
`maintenance_latency_deferral_age_ns`, `maintenance_latency_debt_overrides`

Enum string values for state/pressure/skip/activation are defined beside the renderer in
`server_stats.cpp`.

### 3.3 Durable rotation

`durable_rotation_attempts`, `durable_rotations_committed`, `durable_rotation_compaction_waits`,
`durable_rotation_final_record_commit_attempts`, `durable_rotation_final_record_commits`,
plus `durable_rotation_last_*` / `total_*` / `maximum_*` timing needles (`publication_wait_ns`,
`seal_ns`, `create_ns`, `manifest_publication_ns`, `execution_ns`, `total_ns`,
`final_record_commit_ns`).

Soak samples often watch `useful_compactions` and `durable_rotations_committed` ([soak](soak.md));
short soaks may leave them at zero.

### 3.4 Paired Writer lanes (`lane[N].*`)

Epochs / queues: `reader_safe_epoch`, `writer_epoch`, `queue_depth`, `queued_bytes`,
`maximum_queue_depth`, `maximum_queued_bytes`, payload slot/arena admission fields.

Flow: `admitted`, `rejected`, `expired_before_store`, `completed`, `conflict_retries`,
`conflict_retry_commits`, plus read-refresh / generation / delta / merge counters.

**Latency histograms** (approx; not SLOs):

| Prefix | Meaning |
| --- | --- |
| `lane[N].queue_wait_ns.*` | Time in mutation lane before Store entry |
| `lane[N].service_ns.*` | Store service time |

Per histogram: `.count`, `.sum` (ns), cumulative buckets `.le_1000` … `.le_1000000000`, `.le_inf`,
approximate `.p50`, `.p99` (ns). Bucket edges: 1µs … 1s / +Inf
(`include/glyphastore/core/latency_histogram.hpp`).

### 3.5 Durable batch lanes (`batch[W].*`)

Per shard-pair durable batch surface (index `W` matches owner / shard-pair id; historical “Worker”
label on the wire). Fields: `enabled`, `pending_records`, `committed_batches`, `failed_batches`.

## 4. Structured JSON lifecycle logs

Enable with `--log-format json` (or config). Stderr JSON-lines; common fields: `ts` (epoch seconds),
`event`, `program`. String fields capped (~256 bytes); no TLS private material.

| `event` | Notable fields | Suppressed by `--quiet`? |
| --- | --- | --- |
| `start` | — | yes |
| `listen` | `bind`, ports / `unix_socket`, `executors`, `storage` | yes |
| `ready` | `ready`; if 0: `reason` | **no** |
| `maintenance_emergency` | `maintenance_pressure` | no |
| `maintenance_fault` | `maintenance_state`, `error_code`, `error_message` | no |
| `shutdown_begin` | optional `signal`, `executor_failure` | no |
| `shutdown_drain_begin` | `drain_ms` | no |
| `shutdown_drain_end` | `timed_out`; optional `join_failed` | no |
| `stopped` | optional `signal` | yes |
| `executor_failure` | `error_code`, `error_message` | no |

`ready.reason` ∈ `not_live`, `shutting_down`, `store_not_operational`, `pair_fail_closed`,
`admission_fenced`, `maintenance_emergency`, `maintenance_fault`.

### Security audit (related)

With `--secure-profile` or JSON logging, `auth` / `authz` / `tls` audit events may also appear on
stderr (no payloads). See [secure-profile](../security/secure-profile.md).

## 5. `--dump-config` (startup contract dump)

`glyphastored … --dump-config` prints `GlyphaStore/config` and exits (no listen). Use it to capture
effective observability-related settings: `log-format`, `quiet`, abuse limits, maintenance
rate/p99 guards, durable mutation queue caps, `shutdown-drain-ms`, TLS/authz flags (paths only).

## 6. Suggested scrape / alert mapping

| Intent | Signal |
| --- | --- |
| Liveness | `HEALTH` or log `listen`/`stopped` |
| Traffic readiness | `READY` + log `ready` |
| Overload | wire `OVERLOADED`; STATS `lane[N].rejected`, `abuse_*`, queue depths |
| Maintenance gate | `mutations_rejected`, `maintenance_pressure`, log `maintenance_emergency` |
| Tail latency (diagnostic) | `lane[N].queue_wait_ns.p99`, `lane[N].service_ns.p99`, `maintenance_last_foreground_p99_ns` |
| Durability progress | `durable_rotations_committed`, `useful_compactions` (may be 0 on short soaks) |

Do not publish absolute ops/s or p99 product claims from hosted CI; see
[performance-budgets](../assurance/performance-budgets.md).

## 7. Related

- [Operations index](README.md)
- [Wire protocol v2](../spec/wire-protocol-v2.md)
- [CLI](../cli.md)
- [Server model](../architecture/server-model.md)
- Impl: `src/server/server_stats.cpp`, `src/server/daemon_log.cpp`
