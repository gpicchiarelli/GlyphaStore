# Command-line interface

GlyphaStore command-line programs share one strict parser and a common operational contract.

## Conventions

- `-h` and `--help` print help to standard output and exit successfully.
- `-V` and `--version` print the executable name and GlyphaStore version.
- Long values accept both `--option value` and `--option=value`; selected common options have short forms.
- An option may appear once unless its help explicitly says otherwise. Duplicate options are errors.
- `--` ends option parsing, allowing paths that begin with `-`.
- Invalid syntax is written to standard error with exit code 2 and a help hint.
- Runtime or operating-system failures use exit code 1; successful commands use exit code 0.
- Byte-size options accept bytes or `KB`, `KiB`, `MB`, `MiB`, `GB`, and `GiB` suffixes. Decimal and binary
  suffixes are intentionally distinct.

## Server

Operator guide for durable deployments: [durable TCP daemon](operations/durable-tcp-daemon.md)
(profile/mode selection, resource flags, `HEALTH`/`READY`/`STATS`, drain, offline backup pointers).

```bash
glyphastored --bind 127.0.0.1 --port 7379 --workers 4
glyphastored --config /etc/glyphastore/daemon.conf
glyphastored --profile embedded --data-dir /var/lib/glyphastore --dump-config
glyphastored --port 0 --workers 2 --max-input-bytes 4MiB --max-output-bytes 4MiB
glyphastored --help
```

### Configuration precedence

Settings resolve as **defaults < deployment profile < config file < environment < CLI**. Unknown
keys, unknown profile names, empty values, duplicate keys in one file, and conflicting
`--reuse-port` / `--no-reuse-port` (or their env/file equivalents) fail closed before the process
listens.

- `--profile NAME` or `GLYPHASTORE_PROFILE=NAME` or `profile = NAME` in a config file selects one of
  the built-in deployment presets (`dev`, `embedded`, `production`). Profile selection follows the
  same file/env/CLI precedence as other settings; the profile's preset values sit between hardcoded
  defaults and file/env/CLI overrides.
- `dev` keeps volatile storage and disables background maintenance for local iteration.
- `embedded` selects `durable-periodic` with constrained resource caps suited to sidecar or
  single-node embedded deployments.
- `production` selects `durable-periodic` with background maintenance and the standard durable
  resource defaults; operators still must supply `--data-dir` (or equivalent) before listen.

- `--config PATH` or `GLYPHASTORE_CONFIG=PATH` selects a settings file. The file cannot set `config`,
  `help`, or `version`.
- `--dump-config` prints the fully resolved effective settings (`GlyphaStore/config` ASCII key=value,
  including the selected deployment profile) after the same validation used before listen, then exits without binding or opening a Store. TLS
  settings appear as paths only. The flag is CLI-only (not settable from file or environment).
- File keys are the long option names without `--` (`port = 7379`, `storage-mode = durable-sync`,
  `quiet = true`). Lines may be blank or start with `#`. Values may be quoted with `"..."`.
- Environment variables use `GLYPHASTORE_` plus the long name in `SCREAMING_SNAKE_CASE`
  (`GLYPHASTORE_PORT`, `GLYPHASTORE_DATA_DIR`, `GLYPHASTORE_MAX_STORE_BYTES`). Boolean flags accept
  `true`/`false`, `yes`/`no`, `on`/`off`, or `1`/`0`.

`SIGINT` and `SIGTERM` request an orderly process stop. The daemon stops accepting new connections,
drains existing connections and admitted durable mutations (bounded by `--shutdown-drain-ms`, default
30s; `0` waits unbounded), then closes the Store. Idle connections are closed once in-flight responses
flush; new requests on draining connections are refused. Queued mutations that have not entered Store
execution when the drain deadline expires complete as `unavailable`; in-flight Store work is never
cancelled. A timed-out drain makes process exit fail closed (`join` returns an error).

Wire-protocol `HEALTH` (opcode 7) and `READY` (opcode 8) probes are accepted before `INIT`/`BIND_WORKER`.
`HEALTH` returns `OK` with value `GlyphaStore/live` while executors are live; `READY` returns `OK`
with value `GlyphaStore/ready` only when the Store is operational and maintenance is not in emergency
or a sticky faulted state. `STATS` (opcode 9) returns a bounded ASCII admin report (version, live/ready,
connections, durable lane/batch counters, maintenance snapshot including skip reason and no-gain
planning scan counters, and durable rotation wait/execution phase timings, including seal, Segment
creation, Manifest publication, and final Record commit)
while live. Failed probes return `INTERNAL_ERROR`. `--quiet`
suppresses normal startup and shutdown messages, but never suppresses errors.

Structured lifecycle logging is opt-in via `--log-format json` (or `log-format = json` in a config
file, or `GLYPHASTORE_LOG_FORMAT=json`). Default `human` keeps the legacy one-line startup/shutdown
messages on stdout and errors on stderr. JSON mode emits one object per line on stderr with stable
`event` names: `start`, `listen`, `ready`, `maintenance_emergency`, `maintenance_fault`,
`shutdown_begin`, `shutdown_drain_begin`, `shutdown_drain_end`, `stopped`, and `executor_failure`.
Fields are bounded (256-byte string cap), omit TLS paths and other secrets, and include only bind
address, ports, executor count, storage mode, readiness reason, maintenance pressure/state, and
sanitized error codes/messages. `--quiet` suppresses the normal `start`/`listen`/`stopped` JSON
events but never suppresses readiness loss, maintenance emergency/fault, shutdown drain, or error
events.

Important server controls include bounded connection counts, handoff queues, event batches, and per-connection
input/output buffers. Durable deployments also expose batch and resource caps:
`--sync-interval-ms`, `--group-max-records` / `--group-max-bytes` / `--group-max-wait-ms`,
`--max-store-bytes`, `--reserved-free-bytes`, `--max-segments`, `--max-hot-cache-bytes`, and
`--max-temporary-compaction-bytes`. Those flags require a durable `--storage-mode` and are validated
before the process listens. Unsupported Worker counts and buffers smaller than protocol headers are rejected before
the server binds a socket. `--reuse-port` and `--no-reuse-port` are mutually exclusive; the operating system
decides whether per-executor listeners are available. Executor affinity is strict on supported Linux systems
and advisory on macOS.

`--maintenance-max-copy-bytes-per-cycle` bounds the exact Index-referenced live Record bytes of one
normal background compaction. The daemon default is `128MiB`; a candidate exactly at the limit is
allowed, `0` explicitly removes the limit, and pressure/emergency bypass it to recover capacity.
The same setting is available as `maintenance-max-copy-bytes-per-cycle` in the config file and
`GLYPHASTORE_MAINTENANCE_MAX_COPY_BYTES_PER_CYCLE` in the environment.

### TLS (optional outer transport)

When built with LibreSSL/OpenSSL (`GLYPHASTORE_ENABLE_TLS`), the daemon may wrap protocol v2 in
TLS 1.3 ([ADR 0020](adr/0020-tls-outer-transport.md),
[secure-profile reference](security/secure-profile.md)):

```bash
# TLS-only on --port
glyphastored --bind 127.0.0.1 --port 7379 \
  --tls-cert /etc/glyphastore/server.crt --tls-key /etc/glyphastore/server.key

# Dual: cleartext on --port, TLS on --tls-port
glyphastored --bind 127.0.0.1 --port 7379 --tls-port 7380 \
  --tls-cert /etc/glyphastore/server.crt --tls-key /etc/glyphastore/server.key

# mTLS + authz (principal URI SAN → DNS SAN → CN; --authz-map default-deny)
glyphastored ... --tls-cert ... --tls-key ... --tls-client-ca /etc/glyphastore/clients-ca.crt \
  --authz-map /etc/glyphastore/authz.map

# Fail-closed secure profile (TLS-only on --port; refuses --tls-port dual cleartext;
# applies Phase 5 abuse-limit defaults)
glyphastored --secure-profile --bind 127.0.0.1 --port 7379 \
  --tls-cert ... --tls-key ... --tls-client-ca ... --authz-map ...

# Optional UDS alongside TCP (ADR 0029); peercred principal unix:uid=N for --authz-map
glyphastored --unix-socket /var/run/glyphastore.sock --unix-peercred --authz-map ...
```

`--secure-profile` also selects keyed SipHash Worker routing. The daemon and C++ client implement
the extended wire-v2 `INIT` identity; Python, Perl, Go, Erlang and Ruby currently fail closed on it.
Those SDKs can use explicitly configured TLS/mTLS/authz without `--secure-profile` while default FNV
routing remains active. See the [SDK roadmap](architecture/sdk-roadmap.md) before choosing a client
for the complete profile.

`--tls-cert` and `--tls-key` are both required when any TLS path is set. Without `--tls-port`, TLS
makes `--port` TLS-only. With `--tls-port`, cleartext and TLS use distinct ports (collision fails
closed). `--secure-profile` requires TLS + mTLS + `--authz-map`, refuses `--tls-port`, applies
Phase 5 abuse defaults (explicit `0` refused), randomizes the Index mix seed unless `--index-hash-seed` is set ([ADR 0026](adr/0026-keyed-index-hash-seed.md)),
and randomizes Worker routing unless `--worker-hash-seed` is set ([ADR 0030](adr/0030-keyed-worker-routing.md)). With `--unix-socket`,
secure-profile also requires `--unix-peercred`. Cleartext remains the
default when TLS flags are omitted. Capability `admin` implies `write` ⇒ `read`; prefix-scoped
principals need `admin` for `STATS` ([ADR 0027](adr/0027-stats-isolation-prefix-principals.md)).
UDS is not a TLS replacement ([ADR 0029](adr/0029-uds-peercred.md)).

### Phase 5 abuse controls

Distinct from maintenance rate budgets. Zero disables each limit (trusted cleartext default):

```bash
glyphastored ... \
  --max-accepts-per-sec 128 \
  --idle-timeout-ms 60000 \
  --request-timeout-ms 30000 \
  --connection-max-requests-per-sec 256 \
  --principal-max-requests-per-sec 1024 \
  --principal-max-bytes-per-sec 32MiB
```

Accept floods drop the peer; request/bandwidth quota exceed returns wire `overloaded`.
`HEALTH`/`READY`/`STATS` stay exempt from request/bandwidth quotas. `STATS` exports `abuse_*`
counters. Store mutations already in execution are never cancelled by `--request-timeout-ms`.

Maintenance rate budgets (distinct from connection rate limits / Phase 5 and from E3 power-loss):

```bash
glyphastored ... --maintenance-max-copy-bytes-per-sec 1048576 \
  --maintenance-max-cpu-ms-per-window 25 \
  --maintenance-suspend-on-p99-latency-ms 40 \
  --maintenance-suspend-on-p99-min-samples 32 \
  --maintenance-max-latency-deferral-ms 30000
```

Zero disables each byte/CPU/p99 threshold budget. The latency guard resumes below 80% of its
threshold; its deferral bound admits one reclaim attempt, while pressure/emergency bypass all normal
fairness controls.
## Maintenance tools

```bash
glyphastore_inspect_segment [--json] [--no-scan] -- segment-<16hex>-<8hex>.glypha
glyphastore_verify_store [--json] [--no-scan] -- /path/to/data-dir
glyphastore_backup_store [--json] [--no-scan] -- /path/to/source /path/to/destination
glyphastore_migrate_store [--json] [--no-scan] --workers N -- /path/to/source /path/to/destination
glyphastore_repair_store [--json] [--no-scan] -- /path/to/source /path/to/empty-workspace
glyphastore_rebuild_index -- segment-<16hex>-<8hex>.glypha
```

### `glyphastore_inspect_segment`

Read-only validation of one durable Segment file. It does not take the Store directory lock and does
not modify the file.

Default work:

1. open as a private, singly linked, owner-only regular file of exactly 64 MiB;
2. decode the Segment header and select the newest valid commit slot;
3. when the basename is a canonical `segment-…glypha` name, require it to match header identity;
4. scan the committed Record extent (CRC32C, sequences, commit metadata agreement).

`--no-scan` stops after header/commit validation. `--json` emits a stable single-line JSON object on
stdout. Exit codes: `0` validated, `1` validation/I/O failure, `2` usage.

A concurrent writer can make a point-in-time read observe a torn commit slot; that is reported as
validation failure (fail-closed), not success.

### `glyphastore_verify_store`

```bash
glyphastore_verify_store [--json] [--no-scan] -- /path/to/data-dir
```

Read-only structural validation of a durable Store data directory. Takes the exclusive Store lock
(fail-closed if a daemon or Store already holds it). Stop the daemon before verifying.

Default work:

1. exclusive-lock the data directory;
2. decode `manifest.glypha` and enforce Manifest resource bounds;
3. audit the namespace (missing/unlisted/unsafe entries fail; crash temporaries may be reported);
4. open each catalog Segment read-only against Manifest identity;
5. check Manifest role ↔ commit lifecycle (sealed-active is OK and counted as
   `active_requires_rotation`);
6. scan each committed Record extent unless `--no-scan`.

Does not rebuild Indexes, check key routing, or repair files. Exit codes match inspect.

### `glyphastore_backup_store`

```bash
glyphastore_backup_store [--json] [--no-scan] -- /path/to/source /path/to/destination
```

Offline verified copy of a durable data directory. Stop writers first. Takes exclusive locks,
verifies the source, creates an empty destination, copies catalog Segments then `manifest.glypha`,
syncs, and verifies the destination. Restore uses the same command with backup as source and a new
empty destination. See [backup-restore](architecture/backup-restore.md). Live/hot backup is not
supported.

### `glyphastore_migrate_store`

```bash
glyphastore_migrate_store [--json] [--no-scan] --workers N -- /path/to/source /path/to/destination
```

Offline Worker reshard / logical rewrite. Stop writers first. Verifies the source, copies every live
key/value/expiry into a new destination Store created with `--workers N`, checkpoints progress in
`<destination>.migrate-state`, verifies the destination, then removes the checkpoint. Re-run the
same command to resume after interrupt. Source is never mutated. Same Worker count is allowed as an
offline rewrite into a new Store identity. Online resharding is not supported. See
[store-migration](architecture/store-migration.md) and
[worker-resharding](operations/worker-resharding.md).

### `glyphastore_repair_store`

```bash
glyphastore_repair_store [--json] [--no-scan] -- /path/to/source /path/to/empty-workspace
```

Offline fail-closed repair that never mutates the source. Requires an empty explicit workspace and
creates:

- `<workspace>/store` — clean Manifest + catalog Segments (verified after copy)
- `<workspace>/quarantine` — non-catalog namespace anomalies plus `audit.txt`

Unlisted Segments, crash temporaries, compaction intents, and unknown regular files are quarantined.
Missing catalog entries and unsafe entries (symlinks, hard links, non-regular objects) fail closed
without writing a usable store. Live/hot repair is not supported.

### `glyphastore_rebuild_index`

Durable v1 does not persist a separate Index artifact. Indexes rebuild from committed Segments
during Store recovery. This offline rewrite tool permanently refuses Segment-only input with an
explicit operator path:

1. Reopen or restart the Store on the data directory (ordinary recovery).
2. For offline catalog repair, use `glyphastore_repair_store` with an explicit empty workspace.

Help and version remain available. Do not use this tool for durable v1 operations.
