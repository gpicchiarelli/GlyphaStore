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

```bash
glyphastored --bind 127.0.0.1 --port 7379 --workers 4
glyphastored --port 0 --workers 2 --max-input-bytes 4MiB --max-output-bytes 4MiB
glyphastored --help
```

`SIGINT` and `SIGTERM` request an orderly process stop. The daemon joins its reactor executors, drains
admitted durable mutations (bounded by `--shutdown-drain-ms`, default 30s; `0` waits unbounded), then
closes the Store. Queued mutations that have not entered Store execution when the drain deadline
expires complete as `unavailable`; in-flight Store work is never cancelled. A timed-out drain makes
process exit fail closed (`join` returns an error). Connection draining is not yet promised. `--quiet`
suppresses normal startup and shutdown messages, but never suppresses errors.

Important server controls include bounded connection counts, handoff queues, event batches, and per-connection
input/output buffers. Unsupported Worker counts and buffers smaller than protocol headers are rejected before
the server binds a socket. `--reuse-port` and `--no-reuse-port` are mutually exclusive; the operating system
decides whether per-executor listeners are available. Executor affinity is strict on supported Linux systems
and advisory on macOS.

## Maintenance tools

```bash
glyphastore_inspect_segment [--json] [--no-scan] -- segment-<16hex>-<8hex>.glypha
glyphastore_verify_store [--json] [--no-scan] -- /path/to/data-dir
glyphastore_backup_store [--json] [--no-scan] -- /path/to/source /path/to/destination
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

### `glyphastore_rebuild_index`

The offline rewrite tool is not implemented for durable v1. Durable Indexes are rebuilt by ordinary
Store recovery. Invoking the tool with Segment paths exits `1` with an explicit error. Help and
version remain available.
