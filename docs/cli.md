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

`SIGINT` and `SIGTERM` request an orderly process stop. The daemon joins its executors before returning;
the current prototype does not yet promise connection draining. `--quiet` suppresses normal startup and
shutdown messages, but never suppresses errors.

Important server controls include bounded connection counts, handoff queues, event batches, and per-connection
input/output buffers. Unsupported Worker counts and buffers smaller than protocol headers are rejected before
the server binds a socket. `--reuse-port` and `--no-reuse-port` are mutually exclusive; the operating system
decides whether per-executor listeners are available. Executor affinity is strict on supported Linux systems
and advisory on macOS.

## Maintenance tools

```bash
glyphastore_inspect_segment -- segment.gseg
glyphastore_rebuild_index -- segment-0001.gseg segment-0002.gseg
```

The inspector and rebuild commands currently expose their stable CLI envelope, but file-backed segment
recovery remains a prototype. They report that limitation explicitly instead of implying a completed repair.
