# Manual pages

GlyphaStore ships portable [mdoc(7)](https://man.openbsd.org/mdoc.7) sources for
every binary installed by the Runtime component.

| Page | Section | Binary |
|---|---|---|
| `glyphastore.7` | 7 | overview |
| `glyphastored.8` | 8 | `glyphastored` |
| `glyphastore_demo.1` | 1 | `glyphastore_demo` |
| `glyphastore_inspect_segment.1` | 1 | `glyphastore_inspect_segment` |
| `glyphastore_verify_store.1` | 1 | `glyphastore_verify_store` |
| `glyphastore_backup_store.1` | 1 | `glyphastore_backup_store` |
| `glyphastore_migrate_store.1` | 1 | `glyphastore_migrate_store` |
| `glyphastore_repair_store.1` | 1 | `glyphastore_repair_store` |
| `glyphastore_rebuild_index.1` | 1 | `glyphastore_rebuild_index` |

## Build and install

```bash
cmake -S . -B build -DGLYPHASTORE_MAN_DATE="August 1, 2026"
cmake --build build --target glyphastore_manpages
cmake --install build --component Runtime
```

Pages land under `${CMAKE_INSTALL_MANDIR}/man{1,7,8}` (from `GNUInstallDirs`),
typically `share/man` on Linux/BSD and Homebrew prefixes on macOS.

Optional gzip at install time:

```bash
cmake -S . -B build -DGLYPHASTORE_COMPRESS_MANPAGES=ON
```

Default is uncompressed so installs do not require `gzip`; packaging may compress
afterward.

## Validation

```bash
./scripts/validate-manpages.sh
# or against a build tree:
./scripts/validate-manpages.sh build/man
```

Requires `mandoc` (OpenBSD/mandoc, macOS, or `mandoc` packages on Linux).
On systems with only groff, the script falls back to `groff -mandoc -Tutf8`.
