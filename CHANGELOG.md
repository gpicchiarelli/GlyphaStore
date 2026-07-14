# Changelog

All notable changes will be documented here. GlyphaStore follows Semantic Versioning once a stable
public API exists.

## [Unreleased]

- Define the target alpha durability, recovery, routing persistence, and public read ownership
  contracts in ADRs 0008 and 0009.
- Separate the installed C++ API from engine internals with a PImpl Store, owning reads, byte-key
  overloads, and a build-tree-only server/test access bridge.
- Rename Store read benchmark result identifiers with a `_copy` suffix so owning-read measurements
  cannot be compared accidentally with the former non-owning prototype baseline.
- Add the exact little-endian Segment header v1 codec, alternating CRC32C commit-slot selection,
  fail-closed corruption/version handling, and a canonical golden fixture.
- Add the canonical manifest v1 codec with full-file CRC32C, bounded decoding, deterministic Segment
  catalog invariants, publication-generation selection, and a golden fixture.
- Add a canonical binary-key Record v1 fixture, require minimal aligned extents and zero padding on
  decode, centralize hexadecimal fixture loading, and document the format compatibility matrix.
- Add platform-aware POSIX persistence primitives with descriptor-anchored private paths, complete
  positional I/O, exclusive directory locking, strict synchronization, atomic manifest replacement,
  explicit indeterminate outcomes, and filesystem fault-injection tests.
- Add exact-size durable Segment files with platform-specific physical allocation, verified identity
  reopening, Record-before-slot synchronization, alternating commit/seal slots, bounded committed
  recovery scans, and explicit not-committed versus indeterminate fault outcomes.
- Add manifest-driven durable recovery with constant Segment-descriptor usage, single-pass committed
  Record visitation, per-Worker bounded latest-key rebuild, persisted hash/routing validation,
  sequence restoration, and interrupted-rotation detection.
- Add bounded descriptor-relative namespace audit with strict canonical Segment parsing,
  deterministic anomaly reports, crash-temporary tolerance, no-follow object checks, and fail-closed
  rejection of unlisted, malformed, unknown, unsafe, or missing entries before recovery scans.
- Add OpenBSD as a `kqueue` architectural target from the `0.1.0` prototype line.
- Bootstrap the C++23 architecture prototype.
- Add fixed 64 MiB append-only segments, explicit record codec, derived Index, rebuild, vacuum,
  worker sizing, tests, fuzz targets, benchmarks, and macOS/Xcode tooling.
