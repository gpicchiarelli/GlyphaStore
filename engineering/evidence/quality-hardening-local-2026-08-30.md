# Local quality-hardening evidence — 2026-08-30

Evidence class: **local**. This record does not promote a gate to `PROVATA_IN_CI`, certify a
platform durability row, accept ADR 0036, or raise the architectural-prototype claim ceiling.

## Environment

- Base revision: `24ddfec8e7c6dc774c2658c291dd90d82956e88a` plus the uncommitted changes described here.
- Host: macOS 26.6.2, Apple clang 21.0.0, arm64.
- Formatting: clang-format 21.1.8.
- Static analysis: clang-tidy 21.1.6.

## Scope

- Normalize the tracked C++ tree to the pinned formatting policy.
- Validate UTF-8 and exact-case repository-local links across every tracked Markdown document.
- Add a compile-database-derived, production-only fail-closed clang-tidy pass.
- Harden optional-state, move-ownership, declaration consistency, dead-store, and fail-closed
  paths reported by that pass.
- Preserve ADR 0036's proposed/default-off status while adding the V7 slot-pressure evidence and
  terminal shutdown cleanup already described in the ADR-specific local record.

## Results

| Check | Local result |
| --- | --- |
| Assurance validation | 33 requirements, 33 hazards, 28 gates, 3 waivers; 0 warnings |
| Documentation validation | 254 Markdown files; 1,033 repository-local links |
| Python/tooling tests | 104 passed |
| Pinned clang-format dry run | all tracked C++ sources passed |
| High-signal clang-tidy gate | 109 production sources; 4 fail-closed checks; passed |
| Broad clang-tidy build | completed; lower-confidence test/benchmark/toolchain diagnostics remain visible |
| `unix-strict` build | 235 build steps; passed with compiler warnings as errors |
| `unix-strict` tests | 54/54 passed; 351.02 s |
| macOS ASan+UBSan tests | 54/54 passed; 464.72 s |
| macOS TSan main suite after repair | 696/696 passed; no TSan report |

The first full TSan main-suite run exposed three reports with one root cause: the dedicated
volatile Writer read a caller-stack `SyncMutation::status` after publishing `done`. The Writer now
derives the all-failed/indeterminate decision before the release notification and never
dereferences those nodes afterward. Both direct reproducer tests and the complete 696-test TSan
main suite passed after the repair.

## Residuals

- The complete post-repair TSan CTest matrix was not rerun: its crash-sync row permits up to 2,400
  seconds. The complete main concurrency suite was rerun; strict and ASan+UBSan ran all 54 CTest
  rows, including crash/recovery.
- CI, Linux, FreeBSD, and OpenBSD results remain external to this local record.
- The broad all-target clang-tidy profile is diagnostic, not warning-free. Only the documented
  high-signal production pass is fail-closed.
- External URL availability and Markdown fragment semantics remain covered by the separate
  scheduled link workflow, not the deterministic local-link validator.
