# Fuzz seed and regression corpora

Committed seeds under `fuzz/corpus/<target>/` bootstrap continuous libFuzzer runs.
CI builds and executes each target via `.github/workflows/sanitizers.yml` (`fuzz-run`)
using `scripts/run-fuzzers.sh`.

| Target | Binary | Corpus |
| --- | --- | --- |
| `record_decoder` | `fuzz_record_decoder` | `fuzz/corpus/record_decoder` |
| `segment_scanner` | `fuzz_segment_scanner` | `fuzz/corpus/segment_scanner` |
| `index_rebuild` | `fuzz_index_rebuild` | `fuzz/corpus/index_rebuild` |
| `protocol_decoder` | `fuzz_protocol_decoder` | `fuzz/corpus/protocol_decoder` |

Local smoke (Linux Clang with libFuzzer runtime):

```bash
cmake --preset unix-fuzz
cmake --build --preset unix-fuzz
GLYPHASTORE_FUZZ_SECONDS=30 ./scripts/run-fuzzers.sh
```

On macOS, use a full LLVM toolchain and the `macos-fuzz` preset, then point
`GLYPHASTORE_FUZZ_BUILD_DIR` at `build/macos-fuzz`.

Promote interesting minimized inputs into these directories when they catch a
regression. Do not commit libFuzzer crash-/timeout-/oom- artifacts; those stay
gitignored.
