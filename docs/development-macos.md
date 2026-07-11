# macOS and Xcode development

macOS on Apple Silicon is GlyphaStore's primary development environment. The repository keeps
build generation reproducible through CMake while Xcode supplies editing, debugging, sanitizers,
Instruments, and source navigation.

## One-time setup

Install full Xcode, open it once to accept its license, then run:

```bash
./scripts/bootstrap-macos.sh
```

The script verifies the selected Xcode developer directory and creates `.tools/venv` containing
CMake and Ninja. This avoids requiring `sudo`, Homebrew, or MacPorts. The generated project is
`build/xcode/GlyphaStore.xcodeproj`.

If `xcode-select -p` points at Command Line Tools instead of full Xcode:

```bash
sudo xcode-select -s /Applications/Xcode.app/Contents/Developer
```

## Daily Xcode workflow

```bash
./scripts/open-xcode.sh
```

Select one of these generated schemes:

- `glyphastore_demo` to inspect detected topology.
- `glyphastore_tests` to build and run the test executable.
- `glyphastore_benchmarks` in Release configuration only for local measurements.
- `ALL_BUILD` when changing CMake structure.

Use Product > Scheme > Edit Scheme to add temporary diagnostics locally. Do not commit
`xcuserdata`. For repeatable sanitizer runs, prefer the repository presets:

```bash
./scripts/dev.sh asan
./scripts/dev.sh tsan
```

## Instruments

Build Release with debug information before profiling. In Xcode choose Product > Profile and use:

- Time Profiler for hot paths.
- Allocations for unexpected heap traffic.
- Counters for cache and branch behavior where supported.
- System Trace for scheduling and I/O effects.

Never publish benchmark results from a Debug, sanitizer, Rosetta, or thermally throttled run.
Record Mac model, CPU, OS, Xcode/Clang, payload distribution, dataset size, and build commit.

## Apple Silicon and Intel

CMake defaults to the host architecture. To cross-generate locally, create an untracked
`CMakeUserPresets.json` inheriting from `xcode` and set `CMAKE_OSX_ARCHITECTURES` to `arm64` or
`x86_64`. Universal binaries are not the default because they slow the inner development loop and
hide architecture-specific optimization decisions.

## CLI shortcuts

The root `Makefile` wraps the same scripts:

```bash
make bootstrap
make xcode
make test
make asan
make benchmark
```

## Troubleshooting

- Regenerate after changing CMake: `./scripts/open-xcode.sh`.
- Remove generated state: `./scripts/dev.sh clean` and bootstrap again.
- If Python cannot create a virtual environment, install a current Python 3 or provide `CMAKE` and
  `CTEST` environment variables pointing at existing tools.
- A TSan failure is not waived merely because the same test passes normally.
- Some Apple Xcode distributions do not ship a linkable macOS libFuzzer runtime. The normal,
  ASan, and TSan presets still use Apple Clang. To build fuzz targets locally, install a complete
  LLVM toolchain and configure `macos-fuzz` with `CC`/`CXX` pointing at that Clang. CI builds the
  fuzzers on Linux with a complete runtime.
