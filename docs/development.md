# Development

The project uses CMake 3.25+, C++23, strict warnings, CTest, sanitizer presets, and standalone
fuzz/benchmark targets. macOS instructions are in [development-macos.md](development-macos.md).

Portable commands:

```bash
cmake --preset unix-debug
cmake --build --preset unix-debug
ctest --preset unix-debug
```

Before review, run formatting, unit/integration tests, the sanitizer appropriate to the change,
and any focused benchmark. Build directories and generated Xcode projects are never committed.
