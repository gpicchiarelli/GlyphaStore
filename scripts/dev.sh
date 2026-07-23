#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cmake="${CMAKE:-$root/.tools/venv/bin/cmake}"
ctest="${CTEST:-$root/.tools/venv/bin/ctest}"
export PATH="$root/.tools/venv/bin:$PATH"

require_tools() {
    if [[ ! -x "$cmake" ]]; then
        echo "Local CMake is missing. Run ./scripts/bootstrap-macos.sh first." >&2
        exit 1
    fi
}

case "${1:-help}" in
    configure)
        require_tools
        "$cmake" --preset macos-debug
        ;;
    build)
        require_tools
        "$cmake" --build --preset macos-debug
        ;;
    test)
        require_tools
        "$cmake" --preset macos-debug
        "$cmake" --build --preset macos-debug
        "$ctest" --preset macos-debug
        ;;
    asan)
        require_tools
        "$cmake" --preset macos-asan
        "$cmake" --build --preset macos-asan
        "$ctest" --preset macos-asan
        ;;
    tsan)
        require_tools
        "$cmake" --preset macos-tsan
        "$cmake" --build --preset macos-tsan
        "$ctest" --preset macos-tsan
        ;;
    benchmark)
        require_tools
        "$cmake" --preset macos-release
        "$cmake" --build --preset macos-release --target glyphastore_benchmarks
        "$root/build/macos-release/glyphastore_benchmarks" "${@:2}"
        ;;
    benchmark-lto)
        require_tools
        "$cmake" --preset macos-release-lto
        "$cmake" --build --preset macos-release-lto --target glyphastore_benchmarks
        "$root/build/macos-release-lto/glyphastore_benchmarks" "${@:2}"
        ;;
    benchmark-server)
        require_tools
        "$cmake" --preset macos-release
        "$cmake" --build --preset macos-release --target glyphastore_server_benchmarks
        "$root/build/macos-release/glyphastore_server_benchmarks" "${@:2}"
        ;;
    benchmark-pgo)
        require_tools
        "$cmake" --preset macos-pgo-use
        "$cmake" --build --preset macos-pgo-use --target glyphastore_benchmarks
        "$root/build/macos-pgo-use/glyphastore_benchmarks" "${@:2}"
        ;;
    benchmark-durable)
        require_tools
        "$cmake" --preset macos-release
        "$cmake" --build --preset macos-release --target glyphastore_benchmarks
        "$root/build/macos-release/glyphastore_benchmarks" --filter store-durable-all "${@:2}"
        ;;
    benchmark-compaction)
        require_tools
        "$cmake" --preset macos-release
        "$cmake" --build --preset macos-release --target glyphastore_compaction_benchmark
        "$root/build/macos-release/glyphastore_compaction_benchmark" "${@:2}"
        ;;
    benchmark-maintenance)
        require_tools
        "$cmake" --preset macos-release
        "$cmake" --build --preset macos-release --target glyphastore_maintenance_benchmark
        "$root/build/macos-release/glyphastore_maintenance_benchmark" "${@:2}"
        ;;
    pgo-generate)
        require_tools
        "$cmake" --preset macos-pgo-generate
        "$cmake" --build --preset macos-pgo-generate --target glyphastore_benchmarks
        ;;
    pgo-train)
        require_tools
        "$cmake" --preset macos-pgo-generate
        "$cmake" --build --preset macos-pgo-generate --target glyphastore_benchmarks glyphastore_pgo_durable
        PGO_PRESET=macos-pgo-generate "$root/scripts/pgo-train.sh"
        ;;
    pgo-use)
        require_tools
        "$cmake" --preset macos-pgo-use
        "$cmake" --build --preset macos-pgo-use --target glyphastore_benchmarks
        ;;
    fuzz-build)
        require_tools
        "$cmake" --preset macos-fuzz
        "$cmake" --build --preset macos-fuzz
        ;;
    test-lto)
        require_tools
        "$cmake" --preset macos-release-lto
        "$cmake" --build --preset macos-release-lto
        "$ctest" --preset macos-release-lto
        ;;
    xcode-build)
        require_tools
        "$cmake" --preset xcode
        "$cmake" --build --preset xcode-debug
        "$ctest" --preset xcode-debug
        ;;
    format)
        formatter="$(command -v clang-format || true)"
        if [[ -z "$formatter" ]]; then
            echo "clang-format not found; install LLVM with MacPorts or Homebrew." >&2
            exit 1
        fi
        find "$root/include" "$root/src" "$root/tests" "$root/benchmarks" "$root/fuzz" \
            -type f \( -name '*.cpp' -o -name '*.hpp' \) -print0 | xargs -0 "$formatter" -i
        ;;
    clean)
        "$cmake" -E rm -rf "$root/build"
        ;;
    *)
        echo "usage: $0 {configure|build|test|asan|tsan|test-lto|benchmark|benchmark-durable|benchmark-compaction|benchmark-maintenance|benchmark-server|benchmark-lto|benchmark-pgo|pgo-generate|pgo-train|pgo-use|fuzz-build|xcode-build|format|clean} [benchmark args]"
        echo "PGO: pgo-generate builds instrumented benchmarks; pgo-train runs volatile + durable workloads; pgo-use rebuilds optimized benchmarks."
        ;;
esac
