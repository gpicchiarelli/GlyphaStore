#!/usr/bin/env bash
# Diagnostic coverage report (not a merge gate). See docs/development/test-strategy.md §7.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

outdir="${GLYPHASTORE_COVERAGE_OUT:-$root/build/coverage}"
builddir="$root/build/unix-coverage"
rm -rf "$outdir" "$builddir"
mkdir -p "$outdir"

export CC="${CC:-clang}"
export CXX="${CXX:-clang++}"
export CFLAGS="${CFLAGS:---coverage -O0 -g}"
export CXXFLAGS="${CXXFLAGS:---coverage -O0 -g}"
export LDFLAGS="${LDFLAGS:---coverage}"

cmake -S "$root" -B "$builddir" -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER="$CC" \
  -DCMAKE_CXX_COMPILER="$CXX" \
  -DCMAKE_C_FLAGS="$CFLAGS" \
  -DCMAKE_CXX_FLAGS="$CXXFLAGS" \
  -DCMAKE_EXE_LINKER_FLAGS="$LDFLAGS" \
  -DCMAKE_SHARED_LINKER_FLAGS="$LDFLAGS" \
  -DGLYPHASTORE_FAULT_INJECTION=ON \
  -DBUILD_TESTING=ON

cmake --build "$builddir" --target glyphastore_tests
ctest --test-dir "$builddir" --output-on-failure

if command -v lcov >/dev/null 2>&1; then
  lcov --capture --directory "$builddir" --output-file "$outdir/coverage.raw.lcov" \
    --ignore-errors mismatch,gcov,unused || \
    lcov --capture --directory "$builddir" --output-file "$outdir/coverage.raw.lcov" || true
  if [[ -f "$outdir/coverage.raw.lcov" ]]; then
    lcov --remove "$outdir/coverage.raw.lcov" \
      '/usr/*' '*/tests/*' '*/_deps/*' \
      --output-file "$outdir/coverage.lcov" \
      --ignore-errors unused || cp "$outdir/coverage.raw.lcov" "$outdir/coverage.lcov"
    lcov --list "$outdir/coverage.lcov" >"$outdir/coverage-report.txt" || true
  fi
elif command -v llvm-cov >/dev/null 2>&1; then
  echo "lcov not found; writing note for llvm-cov-only hosts" >"$outdir/coverage-report.txt"
  find "$builddir" -name '*.gcda' | head >"$outdir/gcda-files.txt" || true
fi

echo "Coverage artifacts under $outdir (diagnostic only; not an acceptance gate)."
ls -la "$outdir" || true
# Always succeed: coverage is diagnostic, never a merge gate.
exit 0
