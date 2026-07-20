#!/usr/bin/env bash
# Verify the C++ TCP client is installable via CMake package config.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
expected="$(tr -d '[:space:]' <"$root/VERSION")"

prefer=(
  "$root/build/macos-native-release"
  "$root/build/macos-release"
  "$root/build/unix-release"
  "$root/build/macos-debug"
  "$root/build/unix-debug"
)

build_dir="${GLYPHASTORE_BUILD:-}"
if [[ -z "$build_dir" ]]; then
  for dir in "${prefer[@]}"; do
    if [[ -f "$dir/CMakeCache.txt" ]]; then
      build_dir="$dir"
      break
    fi
  done
fi
if [[ -z "$build_dir" || ! -f "$build_dir/CMakeCache.txt" ]]; then
  echo "missing configured CMake build (set GLYPHASTORE_BUILD=...)" >&2
  exit 1
fi

cmake_bin="${CMAKE:-}"
if [[ -z "$cmake_bin" ]]; then
  if [[ -x "$root/.tools/venv/lib/python3.13/site-packages/cmake/data/bin/cmake" ]]; then
    cmake_bin="$root/.tools/venv/lib/python3.13/site-packages/cmake/data/bin/cmake"
  else
    cmake_bin="$(command -v cmake)"
  fi
fi

prefix="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-cpp-prefix.XXXXXX")"
work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-cpp-consumer.XXXXXX")"
cleanup() { rm -rf "$prefix" "$work"; }
trap cleanup EXIT

"$cmake_bin" --build "$build_dir" -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 2)" \
  --target glyphastore_core glyphastore_wire glyphastore_client 2>/dev/null \
  || "$cmake_bin" --build "$build_dir" -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 2)"

"$cmake_bin" --install "$build_dir" --prefix "$prefix"

# Prefer the existing consumer smoke test when present.
if [[ -d "$root/tests/consumer" ]]; then
  "$cmake_bin" -S "$root/tests/consumer" -B "$work/build" \
    -DCMAKE_PREFIX_PATH="$prefix" \
    -DCMAKE_BUILD_TYPE=Release
  "$cmake_bin" --build "$work/build" -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 2)"
  ctest --test-dir "$work/build" --output-on-failure
else
  cat >"$work/CMakeLists.txt" <<EOF
cmake_minimum_required(VERSION 3.20)
project(glyphastore_client_pkg_check LANGUAGES CXX)
find_package(GlyphaStore $expected REQUIRED CONFIG)
add_executable(check_client check.cpp)
target_link_libraries(check_client PRIVATE GlyphaStore::client)
EOF
  cat >"$work/check.cpp" <<'EOF'
#include <glyphastore/client/client.hpp>
int main() { return 0; }
EOF
  "$cmake_bin" -S "$work" -B "$work/build" -DCMAKE_PREFIX_PATH="$prefix"
  "$cmake_bin" --build "$work/build"
fi

mkdir -p "$root/sdk/cpp-dist"
{
  echo "cmake_package=GlyphaStore"
  echo "target=GlyphaStore::client"
  echo "version=$expected"
  echo "prefix_smoke=$prefix"
  echo "note=C++ client ships via CMake install; not a language-registry tarball"
} >"$root/sdk/cpp-dist/package-info.txt"

echo "C++ client packaging verification OK (GlyphaStore::client via CMake)"
