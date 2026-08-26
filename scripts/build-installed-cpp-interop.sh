#!/usr/bin/env bash
# Build the interop peer exclusively through an installed GlyphaStore CMake package.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
artifact_root="${1:-}"
output="${2:-}"
cmake_bin="${CMAKE:-}"

if [[ -n "$cmake_bin" && ! -x "$cmake_bin" ]]; then
  cmake_bin="$(command -v "$cmake_bin" || true)"
elif [[ -z "$cmake_bin" ]]; then
  cmake_bin="$(command -v cmake || true)"
fi
if [[ -z "$cmake_bin" && -x "$root/.tools/venv/lib/python3.13/site-packages/cmake/data/bin/cmake" ]]; then
  cmake_bin="$root/.tools/venv/lib/python3.13/site-packages/cmake/data/bin/cmake"
fi

if [[ -z "$artifact_root" || -z "$output" ]]; then
  echo "usage: $0 BUILD_DIR_OR_INSTALLED_PREFIX OUTPUT" >&2
  exit 2
fi
if [[ -z "$cmake_bin" || ! -x "$cmake_bin" ]]; then
  echo "CMake is required to build the installed C++ interop peer" >&2
  exit 1
fi

expected="$(tr -d '[:space:]' <"$root/VERSION")"
work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-cpp-artifact.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT
prefix="$work/prefix"
consumer="$work/consumer"
mkdir -p "$prefix" "$consumer"

if [[ -f "$artifact_root/CMakeCache.txt" ]]; then
  "$cmake_bin" --build "$artifact_root" --target glyphastore_client
  "$cmake_bin" --install "$artifact_root" --prefix "$prefix" --component Development
elif [[ -d "$artifact_root" ]]; then
  prefix="$(cd "$artifact_root" && pwd -P)"
else
  echo "first argument must be a configured build or installed prefix: $artifact_root" >&2
  exit 1
fi

configs=()
if command -v rg >/dev/null 2>&1; then
  while IFS= read -r config; do
    configs+=("$config")
  done < <(rg --files "$prefix" | rg '/GlyphaStoreConfig\.cmake$')
else
  while IFS= read -r config; do
    configs+=("$config")
  done < <(find "$prefix" -type f -name GlyphaStoreConfig.cmake -print)
fi
if [[ "${#configs[@]}" -ne 1 ]]; then
  echo "installed prefix must contain exactly one GlyphaStoreConfig.cmake" >&2
  exit 1
fi
package_dir="$(dirname "${configs[0]}")"

cp "$root/tools/interop_client.cpp" "$consumer/interop_client.cpp"
cat >"$consumer/CMakeLists.txt" <<EOF
cmake_minimum_required(VERSION 3.25)
project(GlyphaStoreInstalledInterop LANGUAGES CXX)
find_package(GlyphaStore $expected EXACT REQUIRED CONFIG)
add_executable(glyphastore-interop-cpp interop_client.cpp)
target_link_libraries(glyphastore-interop-cpp PRIVATE GlyphaStore::client)
target_compile_features(glyphastore-interop-cpp PRIVATE cxx_std_23)
if(DEFINED ENV{GLYPHASTORE_SANITIZERS} AND NOT "\$ENV{GLYPHASTORE_SANITIZERS}" STREQUAL "")
  target_compile_options(glyphastore-interop-cpp PRIVATE -fno-omit-frame-pointer
    "-fsanitize=\$ENV{GLYPHASTORE_SANITIZERS}")
  target_link_options(glyphastore-interop-cpp PRIVATE
    "-fsanitize=\$ENV{GLYPHASTORE_SANITIZERS}")
endif()
EOF

"$cmake_bin" -S "$consumer" -B "$consumer/build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DGlyphaStore_DIR="$package_dir" \
  -DCMAKE_FIND_USE_PACKAGE_REGISTRY=FALSE
"$cmake_bin" --build "$consumer/build" --target glyphastore-interop-cpp

built="$consumer/build/glyphastore-interop-cpp"
if [[ ! -x "$built" ]]; then
  echo "installed C++ package did not produce glyphastore-interop-cpp" >&2
  exit 1
fi
mkdir -p "$(dirname "$output")"
cp "$built" "$output"
"$output" --help >/dev/null 2>&1

echo "Installed C++ package interop build OK ($output)"
