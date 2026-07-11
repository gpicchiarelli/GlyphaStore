#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cmake="$root/.tools/venv/bin/cmake"

if [[ ! -x "$cmake" ]]; then
    "$root/scripts/bootstrap-macos.sh"
else
    PATH="$root/.tools/venv/bin:$PATH" "$cmake" --preset xcode
fi

open "$root/build/xcode/GlyphaStore.xcodeproj"
