#!/usr/bin/env bash
# Prove the built Python wheel against the real secure-profile daemon.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="${PYTHON:-python3}"
daemon="${GLYPHASTORED:-}"
cpp_source="${GLYPHASTORE_INTEROP_CLIENT:-}"
go_source="${GLYPHASTORE_GO_INTEROP:-$root/sdk/go/bin/glyphastore-interop}"

if [[ -z "$daemon" || ! -x "$daemon" ]]; then
  echo "GLYPHASTORED must name a TLS-capable daemon" >&2
  exit 1
fi
if [[ -z "$cpp_source" || ! -x "$cpp_source" ]]; then
  echo "GLYPHASTORE_INTEROP_CLIENT must name the built C++ interop peer" >&2
  exit 1
fi
if [[ ! -x "$go_source" ]]; then
  echo "missing Go interop peer: $go_source" >&2
  exit 1
fi

shopt -s nullglob
wheels=("$root"/sdk/python/dist/glyphastore-*.whl)
shopt -u nullglob
if [[ "${#wheels[@]}" -ne 1 ]]; then
  echo "expected exactly one built Python wheel under sdk/python/dist" >&2
  exit 1
fi
wheel="${wheels[0]}"

work="$(mktemp -d "${TMPDIR:-/tmp}/glyphastore-python-wheel-secure.XXXXXX")"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT
venv="$work/venv"
mkdir -p "$work/bin"
cp "$cpp_source" "$work/bin/glyphastore-interop-cpp"
cp "$go_source" "$work/bin/glyphastore-interop-go"

"$python" -m venv "$venv"
"$venv/bin/python" -m pip install --disable-pip-version-check -q --no-deps "$wheel"
loaded_from="$("$venv/bin/python" -c 'import glyphastore; print(glyphastore.__file__)')"
if [[ "$loaded_from" == "$root/"* ]]; then
  echo "wheel smoke resolved Python SDK from source: $loaded_from" >&2
  exit 1
fi
echo "installed Python wheel: $wheel"
echo "loaded Python SDK: $loaded_from"

GLYPHASTORE_INTEROP_USE_INSTALLED=1 \
SECURE_INTEROP_SKIP_PERL=1 \
SECURE_INTEROP_SKIP_RUBY=1 \
SECURE_INTEROP_SKIP_ERLANG=1 \
PYTHON="$venv/bin/python" \
PYTHONPATH= \
PERL5LIB= \
RUBYLIB= \
GLYPHASTORED="$daemon" \
GLYPHASTORE_INTEROP_CLIENT="$work/bin/glyphastore-interop-cpp" \
GLYPHASTORE_GO_INTEROP="$work/bin/glyphastore-interop-go" \
"$root/scripts/test-secure-profile-interop.sh"

echo "Python wheel secure-profile interop PASSED ($wheel)"
