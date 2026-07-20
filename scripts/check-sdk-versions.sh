#!/usr/bin/env bash
# Assert every official SDK reports the same version as the repository VERSION file.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
expected="$(tr -d '[:space:]' <"$root/VERSION")"
if [[ ! "$expected" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "VERSION file is not a semver-like string: '$expected'" >&2
  exit 1
fi

fail=0
check() {
  local label="$1" actual="$2"
  if [[ "$actual" != "$expected" ]]; then
    echo "version mismatch: $label='$actual' expected='$expected'" >&2
    fail=1
  else
    echo "OK $label=$actual"
  fi
}

check "root/VERSION" "$expected"

py_ver="$(
  PYTHONPATH="$root/sdk/python/src${PYTHONPATH:+:$PYTHONPATH}" \
    "${PYTHON:-python3}" -c 'import glyphastore; print(glyphastore.__version__)'
)"
check "python/glyphastore.__version__" "$py_ver"

if ! command -v rg >/dev/null 2>&1; then
  echo "rg (ripgrep) is required for Perl VERSION checks" >&2
  exit 1
fi
perl_versions="$(rg -o "our \\\$VERSION = '([^']+)'" -r '$1' "$root/sdk/perl/lib" --no-filename | sort -u)"
perl_count="$(printf '%s\n' "$perl_versions" | grep -c . || true)"
if [[ "$perl_count" -ne 1 ]]; then
  echo "perl VERSION drift across modules: ${perl_versions:-<none>}" >&2
  fail=1
  perl_ver="<drift>"
else
  perl_ver="$perl_versions"
fi
check "perl/VERSION (all modules)" "$perl_ver"

go_ver="$("${GO:-go}" -C "$root/sdk/go" run ./cmd/glyphastore-version)"
check "go/client.Version" "$go_ver"

ruby_bin="${RUBY:-}"
if [[ -z "$ruby_bin" && -x "${HOME:-}/.local/bin/mise" ]]; then
  ruby_bin="$("${HOME}/.local/bin/mise" exec ruby@3.3 -- which ruby 2>/dev/null || true)"
fi
ruby_bin="${ruby_bin:-$(command -v ruby || true)}"
if [[ -z "$ruby_bin" ]]; then
  echo "ruby not found on PATH (set RUBY=...)" >&2
  exit 1
fi
ruby_ver="$(
  RUBYLIB="$root/sdk/ruby/lib${RUBYLIB:+:$RUBYLIB}" \
    "$ruby_bin" -e 'require "glypha_store"; print GlyphaStore::VERSION'
)"
check "ruby/GlyphaStore::VERSION" "$ruby_ver"

check "cmake/PROJECT_VERSION (VERSION file)" "$expected"

if [[ "$fail" -ne 0 ]]; then
  echo "SDK versions are not locked to $expected" >&2
  exit 1
fi
echo "All official SDK versions match $expected"
