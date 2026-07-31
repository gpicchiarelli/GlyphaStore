#!/usr/bin/env bash
# Verify Go SDK packaging readiness (fixtures, tests, builds, version lock).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GLYPHASTORE_ROOT="$root"
# shellcheck disable=SC1091
source "$root/scripts/export-reproducible-build-env.sh"
go_bin="${GO:-go}"
sdk="$root/sdk/go"

for fixture in wire_requests_v2.hex wire_responses_v2.hex; do
  if ! cmp -s "$root/tests/fixtures/$fixture" "$sdk/testdata/$fixture"; then
    echo "vendored fixture drift: $fixture" >&2
    echo "run ./scripts/sync-sdk-fixtures.sh" >&2
    exit 1
  fi
done

expected="$(tr -d '[:space:]' <"$root/VERSION")"
got="$("$go_bin" -C "$sdk" run ./cmd/glyphastore-version)"
if [[ "$got" != "$expected" ]]; then
  echo "go client.Version='$got' does not match VERSION='$expected'" >&2
  exit 1
fi

(
  cd "$sdk"
  "$go_bin" test ./...
  "$go_bin" test -race ./client ./protocol
  mkdir -p bin
  "$go_bin" build -o bin/glyphastore-interop ./cmd/glyphastore-interop
  "$go_bin" build -o bin/glyphastore-bench ./cmd/glyphastore-bench
  "$go_bin" build -o bin/glyphastore-version ./cmd/glyphastore-version
  if "$go_bin" help mod tidy >/dev/null 2>&1; then
    if "$go_bin" mod tidy -diff 2>/dev/null; then
      :
    else
      # go < 1.21 may lack -diff; fall back to tidy + git status on go.mod
      cp go.mod go.mod.before
      "$go_bin" mod tidy
      if ! cmp -s go.mod go.mod.before; then
        echo "go.mod not tidy" >&2
        diff -u go.mod.before go.mod || true
        mv go.mod.before go.mod
        exit 1
      fi
      rm -f go.mod.before
    fi
  fi
)

mkdir -p "$sdk/dist"
{
  echo "module=github.com/gpicchiarelli/GlyphaStore/sdk/go"
  echo "version=$got"
  echo "tag_hint=sdk/go/v$got"
  echo "go=$("$go_bin" version)"
  echo "source_date_epoch=$SOURCE_DATE_EPOCH"
  echo "built_at=$(glyphastore_repro_iso8601)"
} >"$sdk/dist/package-info.txt"

echo "Go packaging verification OK ($sdk/dist/package-info.txt)"
echo "Publish path: git tag sdk/go/v$got && git push origin sdk/go/v$got"
