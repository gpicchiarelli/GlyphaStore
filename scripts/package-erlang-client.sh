#!/usr/bin/env bash
# Verify Erlang SDK packaging readiness (fixtures, compile, CT, version lock, CLIs).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GLYPHASTORE_ROOT="$root"
# shellcheck disable=SC1091
source "$root/scripts/export-reproducible-build-env.sh"
sdk="$root/sdk/erlang"

if ! command -v rebar3 >/dev/null 2>&1; then
  echo "rebar3 is required (macOS: sudo port install rebar3)" >&2
  exit 1
fi
if ! command -v erl >/dev/null 2>&1; then
  echo "Erlang/OTP is required (macOS: sudo port install erlang; OTP >= 25)" >&2
  exit 1
fi

for fixture in wire_requests_v2.hex wire_responses_v2.hex; do
  if ! cmp -s "$root/tests/fixtures/$fixture" "$sdk/test/fixtures/$fixture"; then
    echo "vendored fixture drift: $fixture" >&2
    echo "run ./scripts/sync-sdk-fixtures.sh" >&2
    exit 1
  fi
done

(
  cd "$sdk"
  rebar3 compile
  rebar3 ct --cover=false
)

expected="$(tr -d '[:space:]' <"$root/VERSION")"
got="$(erl -noshell -pa "$sdk/_build/default/lib/glyphastore/ebin" \
  -eval 'io:format("~s", [glyphastore_version:version()]), halt().')"
if [[ "$got" != "$expected" ]]; then
  echo "erlang glyphastore_version:version()='$got' does not match VERSION='$expected'" >&2
  exit 1
fi

chmod +x "$sdk/scripts/glyphastore-interop.escript" "$sdk/scripts/glyphastore-version.escript" \
  "$sdk/benchmarks/client_benchmark.escript"
escript "$sdk/scripts/glyphastore-version.escript" >/dev/null

mkdir -p "$sdk/dist"
{
  echo "package=glyphastore"
  echo "version=$got"
  echo "otp=$(erl -noshell -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt().')"
  echo "rebar3=$(rebar3 version | head -1)"
  echo "source_date_epoch=$SOURCE_DATE_EPOCH"
  echo "built_at=$(glyphastore_repro_iso8601)"
} >"$sdk/dist/package-info.txt"

echo "Erlang packaging verification OK ($sdk/dist/package-info.txt)"
echo "Publish path: Hex package glyphastore@$got (see sdk/erlang/PACKAGING.md)"
