#!/usr/bin/env bash
# Package and verify every official language SDK, then checksum artifacts.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"$root/scripts/check-sdk-versions.sh"
"$root/scripts/sync-sdk-fixtures.sh"
"$root/scripts/package-python-client.sh"
"$root/scripts/package-perl-client.sh"
"$root/scripts/package-go-client.sh"
"$root/scripts/package-ruby-client.sh"
if command -v erl >/dev/null 2>&1 && command -v rebar3 >/dev/null 2>&1; then
  "$root/scripts/package-erlang-client.sh"
else
  echo "note: skipping Erlang packaging (erl/rebar3 not on PATH)" >&2
fi
"$root/scripts/verify-cpp-client-package.sh"
"$root/scripts/checksum-sdk-artifacts.sh"

echo "All SDK packaging gates passed"
