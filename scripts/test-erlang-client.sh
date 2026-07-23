#!/usr/bin/env bash
# Run GlyphaStore Erlang SDK tests (requires Erlang/OTP >= 25 and rebar3).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk="$root/sdk/erlang"

if ! command -v rebar3 >/dev/null 2>&1; then
  echo "rebar3 is required (https://rebar3.org)" >&2
  exit 1
fi
if ! command -v erl >/dev/null 2>&1; then
  echo "Erlang/OTP is required (OTP >= 25 recommended)" >&2
  exit 1
fi

cd "$sdk"
rebar3 compile
rebar3 ct --cover=false
echo "Erlang SDK tests PASSED"
