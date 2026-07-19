#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
perl="${PERL:-perl}"

cd "$root"
PERL5LIB="$root/sdk/perl/lib" "$perl" -MTest::Harness -e \
  'runtests(@ARGV)' "$root/sdk/perl/t/01-protocol.t" "$root/sdk/perl/t/02-client.t"
