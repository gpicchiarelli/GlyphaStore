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
"$root/scripts/verify-cpp-client-package.sh"
"$root/scripts/checksum-sdk-artifacts.sh"

echo "All SDK packaging gates passed"
