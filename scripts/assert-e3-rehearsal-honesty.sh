#!/usr/bin/env bash
# Fail closed if an E3 harness or campaign-prep artifact claims certification.
# Hosted CI and disposable-image rows must remain e3_certified=no.
set -euo pipefail

kind="auto"
artifact_dir=""

usage() {
  cat <<'EOF'
Usage:
  scripts/assert-e3-rehearsal-honesty.sh --dir DIR [--kind auto|harness|campaign]

Validates that DIR is labeled rehearsal / campaign-prep:
  - e3_certified=no and e4_certified=no in provenance
  - summary marks E3/E4 certified as no
  - never contains e3_certified=yes / E3 certified: `yes`
  - campaign artifacts keep promotion_candidate != yes (scripts never promote)

Exit 0 on honesty pass; non-zero on missing files or certification claims.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      [[ $# -ge 2 ]] || { echo "error: --dir requires a path" >&2; exit 2; }
      artifact_dir="$2"
      shift 2
      ;;
    --kind)
      [[ $# -ge 2 ]] || { echo "error: --kind requires a value" >&2; exit 2; }
      kind="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n "$artifact_dir" ]] || { echo "error: --dir is required" >&2; exit 2; }
[[ -d "$artifact_dir" ]] || { echo "error: not a directory: $artifact_dir" >&2; exit 2; }
case "$kind" in
  auto|harness|campaign) ;;
  *)
    echo "error: --kind accepts auto|harness|campaign" >&2
    exit 2
    ;;
esac

if [[ "$kind" == "auto" ]]; then
  if [[ -f "$artifact_dir/campaign-provenance.txt" ]]; then
    kind="campaign"
  elif [[ -f "$artifact_dir/provenance.txt" ]]; then
    kind="harness"
  else
    echo "error: cannot detect kind; missing provenance in $artifact_dir" >&2
    exit 1
  fi
fi

fail=0
note() { printf '%s\n' "$*"; }
err() { printf 'error: %s\n' "$*" >&2; fail=1; }

require_file() {
  local path="$1"
  [[ -f "$path" ]] || err "missing required file: $path"
}

require_grep() {
  local path="$1"
  local pattern="$2"
  local label="$3"
  if [[ ! -f "$path" ]]; then
    return
  fi
  if ! grep -Eq "$pattern" "$path"; then
    err "$label (pattern /$pattern/ not found in $path)"
  fi
}

forbid_grep() {
  local path="$1"
  local pattern="$2"
  local label="$3"
  if [[ ! -f "$path" ]]; then
    return
  fi
  if grep -Eq "$pattern" "$path"; then
    err "$label (forbidden pattern /$pattern/ in $path)"
  fi
}

assert_common_honesty() {
  local prov="$1"
  local sum="$2"

  require_file "$prov"
  require_file "$sum"

  require_grep "$prov" '^e3_certified=no$' "provenance must set e3_certified=no"
  require_grep "$prov" '^e4_certified=no$' "provenance must set e4_certified=no"
  forbid_grep "$prov" '^e3_certified=yes$' "provenance must not set e3_certified=yes"
  forbid_grep "$prov" '^e4_certified=yes$' "provenance must not set e4_certified=yes"

  require_grep "$sum" 'E3 certified: `no`' "summary must mark E3 certified no"
  require_grep "$sum" 'E4 certified: `no`' "summary must mark E4 certified no"
  forbid_grep "$sum" 'E3 certified: `yes`' "summary must not claim E3 certified yes"
  forbid_grep "$sum" 'E4 certified: `yes`' "summary must not claim E4 certified yes"
}

if [[ "$kind" == "harness" ]]; then
  note "Asserting E3 harness honesty in $artifact_dir"
  assert_common_honesty "$artifact_dir/provenance.txt" "$artifact_dir/summary.md"
  require_grep "$artifact_dir/summary.md" 'Release-certification eligible: `no`' \
    "harness summary must keep release-certification eligible=no"
  forbid_grep "$artifact_dir/summary.md" 'Release-certification eligible: `yes`' \
    "harness summary must not claim release-certification eligible=yes"
elif [[ "$kind" == "campaign" ]]; then
  note "Asserting E3 campaign honesty in $artifact_dir"
  assert_common_honesty "$artifact_dir/campaign-provenance.txt" "$artifact_dir/campaign-summary.md"
  require_file "$artifact_dir/campaign-pin.txt"
  require_file "$artifact_dir/promotion-checklist.md"
  require_grep "$artifact_dir/campaign-provenance.txt" '^promotion_candidate=' \
    "campaign provenance must record promotion_candidate"
  forbid_grep "$artifact_dir/campaign-provenance.txt" '^promotion_candidate=yes$' \
    "scripts must never emit promotion_candidate=yes"
  if [[ -f "$artifact_dir/stages/e3/provenance.txt" ]]; then
    require_grep "$artifact_dir/stages/e3/provenance.txt" '^e3_certified=no$' \
      "nested E3 harness provenance must set e3_certified=no"
    forbid_grep "$artifact_dir/stages/e3/provenance.txt" '^e3_certified=yes$' \
      "nested E3 harness provenance must not set e3_certified=yes"
  fi
fi

if [[ "$fail" -ne 0 ]]; then
  echo "E3 honesty assert FAILED for kind=$kind dir=$artifact_dir" >&2
  exit 1
fi

echo "E3 honesty assert OK (kind=$kind)"
exit 0
