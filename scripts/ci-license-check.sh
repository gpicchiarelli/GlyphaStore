#!/usr/bin/env bash
# License hygiene for official SDK trees (Go + Python). Fail closed on unexpected GPL.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
failed=0

echo "== Go licenses (sdk/go) =="
if ! command -v go >/dev/null 2>&1; then
  echo "go toolchain required" >&2
  exit 1
fi
GOBIN="$tmpdir" go install github.com/google/go-licenses@v1.6.0
(
  cd "$root/sdk/go"
  # Module currently has no third-party require directives; still scan the tree.
  "$tmpdir/go-licenses" report ./... >"$tmpdir/go-licenses.txt" 2>"$tmpdir/go-licenses.err" || true
)
if [[ -s "$tmpdir/go-licenses.err" ]]; then
  # go-licenses warns on stdlib-only modules; treat hard errors only.
  if grep -Ei 'error|fatal' "$tmpdir/go-licenses.err" >/dev/null; then
    cat "$tmpdir/go-licenses.err" >&2
  fi
fi
if [[ -s "$tmpdir/go-licenses.txt" ]] && grep -Ei 'GPL|AGPL|SSPL|Commons Clause' "$tmpdir/go-licenses.txt"; then
  echo "ERROR: disallowed copyleft/commercial-clause license in Go deps" >&2
  failed=1
else
  echo "Go license scan OK ($(wc -l <"$tmpdir/go-licenses.txt" | tr -d ' ') rows)"
fi

echo "== Python licenses (sdk/python) =="
venv="$tmpdir/pyvenv"
python3 -m venv "$venv"
# shellcheck disable=SC1091
source "$venv/bin/activate"
python -m pip install --disable-pip-version-check --quiet 'pip-licenses>=5' >/dev/null
python -m pip install --disable-pip-version-check --quiet -e "$root/sdk/python" >/dev/null
pip-licenses --format=plain --with-urls >"$tmpdir/pip-licenses.txt"
deactivate
if [[ -s "$tmpdir/pip-licenses.txt" ]]; then
  # Fail only on exclusive copyleft (dual BSD/MIT + GPL options are common for docutils etc.).
  if ! python3 - "$tmpdir/pip-licenses.txt" <<'PY'
import re, sys
path = sys.argv[1]
bad = []
for line in open(path, encoding="utf-8"):
    if not line.strip() or line.lower().startswith("name") or set(line.strip()) <= {"-", "="}:
        continue
    parts = re.split(r"\s{2,}", line.strip())
    if len(parts) < 3:
        continue
    name, license_name = parts[0], parts[2]
    if name.lower() in {"glyphastore", "pip", "setuptools", "wheel", "pip-licenses"}:
        continue
    has_copyleft = re.search(r"\b(AGPL|GPL|SSPL|Commons Clause)\b", license_name, re.I)
    has_permissive = re.search(
        r"\b(Apache|BSD|MIT|ISC|MPL|Unlicense|0BSD|CC0|PSF|Python|Zlib|Public Domain|BlueOak)\b",
        license_name,
        re.I,
    )
    if has_copyleft and not has_permissive:
        bad.append(f"{name}: {license_name}")
if bad:
    print("ERROR: exclusive copyleft licenses:\n" + "\n".join(bad), file=sys.stderr)
    sys.exit(1)
PY
  then
    failed=1
  else
    echo "Python license scan OK"
    head -20 "$tmpdir/pip-licenses.txt" || true
  fi
else
  echo "ERROR: empty Python license report" >&2
  failed=1
fi

if [[ "$failed" -ne 0 ]]; then
  echo "License check FAILED" >&2
  exit 1
fi
echo "License check OK"
