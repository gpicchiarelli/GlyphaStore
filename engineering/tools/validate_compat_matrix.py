#!/usr/bin/env python3
"""Validate N↔N-1 compatibility matrix structure and referenced evidence."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    print("PyYAML required", file=sys.stderr)
    sys.exit(2)

ALLOWED_STATUS = {
    "supported",
    "supported_offline_only",
    "intentionally_rejected",
    "unsupported",
    "not_promised",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=None)
    args = parser.parse_args()
    root = (args.root or Path(__file__).resolve().parents[2]).resolve()
    path = root / "engineering" / "compatibility" / "n-n1-matrix.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    errors: list[str] = []

    for section in ("store_open", "wire", "abi"):
        if section not in doc:
            errors.append(f"missing section {section}")
            continue
        rows = doc[section]
        if not isinstance(rows, list) or not rows:
            errors.append(f"{section} must be a non-empty list")
            continue
        ids: set[str] = set()
        for row in rows:
            rid = row.get("id", "<missing>")
            if rid in ids:
                errors.append(f"duplicate row id {rid}")
            ids.add(rid)
            status = row.get("status")
            if status not in ALLOWED_STATUS:
                errors.append(f"{rid}: invalid status {status!r}")
            for ev in row.get("evidence", []) or []:
                if isinstance(ev, str):
                    if not (root / ev).exists():
                        errors.append(f"{rid}: missing evidence path {ev}")
                elif isinstance(ev, dict) and "workflow" in ev:
                    wf = root / ".github" / "workflows" / ev["workflow"]
                    if not wf.is_file():
                        errors.append(f"{rid}: missing workflow {ev['workflow']}")

    policy = doc.get("released_fixture_policy", {})
    for label in policy.get("in_tree_labels", []):
        label_dir = root / "tests" / "fixtures" / "released" / label
        if not label_dir.is_dir():
            errors.append(f"in_tree label missing: tests/fixtures/released/{label}")
    script = policy.get("packaging_script")
    if script and not (root / script).is_file():
        errors.append(f"missing packaging script {script}")

    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        print(f"Compat matrix validation FAILED ({len(errors)} error(s))", file=sys.stderr)
        return 1
    n = sum(len(doc.get(s, [])) for s in ("store_open", "wire", "abi"))
    print(f"Compat matrix validation OK ({n} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
