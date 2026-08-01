#!/usr/bin/env python3
"""Fail if any GitHub Actions workflow uses a mutable tag instead of a commit SHA."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

USES_RE = re.compile(r"^\s*-?\s*uses:\s*(\S+)")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
LOCAL_RE = re.compile(r"^\./")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=None)
    args = parser.parse_args()
    root = (args.root or Path(__file__).resolve().parents[2]).resolve()
    workflows = sorted((root / ".github" / "workflows").glob("*.yml"))
    errors: list[str] = []
    pinned = 0
    for path in workflows:
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            match = USES_RE.match(line)
            if not match:
                continue
            action = match.group(1).split("#", 1)[0].strip()
            if LOCAL_RE.match(action) or action.startswith("docker://"):
                continue
            if "@" not in action:
                errors.append(f"{path.relative_to(root)}:{lineno}: uses without @ref: {action}")
                continue
            name, ref = action.rsplit("@", 1)
            if SHA_RE.fullmatch(ref):
                pinned += 1
                continue
            errors.append(
                f"{path.relative_to(root)}:{lineno}: mutable Action ref "
                f"{name}@{ref} (require 40-char commit SHA)"
            )
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        print(f"Actions pin validation FAILED ({len(errors)} error(s))", file=sys.stderr)
        return 1
    print(f"Actions pin validation OK ({pinned} SHA-pinned uses across {len(workflows)} workflows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
