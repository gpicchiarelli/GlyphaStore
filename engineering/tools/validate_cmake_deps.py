#!/usr/bin/env python3
"""Validate Phase C CMake layout and include dependency matrix."""

from __future__ import annotations

import argparse
import fnmatch
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    print("PyYAML required", file=sys.stderr)
    sys.exit(2)

INCLUDE_RE = re.compile(r'^\s*#\s*include\s*"([^"]+)"')


def iter_sources(root: Path, pattern: str) -> list[Path]:
    if "{" in pattern and "}" in pattern:
        pre, rest = pattern.split("{", 1)
        body, post = rest.split("}", 1)
        paths: list[Path] = []
        for alt in body.split(","):
            paths.extend(iter_sources(root, f"{pre}{alt}{post}"))
        return paths
    return [
        path
        for path in root.rglob("*")
        if path.is_file() and fnmatch.fnmatch(path.relative_to(root).as_posix(), pattern)
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=None)
    args = parser.parse_args()
    root = (args.root or Path(__file__).resolve().parents[2]).resolve()
    matrix_path = root / "engineering" / "build" / "dependency-matrix.yaml"
    matrix = yaml.safe_load(matrix_path.read_text(encoding="utf-8"))

    errors: list[str] = []
    for rel in matrix.get("required_cmake_files", []):
        if not (root / rel).is_file():
            errors.append(f"missing required CMake file: {rel}")
    cmake_root = (root / "CMakeLists.txt").read_text(encoding="utf-8")
    for sub in ("src", "tools", "tests", "benchmarks", "fuzz"):
        if f"add_subdirectory({sub})" not in cmake_root:
            errors.append(f"root CMakeLists.txt missing add_subdirectory({sub})")

    for rule in matrix.get("include_rules", []):
        rid = rule["id"]
        allow = set(rule.get("allow_include_prefix", []))
        forbid = rule["forbid_include_prefix"]
        for path in iter_sources(root, rule["from_glob"]):
            if path.suffix not in {".hpp", ".h", ".cpp", ".cc", ".cxx"}:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for line in text.splitlines():
                match = INCLUDE_RE.match(line)
                if not match:
                    continue
                inc = match.group(1)
                if inc in allow or any(inc.startswith(a) for a in allow if a.endswith("/")):
                    continue
                if inc == forbid or inc.startswith(forbid):
                    errors.append(
                        f"{rid}: {path.relative_to(root).as_posix()} includes forbidden '{inc}'"
                    )

    required_aliases = {"GlyphaStore::core", "GlyphaStore::server", "GlyphaStore::client"}
    found: set[str] = set()
    for spec in matrix.get("targets", {}).values():
        found.update(spec.get("aliases", []))
    for alias in sorted(required_aliases - found):
        errors.append(f"dependency matrix missing required alias {alias}")

    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        print(f"CMake dependency validation FAILED ({len(errors)} error(s))", file=sys.stderr)
        return 1
    print(
        f"CMake dependency validation OK "
        f"({len(matrix.get('targets', {}))} targets, "
        f"{len(matrix.get('include_rules', []))} include rules)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
