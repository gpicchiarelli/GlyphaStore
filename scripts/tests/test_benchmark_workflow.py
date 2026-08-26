from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "benchmarks.yml"
EXPECTED_PUSH_PATHS = (
    ".github/workflows/benchmarks.yml",
    "CMakeLists.txt",
    "CMakePresets.json",
    "VERSION",
    "cmake/**",
    "include/**",
    "src/**",
    "benchmarks/**",
    "scripts/benchmark_report.py",
)


def push_paths(text: str) -> tuple[str, ...]:
    lines = text.splitlines()
    start = lines.index("    paths:") + 1
    paths: list[str] = []
    for line in lines[start:]:
        if not line.startswith("      - "):
            break
        paths.append(line.removeprefix("      - ").strip('"'))
    return tuple(paths)


class BenchmarkWorkflowTests(unittest.TestCase):
    def test_expensive_push_trigger_has_exact_dependency_scope(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(push_paths(text), EXPECTED_PUSH_PATHS)
        for excluded in ("docs/**", "artwork/**", "sdk/**"):
            self.assertNotIn(excluded, push_paths(text))

    def test_scheduled_and_manual_safety_nets_remain(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("  workflow_dispatch:", text)
        self.assertIn('    - cron: "23 4 * * 1"', text)
        self.assertIn("    branches: [main]", text)


if __name__ == "__main__":
    unittest.main()
