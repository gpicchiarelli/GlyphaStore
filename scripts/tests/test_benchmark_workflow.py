from __future__ import annotations

import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "benchmarks.yml"
CONTRACT = ROOT / "engineering" / "performance" / "hosted-benchmark-contract.json"
EXPECTED_PUSH_PATHS = (
    ".github/workflows/benchmarks.yml",
    "CMakeLists.txt",
    "CMakePresets.json",
    "VERSION",
    "cmake/**",
    "include/**",
    "src/**",
    "benchmarks/**",
    "engineering/performance/hosted-benchmark-contract.json",
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

    def test_hosted_matrix_contract_has_all_twenty_one_sources(self) -> None:
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        expected = [
            "core.txt",
            "parallel-uniform.txt",
            "parallel-worker-affine.txt",
            "parallel-single-worker.txt",
            "parallel-zipf.txt",
            "durable-sync.txt",
            "durable-periodic.txt",
            "durable-group.txt",
            *[
                f"server-tcp-w{workers}-p{pipeline}.txt"
                for workers in (1, 2, 4)
                for pipeline in (1, 8, 32, 128)
            ],
            "server-latency-w2-p32.txt",
        ]
        self.assertEqual(contract["expected_sources"], expected)
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("for distribution in uniform worker-affine single-worker zipf", text)
        self.assertIn("for workers in 1 2 4", text)
        self.assertIn("for pipeline in 1 8 32 128", text)
        self.assertIn(
            "--source-contract engineering/performance/hosted-benchmark-contract.json", text
        )


if __name__ == "__main__":
    unittest.main()
