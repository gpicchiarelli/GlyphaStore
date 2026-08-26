from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.benchmark_report import (
    ENVIRONMENT_IDENTITY_FIELDS,
    add_comparisons,
    compare_with_baseline,
    comparison_environment_status,
    environment_identity,
    parse_environment,
    render_markdown,
)


def environment(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "runner_os": "Linux",
        "runner_arch": "X64",
        "runner_image": "ubuntu24",
        "runner_image_version": "20260820.1",
        "kernel_release": "6.11.0",
        "cpu_model": "Fixture CPU",
        "logical_cpu_count": 4,
        "compiler_identity": "clang version 20.1.0",
        "build_preset": "unix-release",
    }
    values.update(overrides)
    return values


def runs(rate: float) -> list[dict[str, object]]:
    return [
        {
            "source": "core.txt",
            "metadata": {},
            "results": [
                {
                    "name": "store_get",
                    "operations": 100,
                    "median_ops_per_second": rate,
                }
            ],
        }
    ]


class BenchmarkEnvironmentTests(unittest.TestCase):
    def test_environment_file_and_identity_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "environment.txt"
            path.write_text(
                "runner_os=Linux\nlogical_cpu_count=4\nignored line\n", encoding="utf-8"
            )
            parsed = parse_environment(path)
        self.assertEqual(parsed, {"runner_os": "Linux", "logical_cpu_count": 4})
        self.assertEqual(
            environment_identity(parsed), environment_identity(dict(reversed(parsed.items())))
        )

    def test_exact_identity_authorizes_comparison(self) -> None:
        current = environment()
        status = comparison_environment_status(current, {"environment": dict(current)})
        self.assertEqual(status["status"], "compatible")

        current_runs = runs(90.0)
        matched = add_comparisons(current_runs, {"runs": runs(100.0)})
        self.assertEqual(matched, 1)
        comparison = current_runs[0]["results"][0]["comparison"]
        self.assertEqual(comparison["median_ops_per_second_delta_percent"], -10.0)

    def test_hardware_change_suppresses_delta_and_is_rendered(self) -> None:
        current = environment(cpu_model="Current CPU")
        baseline = {"environment": environment(cpu_model="Prior CPU")}
        status = comparison_environment_status(current, baseline)
        self.assertEqual(status["status"], "incompatible")
        self.assertIn("cpu_model", status["differences"])

        current_runs = runs(90.0)
        applied_status, matched = compare_with_baseline(current_runs, baseline, current)
        self.assertEqual(applied_status["status"], "incompatible")
        self.assertEqual(matched, 0)
        self.assertNotIn("comparison", current_runs[0]["results"][0])

        markdown = render_markdown(runs(90.0), "now", "before", status)
        self.assertIn("throughput deltas are suppressed", markdown)
        self.assertIn("Current CPU", markdown)
        self.assertNotIn("-10.00%", markdown)

    def test_old_baseline_without_environment_is_not_compared(self) -> None:
        status = comparison_environment_status(environment(), {"schema_version": 2})
        self.assertEqual(status["status"], "incompatible")
        self.assertEqual(status["reason"], "baseline-environment-missing")

    def test_every_identity_field_is_required(self) -> None:
        current = environment()
        current[ENVIRONMENT_IDENTITY_FIELDS[0]] = "unknown"
        status = comparison_environment_status(current, {"environment": environment()})
        self.assertEqual(status["status"], "incompatible")
        self.assertEqual(status["reason"], "identity-fields-missing")


if __name__ == "__main__":
    unittest.main()
