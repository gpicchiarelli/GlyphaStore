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
    regressions_over_threshold,
    render_markdown,
    validate_runs,
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


def runs(
    rate: float, minimum: float | None = None, maximum: float | None = None
) -> list[dict[str, object]]:
    return [
        {
            "source": "core.txt",
            "metadata": {},
            "results": [
                {
                    "name": "store_get",
                    "operations": 100,
                    "median_ops_per_second": rate,
                    "min_ops_per_second": minimum if minimum is not None else rate * 0.9,
                    "max_ops_per_second": maximum if maximum is not None else rate * 1.1,
                }
            ],
        }
    ]


def strict_runs() -> list[dict[str, object]]:
    fixture = runs(100.0, 90.0, 110.0)
    fixture[0]["metadata"] = {
        "git_sha": "abc123",
        "arch": "x86_64",
        "platform": "linux",
        "compiler": "clang 20",
        "benchmark_warmup": 1,
        "benchmark_repeats": 3,
    }
    fixture[0]["results"][0].update(
        {
            "samples": 3,
            "warmup": 1,
            "median_seconds": 1.0,
            "min_seconds": 0.9,
            "max_seconds": 1.1,
        }
    )
    return fixture


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
        self.assertEqual(comparison["interpretation"], "inconclusive-overlap")

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

    def test_disjoint_lower_range_is_regression_candidate(self) -> None:
        current_runs = runs(70.0, 65.0, 75.0)
        matched = add_comparisons(
            current_runs, {"runs": runs(100.0, 95.0, 105.0)}
        )
        self.assertEqual(matched, 1)
        comparison = current_runs[0]["results"][0]["comparison"]
        self.assertEqual(comparison["interpretation"], "regression-candidate")
        self.assertEqual(len(regressions_over_threshold(current_runs, 10.0)), 1)

    def test_overlap_never_triggers_optional_threshold(self) -> None:
        current_runs = runs(80.0, 70.0, 100.0)
        add_comparisons(current_runs, {"runs": runs(100.0, 90.0, 110.0)})
        self.assertEqual(
            current_runs[0]["results"][0]["comparison"]["interpretation"],
            "inconclusive-overlap",
        )
        self.assertEqual(regressions_over_threshold(current_runs, 10.0), [])
        markdown = render_markdown(
            current_runs, "now", "before", {"status": "compatible"}
        )
        self.assertIn("inconclusive (ranges overlap)", markdown)

    def test_disjoint_higher_range_is_improvement_candidate(self) -> None:
        current_runs = runs(130.0, 125.0, 135.0)
        add_comparisons(current_runs, {"runs": runs(100.0, 95.0, 105.0)})
        self.assertEqual(
            current_runs[0]["results"][0]["comparison"]["interpretation"],
            "improvement-candidate",
        )

    def test_strict_report_accepts_complete_results(self) -> None:
        validate_runs(strict_runs())

    def test_strict_report_rejects_empty_suite(self) -> None:
        fixture = strict_runs()
        fixture[0]["results"] = []
        with self.assertRaisesRegex(ValueError, "no benchmark results"):
            validate_runs(fixture)

    def test_strict_report_rejects_duplicate_result_key(self) -> None:
        fixture = strict_runs()
        fixture[0]["results"].append(dict(fixture[0]["results"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate benchmark result key"):
            validate_runs(fixture)

    def test_strict_report_rejects_sample_mismatch(self) -> None:
        fixture = strict_runs()
        fixture[0]["results"][0]["samples"] = 2
        with self.assertRaisesRegex(ValueError, "samples do not match"):
            validate_runs(fixture)


if __name__ == "__main__":
    unittest.main()
