from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.benchmark_report import (
    ENVIRONMENT_IDENTITY_FIELDS,
    add_comparisons,
    build_tcp_scaling_analysis,
    compare_with_baseline,
    comparison_environment_status,
    environment_identity,
    has_durable_pipeline_profile,
    load_source_contract,
    parse_environment,
    regressions_over_threshold,
    render_markdown,
    validate_runs,
    validate_source_contract,
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
        "benchmark_contract_sha256": "a" * 64,
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


def tcp_runs() -> list[dict[str, object]]:
    rates = {
        1: {1: 100.0, 8: 200.0, 32: 180.0, 128: 150.0},
        2: {1: 180.0, 8: 360.0, 32: 350.0, 128: 300.0},
        4: {1: 320.0, 8: 640.0, 32: 600.0, 128: 500.0},
    }
    return [
        {
            "source": f"server-tcp-w{workers}-p{pipeline}.txt",
            "metadata": {"pipeline": pipeline},
            "results": [
                {
                    "workers": workers,
                    "operations": 1_000,
                    "median_ops_per_second": rate,
                    "min_ops_per_second": rate * 0.9,
                    "max_ops_per_second": rate * 1.1,
                    "median_reactor_input_buffer_compactions": pipeline,
                    "maximum_reactor_input_buffer_compactions": pipeline + 1,
                    "median_reactor_input_buffer_bytes_moved": workers * pipeline * 100,
                    "maximum_reactor_input_buffer_bytes_moved": workers * pipeline * 125,
                }
            ],
        }
        for workers, pipelines in rates.items()
        for pipeline, rate in pipelines.items()
    ]


def strict_tcp_run() -> list[dict[str, object]]:
    fixture = strict_runs()
    fixture[0]["source"] = "server-tcp-w2-p32.txt"
    fixture[0]["metadata"].update(
        {
            "pipeline": 32,
            "client_mode": "raw-wire",
            "storage_mode": "volatile",
            "latency_measurement": "disabled",
        }
    )
    fixture[0]["results"][0].update(
        {"workers": 2, "threads": 2, "distribution": "owner-bound"}
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

    def test_contract_change_suppresses_all_deltas(self) -> None:
        current = environment(benchmark_contract_sha256="b" * 64)
        baseline = {"environment": environment(benchmark_contract_sha256="a" * 64)}
        current_runs = runs(90.0)
        status, matched = compare_with_baseline(current_runs, baseline, current)
        self.assertEqual(status["status"], "incompatible")
        self.assertIn("benchmark_contract_sha256", status["differences"])
        self.assertEqual(matched, 0)

    def test_sampling_change_does_not_match_result_identity(self) -> None:
        current = runs(90.0)
        prior = runs(100.0)
        current[0]["results"][0].update({"warmup": 1, "samples": 7})
        prior[0]["results"][0].update({"warmup": 1, "samples": 3})
        self.assertEqual(add_comparisons(current, {"runs": prior}), 0)
        self.assertNotIn("comparison", current[0]["results"][0])

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

    def test_strict_tcp_coordinates_match_source_name(self) -> None:
        validate_runs(strict_tcp_run())
        fixture = strict_tcp_run()
        fixture[0]["metadata"]["pipeline"] = 8
        with self.assertRaisesRegex(ValueError, "metadata.pipeline is 8, expected 32"):
            validate_runs(fixture)

    def test_source_contract_accepts_exact_suite(self) -> None:
        contract = {
            "schema_version": 2,
            "suite": "fixture",
            "expected_sources": [
                {"source": "core.txt", "benchmark_warmup": 1, "benchmark_repeats": 3}
            ],
        }
        validate_source_contract(strict_runs(), contract)

    def test_source_contract_rejects_missing_and_extra_suites(self) -> None:
        contract = {
            "schema_version": 2,
            "suite": "fixture",
            "expected_sources": [
                {"source": "core.txt", "benchmark_warmup": 1, "benchmark_repeats": 3},
                {"source": "server.txt", "benchmark_warmup": 1, "benchmark_repeats": 3},
            ],
        }
        with self.assertRaisesRegex(ValueError, "missing sources: server.txt"):
            validate_source_contract(strict_runs(), contract)

        contract["expected_sources"] = [
            {"source": "server.txt", "benchmark_warmup": 1, "benchmark_repeats": 3}
        ]
        with self.assertRaisesRegex(ValueError, "unexpected sources: core.txt"):
            validate_source_contract(strict_runs(), contract)

    def test_tracked_source_contract_is_valid_json_contract(self) -> None:
        path = (
            Path(__file__).resolve().parents[2]
            / "engineering"
            / "performance"
            / "hosted-benchmark-contract.json"
        )
        contract = load_source_contract(path)
        self.assertEqual(contract["schema_version"], 2)
        self.assertEqual(len(contract["expected_sources"]), 21)
        validate_source_contract(
            [
                {
                    "source": entry["source"],
                    "metadata": {
                        "benchmark_warmup": entry["benchmark_warmup"],
                        "benchmark_repeats": entry["benchmark_repeats"],
                    },
                }
                for entry in contract["expected_sources"]
            ],
            contract,
        )

    def test_source_contract_rejects_weakened_sampling(self) -> None:
        contract = {
            "schema_version": 2,
            "suite": "fixture",
            "expected_sources": [
                {"source": "core.txt", "benchmark_warmup": 1, "benchmark_repeats": 7}
            ],
        }
        with self.assertRaisesRegex(ValueError, "benchmark_repeats is 3, expected 7"):
            validate_source_contract(strict_runs(), contract)

    def test_tcp_scaling_analysis_reports_best_pipeline_and_efficiency(self) -> None:
        analysis = build_tcp_scaling_analysis(tcp_runs())
        self.assertIsNotNone(analysis)
        assert analysis is not None
        self.assertEqual(analysis["status"], "complete")
        best = {
            cell["workers"]: cell
            for cell in analysis["highest_observed_median_by_workers"]
        }
        self.assertEqual(best[4]["pipeline"], 8)
        self.assertAlmostEqual(best[4]["speedup_vs_one_worker"], 3.2)
        self.assertAlmostEqual(best[4]["scaling_efficiency_percent"], 80.0)
        self.assertAlmostEqual(best[4]["gain_vs_pipeline_one_percent"], 100.0)
        self.assertAlmostEqual(best[4]["median_input_buffer_bytes_moved_per_operation"], 3.2)

        markdown = render_markdown(
            tcp_runs(), "now", None, {"status": "no-baseline"}, analysis
        )
        self.assertIn("## TCP scaling summary", markdown)
        self.assertIn(
            "| 4 | 8 | 640 | 576–704 | +100.00% | 3.20× | 80.00% | 8 | 3.20 B |",
            markdown,
        )

    def test_tcp_scaling_analysis_marks_missing_cells_partial(self) -> None:
        analysis = build_tcp_scaling_analysis(tcp_runs()[:1])
        self.assertIsNotNone(analysis)
        assert analysis is not None
        self.assertEqual(analysis["status"], "partial")
        self.assertEqual(len(analysis["missing_cells"]), 11)

    def test_durable_profile_excludes_volatile_completion_counters(self) -> None:
        result = {"durable_completed": 100}
        volatile = {"metadata": {"storage_mode": "volatile"}}
        durable = {"metadata": {"storage_mode": "durable-periodic"}}
        self.assertFalse(has_durable_pipeline_profile(volatile, result))
        self.assertTrue(has_durable_pipeline_profile(durable, result))

        report_runs = [
            {"source": "volatile.txt", **volatile, "results": [dict(result)]},
            {"source": "durable.txt", **durable, "results": [dict(result)]},
        ]
        markdown = render_markdown(
            report_runs, "now", None, {"status": "no-baseline"}
        )
        durable_section = markdown.split("## Durable pipeline profile", 1)[1]
        self.assertIn("| durable |", durable_section)
        self.assertNotIn("| volatile |", durable_section)


if __name__ == "__main__":
    unittest.main()
