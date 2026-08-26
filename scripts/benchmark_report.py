#!/usr/bin/env python3
"""Convert GlyphaStore benchmark output into JSON and GitHub-flavored Markdown."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import re
import shlex
import sys
from pathlib import Path
from typing import Any


ENVIRONMENT_IDENTITY_FIELDS = (
    "runner_os",
    "runner_arch",
    "runner_image",
    "runner_image_version",
    "kernel_release",
    "cpu_model",
    "logical_cpu_count",
    "compiler_identity",
    "build_preset",
    "benchmark_contract_sha256",
)
TCP_SOURCE_PATTERN = re.compile(r"server-tcp-w(?P<workers>\d+)-p(?P<pipeline>\d+)\.txt")


def identity_value_present(environment: dict[str, Any], field: str) -> bool:
    if field not in environment or environment[field] in ("", "unknown", None):
        return False
    if field == "benchmark_contract_sha256":
        value = environment[field]
        return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None
    return True


def scalar(value: str) -> str | int | float:
    try:
        return int(value)
    except ValueError:
        try:
            number = float(value)
        except ValueError:
            return value
        return number if math.isfinite(number) else value


def fields(line: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for token in shlex.split(line):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        parsed[key] = scalar(value)
    return parsed


def parse_output(path: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    results: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("# ") and "=" in line:
            key, value = line[2:].split("=", 1)
            metadata[key] = scalar(value)
        elif line.startswith("name="):
            result = fields(line)
            if result:
                results.append(result)
    return {"source": path.name, "metadata": metadata, "results": results}


def parse_environment(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    environment: dict[str, Any] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        key = key.strip()
        if key:
            environment[key] = scalar(value.strip())
    return environment


def positive_finite(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and value > 0
        and math.isfinite(value)
    )


def positive_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def nonnegative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_runs(runs: list[dict[str, Any]]) -> None:
    if not runs:
        raise ValueError("benchmark report contains no input suites")
    required_metadata = (
        "git_sha",
        "arch",
        "platform",
        "compiler",
        "benchmark_warmup",
        "benchmark_repeats",
    )
    seen_sources: set[str] = set()
    seen_results: set[tuple[Any, ...]] = set()
    for run in runs:
        source = run.get("source")
        if not isinstance(source, str) or not source:
            raise ValueError("benchmark suite has no source name")
        if source in seen_sources:
            raise ValueError(f"duplicate benchmark source: {source}")
        seen_sources.add(source)
        metadata = run.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError(f"{source}: metadata is missing")
        missing_metadata = [
            field
            for field in required_metadata
            if field not in metadata or metadata[field] in ("", "unknown", None)
        ]
        if missing_metadata:
            raise ValueError(f"{source}: missing metadata: {', '.join(missing_metadata)}")
        if not nonnegative_integer(metadata["benchmark_warmup"]):
            raise ValueError(f"{source}: benchmark_warmup must be a non-negative integer")
        if not positive_integer(metadata["benchmark_repeats"]):
            raise ValueError(f"{source}: benchmark_repeats must be a positive integer")
        results = run.get("results")
        if not isinstance(results, list) or not results:
            raise ValueError(f"{source}: no benchmark results parsed")
        for result in results:
            if not isinstance(result, dict):
                raise ValueError(f"{source}: result is not an object")
            name = result.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(f"{source}: result has no benchmark name")
            key = result_key(source, result)
            if key in seen_results:
                raise ValueError(f"{source}: duplicate benchmark result key for {name}")
            seen_results.add(key)
            for field in ("operations", "samples"):
                if not positive_integer(result.get(field)):
                    raise ValueError(f"{source}/{name}: {field} must be a positive integer")
            if not nonnegative_integer(result.get("warmup")):
                raise ValueError(f"{source}/{name}: warmup must be a non-negative integer")
            for field in (
                "median_seconds",
                "min_seconds",
                "max_seconds",
                "median_ops_per_second",
                "min_ops_per_second",
                "max_ops_per_second",
            ):
                if not positive_finite(result.get(field)):
                    raise ValueError(f"{source}/{name}: {field} must be finite and positive")
            if result["samples"] != metadata["benchmark_repeats"]:
                raise ValueError(f"{source}/{name}: samples do not match benchmark_repeats")
            if result.get("warmup") != metadata["benchmark_warmup"]:
                raise ValueError(f"{source}/{name}: warmup does not match benchmark_warmup")
            for suffix in ("seconds", "ops_per_second"):
                if not (
                    result[f"min_{suffix}"]
                    <= result[f"median_{suffix}"]
                    <= result[f"max_{suffix}"]
                ):
                    raise ValueError(
                        f"{source}/{name}: invalid {suffix} min/median/max ordering"
                    )
        tcp_match = TCP_SOURCE_PATTERN.fullmatch(source)
        if tcp_match is not None:
            expected_workers = int(tcp_match.group("workers"))
            expected_pipeline = int(tcp_match.group("pipeline"))
            if len(results) != 1:
                raise ValueError(f"{source}: TCP matrix source must contain exactly one result")
            result = results[0]
            expected_fields = {
                "metadata.pipeline": (metadata.get("pipeline"), expected_pipeline),
                "metadata.client_mode": (metadata.get("client_mode"), "raw-wire"),
                "metadata.storage_mode": (metadata.get("storage_mode"), "volatile"),
                "metadata.latency_measurement": (
                    metadata.get("latency_measurement"),
                    "disabled",
                ),
                "result.workers": (result.get("workers"), expected_workers),
                "result.threads": (result.get("threads"), expected_workers),
                "result.distribution": (result.get("distribution"), "owner-bound"),
            }
            for field, (actual, expected) in expected_fields.items():
                if actual != expected:
                    raise ValueError(
                        f"{source}: {field} is {actual!r}, expected {expected!r}"
                    )


def load_source_contract(path: Path) -> dict[str, Any]:
    try:
        contract = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read source contract {path}: {error}") from error
    if not isinstance(contract, dict):
        raise ValueError("source contract must be a JSON object")
    return contract


def validate_source_contract(runs: list[dict[str, Any]], contract: dict[str, Any]) -> None:
    if contract.get("schema_version") != 2:
        raise ValueError("source contract schema_version must be 2")
    if not isinstance(contract.get("suite"), str) or not contract["suite"]:
        raise ValueError("source contract suite must be a non-empty string")
    expected = contract.get("expected_sources")
    if not isinstance(expected, list) or not expected:
        raise ValueError("source contract expected_sources must be a non-empty list")
    required_entry_keys = {"source", "benchmark_warmup", "benchmark_repeats"}
    source_names: list[str] = []
    for entry in expected:
        if not isinstance(entry, dict) or set(entry) != required_entry_keys:
            raise ValueError(
                "source contract entries require source, benchmark_warmup, benchmark_repeats"
            )
        source = entry["source"]
        if not isinstance(source, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]*\.txt", source):
            raise ValueError(f"source contract contains unsafe source name: {source!r}")
        if not nonnegative_integer(entry["benchmark_warmup"]):
            raise ValueError(f"source contract {source}: benchmark_warmup must be non-negative")
        if not positive_integer(entry["benchmark_repeats"]):
            raise ValueError(f"source contract {source}: benchmark_repeats must be positive")
        source_names.append(source)
    if len(source_names) != len(set(source_names)):
        raise ValueError("source contract expected_sources contains duplicates")
    actual = {str(run.get("source", "")) for run in runs}
    wanted = set(source_names)
    missing = sorted(wanted - actual)
    unexpected = sorted(actual - wanted)
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing sources: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected sources: {', '.join(unexpected)}")
        raise ValueError("source contract mismatch; " + "; ".join(details))
    runs_by_source = {str(run["source"]): run for run in runs}
    for entry in expected:
        source = entry["source"]
        metadata = runs_by_source[source].get("metadata", {})
        for field in ("benchmark_warmup", "benchmark_repeats"):
            if metadata.get(field) != entry[field]:
                raise ValueError(
                    f"source contract {source}: {field} is {metadata.get(field)!r}, "
                    f"expected {entry[field]!r}"
                )


def environment_identity(environment: dict[str, Any]) -> dict[str, Any]:
    fields = {
        field: environment[field]
        for field in ENVIRONMENT_IDENTITY_FIELDS
        if identity_value_present(environment, field)
    }
    encoded = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "fields": fields,
        "sha256": hashlib.sha256(encoded).hexdigest() if fields else None,
    }


def comparison_environment_status(
    current: dict[str, Any], baseline: dict[str, Any] | None
) -> dict[str, Any]:
    if baseline is None:
        return {"status": "no-baseline", "differences": {}}
    prior = baseline.get("environment")
    if not isinstance(prior, dict):
        return {
            "status": "incompatible",
            "reason": "baseline-environment-missing",
            "differences": {},
        }
    missing_current = [
        field for field in ENVIRONMENT_IDENTITY_FIELDS if not identity_value_present(current, field)
    ]
    missing_prior = [
        field for field in ENVIRONMENT_IDENTITY_FIELDS if not identity_value_present(prior, field)
    ]
    if missing_current or missing_prior:
        return {
            "status": "incompatible",
            "reason": "identity-fields-missing",
            "missing_current": missing_current,
            "missing_baseline": missing_prior,
            "differences": {},
        }
    differences = {
        field: {"current": current[field], "baseline": prior[field]}
        for field in ENVIRONMENT_IDENTITY_FIELDS
        if current[field] != prior[field]
    }
    return {
        "status": "compatible" if not differences else "incompatible",
        "reason": "identity-match" if not differences else "identity-mismatch",
        "differences": differences,
    }


def number(value: Any) -> float:
    return float(value) if isinstance(value, (int, float)) else 0.0


def rate(value: Any) -> str:
    amount = number(value)
    if amount >= 1_000_000_000:
        return f"{amount / 1_000_000_000:.2f} G"
    if amount >= 1_000_000:
        return f"{amount / 1_000_000:.2f} M"
    if amount >= 1_000:
        return f"{amount / 1_000:.2f} k"
    return f"{amount:.2f}"


def decimal(value: Any, suffix: str = "") -> str:
    return f"{number(value):,.2f}{suffix}"


def latency(value: Any) -> str:
    amount = number(value)
    if amount <= 0:
        return "—"
    if amount >= 1_000_000:
        return f"{amount / 1_000_000:.2f} ms"
    if amount >= 1_000:
        return f"{amount / 1_000:.2f} µs"
    return f"{amount:.2f} ns"


def mib(value: Any) -> str:
    return decimal(number(value) / (1024 * 1024), " MiB")


def escape(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def result_key(source: str, result: dict[str, Any]) -> tuple[Any, ...]:
    return (
        source,
        result.get("name"),
        result.get("key_size"),
        result.get("value_size"),
        result.get("workers"),
        result.get("threads"),
        result.get("distribution"),
        result.get("random"),
        # Ops count is part of the contract: CI may intentionally shrink a
        # distribution (e.g. single-worker) without treating it as a regression
        # against a prior larger working set.
        result.get("operations"),
        result.get("warmup"),
        result.get("samples"),
    )


def baseline_results(report: dict[str, Any]) -> dict[tuple[Any, ...], dict[str, Any]]:
    indexed: dict[tuple[Any, ...], dict[str, Any]] = {}
    for run in report.get("runs", []):
        source = run.get("source", "")
        for result in run.get("results", []):
            indexed[result_key(source, result)] = result
    return indexed


def classify_rate_ranges(current: dict[str, Any], prior: dict[str, Any]) -> str:
    current_min = number(current.get("min_ops_per_second", 0))
    current_max = number(current.get("max_ops_per_second", 0))
    prior_min = number(prior.get("min_ops_per_second", 0))
    prior_max = number(prior.get("max_ops_per_second", 0))
    if (
        min(current_min, current_max, prior_min, prior_max) <= 0
        or current_min > current_max
        or prior_min > prior_max
    ):
        return "median-only"
    if max(current_min, prior_min) <= min(current_max, prior_max):
        return "inconclusive-overlap"
    if current_max < prior_min:
        return "regression-candidate"
    if current_min > prior_max:
        return "improvement-candidate"
    return "inconclusive-invalid-ranges"


def add_comparisons(runs: list[dict[str, Any]], baseline: dict[str, Any] | None) -> int:
    if baseline is None:
        return 0
    previous = baseline_results(baseline)
    matched = 0
    for run in runs:
        for result in run["results"]:
            prior = previous.get(result_key(run["source"], result))
            prior_rate = number(prior.get("median_ops_per_second", 0)) if prior else 0.0
            current_rate = number(result.get("median_ops_per_second", 0))
            if prior_rate <= 0:
                continue
            result["comparison"] = {
                "baseline_median_ops_per_second": prior_rate,
                "baseline_min_ops_per_second": number(prior.get("min_ops_per_second", 0)),
                "baseline_max_ops_per_second": number(prior.get("max_ops_per_second", 0)),
                "median_ops_per_second_delta_percent": (current_rate - prior_rate) / prior_rate * 100,
                "interpretation": classify_rate_ranges(result, prior),
            }
            matched += 1
    return matched


def compare_with_baseline(
    runs: list[dict[str, Any]], baseline: dict[str, Any] | None, environment: dict[str, Any]
) -> tuple[dict[str, Any], int]:
    status = comparison_environment_status(environment, baseline)
    matched = add_comparisons(runs, baseline) if status["status"] == "compatible" else 0
    return status, matched


def delta(result: dict[str, Any]) -> str:
    comparison = result.get("comparison")
    if not isinstance(comparison, dict):
        return "—"
    change = number(comparison.get("median_ops_per_second_delta_percent", 0))
    return f"{change:+.2f}%"


def comparison_signal(result: dict[str, Any]) -> str:
    comparison = result.get("comparison")
    if not isinstance(comparison, dict):
        return "—"
    labels = {
        "inconclusive-overlap": "inconclusive (ranges overlap)",
        "regression-candidate": "regression candidate",
        "improvement-candidate": "improvement candidate",
        "median-only": "median only",
        "inconclusive-invalid-ranges": "inconclusive (invalid ranges)",
    }
    interpretation = str(comparison.get("interpretation", "median-only"))
    return labels.get(interpretation, interpretation)


def regressions_over_threshold(runs: list[dict[str, Any]], threshold: float) -> list[str]:
    regressions: list[str] = []
    for run in runs:
        suite = Path(run["source"]).stem
        for result in run["results"]:
            comparison = result.get("comparison")
            if not isinstance(comparison, dict):
                continue
            if comparison.get("interpretation") != "regression-candidate":
                continue
            delta_percent = number(comparison.get("median_ops_per_second_delta_percent", 0))
            if delta_percent < -threshold:
                regressions.append(
                    f"{suite}/{result.get('name', 'unknown')}: {delta_percent:+.2f}% "
                    f"(threshold -{threshold:.2f}%)"
                )
    return regressions


def build_tcp_scaling_analysis(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    cells: list[dict[str, Any]] = []
    for run in runs:
        match = TCP_SOURCE_PATTERN.fullmatch(str(run.get("source", "")))
        results = run.get("results", [])
        if match is None or not isinstance(results, list) or len(results) != 1:
            continue
        result = results[0]
        rate = number(result.get("median_ops_per_second", 0))
        if rate <= 0:
            continue
        cells.append(
            {
                "source": run["source"],
                "workers": int(match.group("workers")),
                "pipeline": int(match.group("pipeline")),
                "median_ops_per_second": rate,
                "min_ops_per_second": number(result.get("min_ops_per_second", 0)),
                "max_ops_per_second": number(result.get("max_ops_per_second", 0)),
            }
        )
        cell = cells[-1]
        if "median_reactor_input_buffer_compactions" in result:
            cell["median_input_buffer_compactions"] = number(
                result.get("median_reactor_input_buffer_compactions", 0)
            )
            cell["maximum_input_buffer_compactions"] = number(
                result.get("maximum_reactor_input_buffer_compactions", 0)
            )
            cell["median_input_buffer_bytes_moved"] = number(
                result.get("median_reactor_input_buffer_bytes_moved", 0)
            )
            cell["maximum_input_buffer_bytes_moved"] = number(
                result.get("maximum_reactor_input_buffer_bytes_moved", 0)
            )
            operations = number(result.get("operations", 0))
            if operations > 0:
                cell["median_input_buffer_bytes_moved_per_operation"] = (
                    cell["median_input_buffer_bytes_moved"] / operations
                )
    if not cells:
        return None
    cells.sort(key=lambda cell: (cell["workers"], cell["pipeline"]))
    by_coordinate = {(cell["workers"], cell["pipeline"]): cell for cell in cells}
    expected_coordinates = {
        (workers, pipeline)
        for workers in (1, 2, 4)
        for pipeline in (1, 8, 32, 128)
    }
    missing = sorted(expected_coordinates - set(by_coordinate))
    for cell in cells:
        one_worker = by_coordinate.get((1, cell["pipeline"]))
        worker_depth_one = by_coordinate.get((cell["workers"], 1))
        if one_worker is not None:
            speedup = cell["median_ops_per_second"] / one_worker["median_ops_per_second"]
            cell["speedup_vs_one_worker"] = speedup
            cell["scaling_efficiency_percent"] = speedup / cell["workers"] * 100
        if worker_depth_one is not None:
            cell["gain_vs_pipeline_one_percent"] = (
                cell["median_ops_per_second"] / worker_depth_one["median_ops_per_second"] - 1
            ) * 100
    best_by_workers = []
    for workers in sorted({cell["workers"] for cell in cells}):
        candidates = [cell for cell in cells if cell["workers"] == workers]
        best_by_workers.append(max(candidates, key=lambda cell: cell["median_ops_per_second"]))
    return {
        "status": "complete" if not missing else "partial",
        "missing_cells": [
            {"workers": workers, "pipeline": pipeline} for workers, pipeline in missing
        ],
        "cells": cells,
        "highest_observed_median_by_workers": best_by_workers,
    }


def has_durable_pipeline_profile(run: dict[str, Any], result: dict[str, Any]) -> bool:
    metadata = run.get("metadata", {})
    storage_mode = metadata.get("storage_mode") if isinstance(metadata, dict) else None
    return (
        isinstance(storage_mode, str)
        and storage_mode.startswith("durable-")
        and number(result.get("durable_completed", 0)) > 0
    )


def render_markdown(
    runs: list[dict[str, Any]],
    generated_at: str,
    baseline_generated_at: str | None,
    comparison_environment: dict[str, Any],
    tcp_scaling: dict[str, Any] | None = None,
) -> str:
    result_count = sum(len(run["results"]) for run in runs)
    lines = [
        "# GlyphaStore benchmark report",
        "",
        f"Generated at `{generated_at}` from {result_count} result(s).",
        "",
        "> Results from GitHub-hosted runners are suitable for observing large regressions, "
        "not for absolute performance claims. Runner contention and hardware can vary.",
        "",
    ]
    if baseline_generated_at:
        lines.extend([f"Baseline report: `{baseline_generated_at}`.", ""])
    status = comparison_environment["status"]
    if status == "compatible":
        lines.extend(["Environment identity: **compatible**; throughput deltas are shown.", ""])
    elif status == "no-baseline":
        lines.extend(["No retained baseline is available; throughput deltas are not shown.", ""])
    elif status == "incompatible":
        reason = comparison_environment.get("reason", "unknown")
        lines.extend(
            [
                f"Environment identity: **incompatible** (`{reason}`); throughput deltas "
                "are suppressed.",
                "",
            ]
        )
        differences = comparison_environment.get("differences", {})
        if isinstance(differences, dict) and differences:
            lines.extend(
                [
                    "| Environment field | Current | Baseline |",
                    "| --- | --- | --- |",
                ]
            )
            for field, values in differences.items():
                lines.append(
                    f"| {escape(field)} | {escape(values['current'])} | "
                    f"{escape(values['baseline'])} |"
                )
            lines.append("")
        missing_current = comparison_environment.get("missing_current", [])
        missing_baseline = comparison_environment.get("missing_baseline", [])
        if missing_current or missing_baseline:
            lines.extend(
                [
                    f"Missing current identity fields: `{', '.join(missing_current) or 'none'}`.",
                    f"Missing baseline identity fields: `{', '.join(missing_baseline) or 'none'}`.",
                    "",
                ]
            )
    if result_count == 0:
        lines.extend(["No benchmark results were produced.", ""])
        return "\n".join(lines)

    lines.extend(
        [
            "| Suite | Benchmark | Configuration | Median ops/s | Δ ops/s | Interpretation | Median ns/op | p50 | p95 | p99 | p99.9 | RSS | Duplex |",
            "| --- | --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for run in runs:
        suite = Path(run["source"]).stem
        for result in run["results"]:
            config = (
                f"k={result.get('key_size', '?')}, v={result.get('value_size', '?')}, "
                f"w={result.get('workers', '?')}, t={result.get('threads', '?')}, "
                f"{result.get('distribution', '?')}"
            )
            pipeline = run["metadata"].get("pipeline")
            if pipeline is not None:
                config += f", p={pipeline}"
            lines.append(
                "| "
                + " | ".join(
                    [
                        escape(suite),
                        escape(result.get("name", "unknown")),
                        escape(config),
                        rate(result.get("median_ops_per_second", 0)),
                        delta(result),
                        comparison_signal(result),
                        decimal(result.get("median_ns_per_op", 0)),
                        latency(result.get("p50_latency_ns", 0)),
                        latency(result.get("p95_latency_ns", 0)),
                        latency(result.get("p99_latency_ns", 0)),
                        latency(result.get("p999_latency_ns", 0)),
                        mib(result.get("median_rss_bytes", 0)),
                        rate(result.get("median_duplex_bytes_per_second", 0)),
                    ]
                )
                + " |"
            )

    lines.extend(["", "## Run metadata", ""])
    lines.extend(
        [
            "| Suite | Commit | Platform | Architecture | Compiler |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for run in runs:
        metadata = run["metadata"]
        lines.append(
            "| "
            + " | ".join(
                [
                    escape(Path(run["source"]).stem),
                    escape(metadata.get("git_sha", "unknown")),
                    escape(metadata.get("platform", "unknown")),
                    escape(metadata.get("arch", "unknown")),
                    escape(metadata.get("compiler", "unknown")),
                ]
            )
            + " |"
        )
    lines.append("")

    if tcp_scaling is not None:
        lines.extend(
            [
                "## TCP scaling summary",
                "",
                f"Matrix status: **{tcp_scaling['status']}**. Highest observed median per Worker "
                "count is descriptive; overlapping ranges remain inconclusive.",
                "",
                "| Workers | Pipeline | Median ops/s | Observed min–max | Gain vs p1 | Speedup vs W1 | Efficiency | Input compactions | Input bytes copied/op |",
                "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for cell in tcp_scaling["highest_observed_median_by_workers"]:
            gain = cell.get("gain_vs_pipeline_one_percent")
            speedup = cell.get("speedup_vs_one_worker")
            efficiency = cell.get("scaling_efficiency_percent")
            gain_text = f"{gain:+.2f}%" if isinstance(gain, (int, float)) else "—"
            speedup_text = f"{speedup:.2f}×" if isinstance(speedup, (int, float)) else "—"
            efficiency_text = (
                f"{efficiency:.2f}%" if isinstance(efficiency, (int, float)) else "—"
            )
            compactions = cell.get("median_input_buffer_compactions")
            copied_per_operation = cell.get("median_input_buffer_bytes_moved_per_operation")
            compactions_text = (
                f"{compactions:,.0f}" if isinstance(compactions, (int, float)) else "—"
            )
            copied_text = (
                f"{copied_per_operation:,.2f} B"
                if isinstance(copied_per_operation, (int, float))
                else "—"
            )
            lines.append(
                f"| {cell['workers']} | {cell['pipeline']} | "
                f"{cell['median_ops_per_second']:,.0f} | "
                f"{cell['min_ops_per_second']:,.0f}–{cell['max_ops_per_second']:,.0f} | "
                f"{gain_text} | {speedup_text} | {efficiency_text} | "
                f"{compactions_text} | {copied_text} |"
            )
        lines.extend(
            [
                "",
                "Efficiency is the observed throughput speedup divided by Worker count; it is not "
                "CPU utilization or a production-capacity claim. Input-copy columns expose the "
                "sliding Reactor buffer's copy pressure for the selected sample.",
                "",
            ]
        )

    durable_results = [
        (Path(run["source"]).stem, result)
        for run in runs
        for result in run["results"]
        if has_durable_pipeline_profile(run, result)
    ]
    if durable_results:
        lines.extend(
            [
                "## Durable pipeline profile",
                "",
                "Queue and service values are per-sample averages; maxima are the worst observed "
                "operation across all measured samples. Commit timing is the v1 batch publication "
                "boundary and is not available for unbatched durable-sync.",
                "",
                "| Suite | Queue avg/max | Queue peak | Store avg/max | Commit avg/max | Batch avg/max | Pending | Closes r/b/a/d | Rejected/expired/failed |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for suite, result in durable_results:
            lines.append(
                "| "
                + " | ".join(
                    [
                        escape(suite),
                        f"{latency(result.get('median_durable_queue_wait_ns', 0))} / "
                        f"{latency(result.get('maximum_durable_queue_wait_ns', 0))}",
                        f"{escape(result.get('durable_maximum_queue_depth', 0))} rec / "
                        f"{escape(result.get('durable_maximum_queued_bytes', 0))} B",
                        f"{latency(result.get('median_durable_service_ns', 0))} / "
                        f"{latency(result.get('maximum_durable_service_ns', 0))}",
                        f"{latency(result.get('median_durable_commit_ns', 0))} / "
                        f"{latency(result.get('maximum_durable_commit_ns', 0))}",
                        f"{decimal(result.get('median_durable_batch_records', 0))} / "
                        f"{decimal(result.get('maximum_durable_batch_records', 0))}",
                        f"{escape(result.get('durable_pending_records', 0))} rec / "
                        f"{escape(result.get('durable_pending_bytes', 0))} B",
                        f"{escape(result.get('durable_record_limit_closes', 0))}/"
                        f"{escape(result.get('durable_byte_limit_closes', 0))}/"
                        f"{escape(result.get('durable_adaptive_target_closes', 0))}/"
                        f"{escape(result.get('durable_deadline_closes', 0))}",
                        f"{escape(result.get('durable_rejected', 0))}/"
                        f"{escape(result.get('durable_expired', 0))}/"
                        f"{escape(result.get('durable_failed_batches', 0))}",
                    ]
                )
                + " |"
            )
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--json", required=True, type=Path, dest="json_path")
    parser.add_argument("--markdown", required=True, type=Path, dest="markdown_path")
    parser.add_argument("--baseline", type=Path)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on missing metadata, empty suites, duplicates, or invalid result statistics.",
    )
    parser.add_argument(
        "--source-contract",
        type=Path,
        help="JSON contract naming the exact source files required by a retained suite.",
    )
    parser.add_argument(
        "--environment",
        type=Path,
        help="Machine-readable key=value environment record used to authorize baseline deltas.",
    )
    parser.add_argument(
        "--fail-regression-threshold",
        type=float,
        help=(
            "Exit with status 1 when median ops/s regresses more than this percent versus an "
            "environment-compatible baseline."
        ),
    )
    args = parser.parse_args()
    if args.fail_regression_threshold is not None and args.fail_regression_threshold < 0:
        parser.error("fail-regression-threshold must be non-negative")
    if args.source_contract is not None and not args.strict:
        parser.error("source-contract requires strict mode")

    inputs = sorted(path for path in args.inputs if path.name != "environment.txt")
    runs = [parse_output(path) for path in inputs if path.is_file()]
    if args.strict:
        try:
            validate_runs(runs)
            if args.source_contract is not None:
                validate_source_contract(runs, load_source_contract(args.source_contract))
        except ValueError as error:
            parser.error(str(error))
    baseline = None
    if args.baseline and args.baseline.is_file():
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    environment = parse_environment(args.environment)
    comparison_environment, matched_results = compare_with_baseline(runs, baseline, environment)
    tcp_scaling = build_tcp_scaling_analysis(runs)
    generated_at = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    baseline_generated_at = baseline.get("generated_at") if baseline else None
    report = {
        "schema_version": 4,
        "generated_at": generated_at,
        "baseline_generated_at": baseline_generated_at,
        "environment": environment,
        "environment_identity": environment_identity(environment),
        "baseline_comparison": comparison_environment,
        "matched_baseline_results": matched_results,
        "analyses": {"tcp_scaling": tcp_scaling},
        "runs": runs,
    }

    args.json_path.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_path.parent.mkdir(parents=True, exist_ok=True)
    args.json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.markdown_path.write_text(
        render_markdown(
            runs, generated_at, baseline_generated_at, comparison_environment, tcp_scaling
        ),
        encoding="utf-8",
    )

    if args.fail_regression_threshold is not None:
        threshold = args.fail_regression_threshold
        regressions = regressions_over_threshold(runs, threshold)
        if regressions:
            print("Benchmark regression gate failed:", file=sys.stderr)
            for entry in regressions:
                print(f"  - {entry}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
