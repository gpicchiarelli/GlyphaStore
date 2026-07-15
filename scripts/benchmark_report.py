#!/usr/bin/env python3
"""Convert GlyphaStore benchmark output into JSON and GitHub-flavored Markdown."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import shlex
import sys
from pathlib import Path
from typing import Any


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
    )


def baseline_results(report: dict[str, Any]) -> dict[tuple[Any, ...], dict[str, Any]]:
    indexed: dict[tuple[Any, ...], dict[str, Any]] = {}
    for run in report.get("runs", []):
        source = run.get("source", "")
        for result in run.get("results", []):
            indexed[result_key(source, result)] = result
    return indexed


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
                "median_ops_per_second_delta_percent": (current_rate - prior_rate) / prior_rate * 100,
            }
            matched += 1
    return matched


def delta(result: dict[str, Any]) -> str:
    comparison = result.get("comparison")
    if not isinstance(comparison, dict):
        return "—"
    change = number(comparison.get("median_ops_per_second_delta_percent", 0))
    return f"{change:+.2f}%"


def render_markdown(
    runs: list[dict[str, Any]], generated_at: str, baseline_generated_at: str | None
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
    if result_count == 0:
        lines.extend(["No benchmark results were produced.", ""])
        return "\n".join(lines)

    lines.extend(
        [
            "| Suite | Benchmark | Configuration | Median ops/s | Δ ops/s | Median ns/op | p50 | p95 | p99 | p99.9 | RSS | Duplex |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
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
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--json", required=True, type=Path, dest="json_path")
    parser.add_argument("--markdown", required=True, type=Path, dest="markdown_path")
    parser.add_argument("--baseline", type=Path)
    parser.add_argument(
        "--fail-regression-threshold",
        type=float,
        help="Exit with status 1 when median ops/s regresses more than this percent versus baseline.",
    )
    args = parser.parse_args()

    inputs = sorted(path for path in args.inputs if path.name != "environment.txt")
    runs = [parse_output(path) for path in inputs if path.is_file()]
    baseline = None
    if args.baseline and args.baseline.is_file():
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    matched_results = add_comparisons(runs, baseline)
    generated_at = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    baseline_generated_at = baseline.get("generated_at") if baseline else None
    report = {
        "schema_version": 2,
        "generated_at": generated_at,
        "baseline_generated_at": baseline_generated_at,
        "matched_baseline_results": matched_results,
        "runs": runs,
    }

    args.json_path.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_path.parent.mkdir(parents=True, exist_ok=True)
    args.json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.markdown_path.write_text(
        render_markdown(runs, generated_at, baseline_generated_at), encoding="utf-8"
    )

    if args.fail_regression_threshold is not None:
        threshold = args.fail_regression_threshold
        regressions: list[str] = []
        for run in runs:
            suite = Path(run["source"]).stem
            for result in run["results"]:
                comparison = result.get("comparison")
                if not isinstance(comparison, dict):
                    continue
                delta_percent = number(comparison.get("median_ops_per_second_delta_percent", 0))
                if delta_percent < -threshold:
                    regressions.append(
                        f"{suite}/{result.get('name', 'unknown')}: {delta_percent:+.2f}% "
                        f"(threshold -{threshold:.2f}%)"
                    )
        if regressions:
            print("Benchmark regression gate failed:", file=sys.stderr)
            for entry in regressions:
                print(f"  - {entry}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
