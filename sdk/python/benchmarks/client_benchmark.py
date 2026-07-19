#!/usr/bin/env python3
"""Reproducible same-process-external-server benchmark for the Python SDK."""

from __future__ import annotations

import argparse
import statistics
import sys
import threading
from pathlib import Path
from time import perf_counter

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "src"))

from glyphastore import (  # noqa: E402
    Client,
    ClientConfig,
    PipelineOpcode,
    PipelineRequest,
)
from glyphastore.protocol import worker_for  # noqa: E402


def material(operations: int, workers: int, pipeline: int) -> list[list[list[PipelineRequest]]]:
    quotas = [operations // workers + (1 if worker < operations % workers else 0) for worker in range(workers)]
    requests: list[list[PipelineRequest]] = [[] for _ in range(workers)]
    candidate = 0
    while any(quotas):
        key = f"python-bench-{candidate:012d}".encode()
        owner = worker_for(key, workers)
        if quotas[owner]:
            value = bytes([candidate & 0xFF]) * 64
            requests[owner].append(PipelineRequest(PipelineOpcode.PUT, key, value))
            requests[owner].append(PipelineRequest(PipelineOpcode.GET, key))
            quotas[owner] -= 1
        candidate += 1
    batch_frames = pipeline * 2
    return [
        [worker_requests[offset : offset + batch_frames] for offset in range(0, len(worker_requests), batch_frames)]
        for worker_requests in requests
    ]


def run_pipeline(client: Client, batches: list[list[list[PipelineRequest]]]) -> float:
    barrier = threading.Barrier(len(batches) + 1)
    failures: list[str] = []

    def run(worker_batches: list[list[PipelineRequest]]) -> None:
        barrier.wait()
        for batch in worker_batches:
            responses = client.execute_pipeline(batch)
            if len(responses) != len(batch) or not all(response.succeeded for response in responses):
                failures.append("pipeline request failed")
                return
            for index in range(1, len(batch), 2):
                if responses[index].value != bytes(batch[index - 1].value):
                    failures.append("pipeline GET value mismatch")
                    return

    threads = [threading.Thread(target=run, args=(worker_batches,)) for worker_batches in batches]
    for thread in threads:
        thread.start()
    barrier.wait()
    started = perf_counter()
    for thread in threads:
        thread.join()
    elapsed = perf_counter() - started
    if failures:
        raise RuntimeError(failures[0])
    return elapsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--ops", type=int, default=200_000, help="PUT/GET pair count")
    parser.add_argument("--pipeline", type=int, default=64, help="PUT/GET pairs per batch")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=7)
    args = parser.parse_args()
    if min(args.workers, args.ops, args.pipeline, args.repeats) <= 0 or args.warmup < 0:
        parser.error("numeric options must be positive (warmup may be zero)")

    batches = material(args.ops, args.workers, args.pipeline)
    with Client.connect(ClientConfig(host=args.host, port=args.port)) as client:
        if client.worker_count != args.workers:
            parser.error("server Worker count does not match --workers")
        for _ in range(args.warmup):
            run_pipeline(client, batches)
        samples = [run_pipeline(client, batches) for _ in range(args.repeats)]

    operation_count = args.ops * 2
    rates = [operation_count / sample for sample in samples]
    print("# glyphastore Python client benchmark")
    print(f"# workers={args.workers} pipeline_pairs={args.pipeline} operations={operation_count}")
    print(
        "name=python_client_pipeline_read_after_write "
        f"samples={len(samples)} median_seconds={statistics.median(samples):.9f} "
        f"min_seconds={min(samples):.9f} max_seconds={max(samples):.9f} "
        f"median_ops_per_second={statistics.median(rates):.3f} "
        f"min_ops_per_second={min(rates):.3f} max_ops_per_second={max(rates):.3f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
