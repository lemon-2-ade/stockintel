"""Measure inference latency: in-process model call and HTTP round trip.

    uv run python scripts/bench_inference.py --url http://localhost:8010 [--requests 2000]

Requests are built from real rows of the feature dataset (``make baselines``
writes it). Requests are sent **sequentially** from one client, so the numbers
are per-request latency on an idle service, not throughput under load (that is
Phase 12). Prints a JSON summary; nothing is written unless ``--out`` is given.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import time
import uuid
from pathlib import Path
from typing import Any

import httpx2 as httpx
import pandas as pd

from shared.features import FEATURE_NAMES, FEATURE_SET_VERSION
from stockml.data.catalog import REPO_ROOT

DATASET_GLOB = "data/features/*/*/" + FEATURE_SET_VERSION + "/dataset.parquet"


def cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine()


def percentiles(samples_s: list[float]) -> dict[str, float]:
    ms = sorted(s * 1_000 for s in samples_s)

    def pct(q: float) -> float:
        return ms[min(len(ms) - 1, round(q * (len(ms) - 1)))]

    return {
        "n": float(len(ms)),
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "max_ms": ms[-1],
        "mean_ms": statistics.fmean(ms),
    }


def instances(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "symbol": row.symbol,
            "timestamp": pd.Timestamp(str(row.timestamp)).isoformat(),
            "interval": "1d",
            "source_event_id": str(uuid.uuid4()),
            "feature_set_version": FEATURE_SET_VERSION,
            "features": {f: float(getattr(row, f)) for f in FEATURE_NAMES},
        }
        for row in frame.itertuples(index=False)
    ]


def run(url: str, frame: pd.DataFrame, requests: int, batch: int, warmup: int) -> dict[str, Any]:
    payloads = [
        {"instances": instances(frame.sample(batch, random_state=i))}
        for i in range(requests + warmup)
    ]
    latencies: list[float] = []
    server_model: list[float] = []
    with httpx.Client(base_url=url, timeout=10) as client:
        for i, payload in enumerate(payloads):
            started = time.perf_counter()
            response = client.post("/predict", json=payload)
            elapsed = time.perf_counter() - started
            response.raise_for_status()
            if i >= warmup:
                latencies.append(elapsed)
                first = response.json()["predictions"][0]
                server_model.append(first["inference_latency_ms"] / 1_000)
    return {
        "batch_size": batch,
        "http_round_trip": percentiles(latencies),
        "model_call": percentiles(server_model),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8010")
    parser.add_argument("--requests", type=int, default=2_000)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--batches", default="1,12,64")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    paths = sorted(REPO_ROOT.glob(DATASET_GLOB))
    if not paths:
        raise SystemExit("feature dataset not found: run `make baselines` first")
    frame = pd.read_parquet(paths[-1])
    with httpx.Client(base_url=args.url, timeout=10) as client:
        model = client.get("/model").raise_for_status().json()
    results = {
        "model": model,
        "machine": {
            "platform": platform.platform(),
            "cpu": cpu_model(),
            "cpu_count": os.cpu_count(),
            "python": platform.python_version(),
        },
        "mode": "sequential, single client, same host",
        "runs": [
            run(args.url, frame, args.requests, int(b), args.warmup)
            for b in args.batches.split(",")
        ],
    }
    text = json.dumps(results, indent=2)
    print(text)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n")


if __name__ == "__main__":
    main()
