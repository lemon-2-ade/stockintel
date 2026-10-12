from __future__ import annotations

import importlib.util
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("bench", ROOT / "scripts" / "bench_pipeline.py")
assert spec is not None
assert spec.loader is not None
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def test_percentile_is_nearest_rank() -> None:
    values = [float(i) for i in range(1, 101)]
    assert bench.pct(values, 0.5) == 50.0
    assert bench.pct(values, 0.99) == 99.0
    assert bench.pct(values, 1.0) == 100.0
    assert math.isnan(bench.pct([], 0.5))


def test_generated_bars_are_valid_unique_and_ordered_per_symbol() -> None:
    symbols = ["A1", "B2", "C3"]
    bars = bench.make_bars(symbols, 30, seed=1)
    assert len({b.event_id for b in bars}) == 30
    for s in symbols:
        times = [b.timestamp for b in bars if b.symbol == s]
        assert times == sorted(times)
        assert len(set(times)) == len(times)


def test_bars_are_deterministic_for_a_seed() -> None:
    a = [b.close for b in bench.make_bars(["A1"], 10, seed=5)]
    b = [x.close for x in bench.make_bars(["A1"], 10, seed=5)]
    assert a == b
