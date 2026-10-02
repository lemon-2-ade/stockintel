"""Score the stream processor's anomaly detectors against simulator ground truth.

    uv run python scripts/evaluate_detectors.py [--bars 20000] [--seed 7] [--interval 1s]

Runs the calibrated simulator (all symbols) bar by bar through the real
BarProcessor, compares every detection with the injected-anomaly labels and
prints precision / recall per detector, plus the processor's single-core
throughput (indicator + detector computation and event construction only, no
Kafka). Results are deterministic for a given seed.
"""

from __future__ import annotations

import argparse
import time
from collections import Counter
from datetime import UTC, datetime

from market_producer.calibration import DEFAULT_CALIBRATION_FILE, Calibration
from market_producer.providers.simulated import SimulatorProvider
from market_producer.simulator import AnomalyConfig, MarketSimulator, SymbolSimulator
from shared.schemas import BarInterval
from stream_processor.evaluation import DETECTOR_TRUTH, DetectionOutcome, score_bar
from stream_processor.processor import BarProcessor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bars", type=int, default=20_000, help="bars per symbol")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--interval", default="1s")
    args = parser.parse_args()

    interval = BarInterval(args.interval)
    calibration = Calibration.load(DEFAULT_CALIBRATION_FILE)
    simulator = MarketSimulator(
        [
            SymbolSimulator(
                symbol,
                calibration.for_symbol(symbol),
                interval=interval,
                seed=args.seed,
                anomalies=AnomalyConfig(),  # production defaults
            )
            for symbol in calibration.params
        ]
    )
    provider = SimulatorProvider(
        simulator, start=datetime(2026, 1, 5, 14, 30, tzinfo=UTC), max_bars=args.bars
    )
    processor = BarProcessor()
    outcomes: Counter[tuple[str, DetectionOutcome]] = Counter()
    injected_counts: Counter[str] = Counter()
    bars = 0
    compute = 0.0

    for batch in provider.batches():
        for produced in batch.bars:
            injected = frozenset(produced.injected)
            injected_counts.update(injected)
            started = time.perf_counter()
            result = processor.process(produced.event)
            compute += time.perf_counter() - started
            outcomes.update(score_bar(injected, result.anomalies))
            bars += 1

    print(
        f"bars={bars:,} symbols={len(simulator.symbols)} interval={interval.value} seed={args.seed}"
    )
    print("injected:", {str(k): v for k, v in sorted(injected_counts.items())})
    print()
    print("| detector | TP | FP | FN | precision | recall |")
    print("| --- | ---: | ---: | ---: | ---: | ---: |")
    for detector in DETECTOR_TRUTH:
        tp = outcomes[(detector, DetectionOutcome.TRUE_POSITIVE)]
        fp = outcomes[(detector, DetectionOutcome.FALSE_POSITIVE)]
        fn = outcomes[(detector, DetectionOutcome.FALSE_NEGATIVE)]
        precision = tp / (tp + fp) if tp + fp else float("nan")
        recall = tp / (tp + fn) if tp + fn else float("nan")
        print(f"| {detector} | {tp} | {fp} | {fn} | {precision:.3f} | {recall:.3f} |")
    print()
    print(
        f"processor compute: {compute / bars * 1e6:.1f} us/bar "
        f"(~{bars / compute:,.0f} bars/s on one core, excluding Kafka I/O)"
    )


if __name__ == "__main__":
    main()
