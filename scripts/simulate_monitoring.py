"""Run the prediction + monitoring path end to end on simulated bars, without Kafka.

    uv run python scripts/simulate_monitoring.py --interval 1d --bars 400
    uv run python scripts/simulate_monitoring.py --interval 1m --bars 400

Needs PostgreSQL (migrated), a running inference service with a ``@champion``
and the MLflow registry it uses (for the reference profile). Steps, using the
production code at each stage:

    calibrated simulator -> BarProcessor (indicators) -> FeatureStore
    -> InferenceClient (POST /predict) -> PostgresSink (bars + predictions)
    -> model monitor cycle (outcomes, drift, performance) -> printed summary

Kafka is replaced by direct calls, so this checks the data path and the
monitor's verdicts, not delivery semantics. Rows are written under fresh
symbol names (prefix ``--prefix``) so repeated runs do not mix.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime, timedelta

from market_producer.calibration import DEFAULT_CALIBRATION_FILE, Calibration
from market_producer.providers.simulated import SimulatorProvider
from market_producer.simulator import AnomalyConfig, MarketSimulator, SymbolSimulator
from model_monitor.main import MonitorMetrics, MonitorSettings, run_cycle
from model_monitor.store import MlflowProfileSource
from prediction_pipeline.client import InferenceClient
from prediction_pipeline.features import FeatureStore
from shared.config import PostgresSettings
from shared.db.session import create_db_engine
from shared.features import FEATURE_SET_VERSION
from shared.schemas import BarInterval, BaseEvent, MarketBarEvent
from shared.schemas.inference import MAX_BATCH, PredictInstance
from sinks.postgres_sink import PostgresSink
from stream_processor.processor import BarProcessor

START = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def simulate(interval: BarInterval, bars: int, seed: int, prefix: str) -> list[MarketBarEvent]:
    calibration = Calibration.load(DEFAULT_CALIBRATION_FILE)
    simulator = MarketSimulator(
        [
            SymbolSimulator(
                symbol,
                calibration.for_symbol(symbol),
                interval=interval,
                seed=seed,
                anomalies=AnomalyConfig(),
            )
            for symbol in calibration.params
        ]
    )
    provider = SimulatorProvider(simulator, start=START, max_bars=bars)
    events = []
    for batch in provider.batches():
        for produced in batch.bars:
            event = produced.event
            events.append(event.model_copy(update={"symbol": f"{prefix}{event.symbol}"[:15]}))
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--bars", type=int, default=400, help="bars per symbol")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--prefix", default="SIM")
    parser.add_argument("--inference-url", default="http://localhost:8010")
    args = parser.parse_args()
    interval = BarInterval(args.interval)

    raw = simulate(interval, args.bars, args.seed, args.prefix)
    processor, store = BarProcessor(), FeatureStore()
    client = InferenceClient(args.inference_url, timeout_s=10)
    events: list[BaseEvent] = []
    pending: list[tuple[PredictInstance, dict[str, float]]] = []
    for bar in raw:
        events.append(bar)
        result = processor.process(bar)
        if result.enriched is None:
            continue
        update = store.update(result.enriched)
        if update.features is not None:
            instance = PredictInstance(
                symbol=bar.symbol,
                timestamp=bar.timestamp,
                interval=bar.interval,
                source_event_id=bar.event_id,
                feature_set_version=FEATURE_SET_VERSION,
                features=update.features,
            )
            pending.append((instance, update.features))
    for start in range(0, len(pending), MAX_BATCH):
        chunk = pending[start : start + MAX_BATCH]
        response = client.predict([i for i, _ in chunk])
        events += [
            e.model_copy(update={"features": f})
            for (_, f), e in zip(chunk, response.predictions, strict=True)
        ]
    client.close()

    engine = create_db_engine(PostgresSettings(), application_name="simulate-monitoring")
    PostgresSink(engine).write(events)
    now = raw[-1].timestamp + interval.duration
    span = (now - START) / timedelta(hours=1) + 1
    reports = run_cycle(
        engine,
        MlflowProfileSource(),
        MonitorSettings(window_hours=span, outcome_lookback_hours=span),
        MonitorMetrics(),
        now=now,
    )
    statuses = Counter((r.report_type, r.status) for r in reports)
    summary = {
        "interval": interval.value,
        "bars": len(raw),
        "predictions": len(pending),
        "status_counts": {f"{k[0]}:{k[1]}": v for k, v in sorted(statuses.items())},
        "reports": [
            {
                "type": r.report_type,
                "metric": r.metric_name,
                "feature": r.feature_name,
                "value": round(r.value, 4),
                "status": r.status,
                "details": {
                    k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.details.items()
                },
            }
            for r in reports
        ],
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
