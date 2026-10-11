"""Feed the API and dashboard with live simulated data, without Kafka.

    uv run python scripts/demo_feed.py [--backfill 900] [--interval 1s]

For local UI work and screenshots when the Kafka stack is not running. Each
bar goes through the production code of every stage, called directly:

    simulator -> BarProcessor (indicators, anomalies) -> FeatureStore
    -> inference /predict (optional) -> PostgresSink + RedisCacheSink

The Redis sink publishes on the live channels, so the API's WebSocket pushes
updates to the browser exactly as in the full stack. ``--backfill`` bars are
written at once (so charts have history), then one bar per interval in real
time until interrupted. ``--monitor-every`` runs a model-monitor cycle
periodically (needs MLflow for reference profiles).

This replaces Kafka's delivery guarantees with direct calls: use it to look
at the product, not to test the pipeline.
"""

from __future__ import annotations

import argparse
import signal
import threading
import time
from datetime import UTC, datetime

import redis

from market_producer.calibration import DEFAULT_CALIBRATION_FILE, Calibration
from market_producer.simulator import AnomalyConfig, MarketSimulator, SymbolSimulator
from model_monitor.main import MonitorMetrics, MonitorSettings, run_cycle
from model_monitor.store import MlflowProfileSource
from prediction_pipeline.client import InferenceClient, InferenceUnavailableError
from prediction_pipeline.features import FeatureStore
from shared.config import PostgresSettings, RedisSettings
from shared.db.session import create_db_engine
from shared.features import FEATURE_SET_VERSION
from shared.schemas import BarInterval, BaseEvent
from shared.schemas.inference import MAX_BATCH, PredictInstance
from sinks.postgres_sink import PostgresSink
from sinks.redis_sink import RedisCacheSink
from stream_processor.processor import BarProcessor


class DemoFeed:
    def __init__(self, args: argparse.Namespace) -> None:
        self.interval = BarInterval(args.interval)
        calibration = Calibration.load(DEFAULT_CALIBRATION_FILE)
        anomalies = AnomalyConfig(
            price_jump_probability=args.anomaly_rate, volume_spike_probability=args.anomaly_rate
        )
        self.simulator = MarketSimulator(
            [
                SymbolSimulator(
                    symbol,
                    calibration.for_symbol(symbol),
                    interval=self.interval,
                    seed=args.seed,
                    anomalies=anomalies,
                )
                for symbol in calibration.params
            ]
        )
        self.processor, self.features = BarProcessor(), FeatureStore()
        self.client = (
            None if args.no_inference else InferenceClient(args.inference_url, timeout_s=5)
        )
        self.engine = create_db_engine(PostgresSettings(), application_name="demo-feed")
        self.postgres = PostgresSink(self.engine)
        self.cache = RedisCacheSink(redis.Redis(**RedisSettings().client_kwargs()))  # type: ignore[arg-type]

    def emit(self, timestamps: list[datetime]) -> None:
        events: list[BaseEvent] = []
        pending: list[tuple[PredictInstance, dict[str, float]]] = []
        for ts in timestamps:
            for produced in self.simulator.step(ts):
                bar = produced.event
                events.append(bar)
                result = self.processor.process(bar)
                if result.enriched is None:
                    continue
                events.append(result.enriched)
                events.extend(result.anomalies)
                update = self.features.update(result.enriched)
                if update.features is not None and self.client is not None:
                    instance = PredictInstance(
                        symbol=bar.symbol,
                        timestamp=bar.timestamp,
                        interval=bar.interval,
                        source_event_id=bar.event_id,
                        feature_set_version=FEATURE_SET_VERSION,
                        features=update.features,
                    )
                    pending.append((instance, update.features))
        events += self.predict(pending)
        self.postgres.write(events)
        self.cache.write(events)

    def predict(self, pending: list[tuple[PredictInstance, dict[str, float]]]) -> list[BaseEvent]:
        if self.client is None:
            return []
        out: list[BaseEvent] = []
        for start in range(0, len(pending), MAX_BATCH):
            chunk = pending[start : start + MAX_BATCH]
            try:
                response = self.client.predict([i for i, _ in chunk])
            except InferenceUnavailableError:
                continue  # same policy as the pipeline: no prediction, data moves on
            out += [
                e.model_copy(update={"features": f})
                for (_, f), e in zip(chunk, response.predictions, strict=True)
            ]
        return out

    def monitor(self, now: datetime) -> None:
        run_cycle(
            self.engine,
            MlflowProfileSource(),
            MonitorSettings(window_hours=1, min_samples=200, min_outcomes=50),
            MonitorMetrics(),
            now=now,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", default="1s")
    parser.add_argument("--backfill", type=int, default=900, help="bars per symbol written at once")
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("--inference-url", default="http://localhost:8010")
    parser.add_argument("--no-inference", action="store_true")
    parser.add_argument("--monitor-every", type=float, default=0, help="seconds; 0 = never")
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.001,
        help="per-bar probability of injected price jumps and volume spikes (simulator: 0.002)",
    )
    args = parser.parse_args()
    feed = DemoFeed(args)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    step = feed.interval.duration
    now = datetime.now(UTC).replace(microsecond=0)
    backfill = [now - (args.backfill - k) * step for k in range(args.backfill)]
    for start in range(0, len(backfill), 60):
        feed.emit(backfill[start : start + 60])
    print(f"backfilled {args.backfill} bars x {len(feed.simulator.symbols)} symbols")
    t = now

    last_monitor = 0.0
    while not stop.is_set():
        delay = (t + step - datetime.now(UTC)).total_seconds()  # bar closes at t + step
        if delay > 0 and stop.wait(delay):
            break
        feed.emit([t])
        t += step
        if args.monitor_every and time.monotonic() - last_monitor > args.monitor_every:
            last_monitor = time.monotonic()
            feed.monitor(t)
    if feed.client is not None:
        feed.client.close()


if __name__ == "__main__":
    main()
