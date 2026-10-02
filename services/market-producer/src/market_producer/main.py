"""Entry point: ``python -m market_producer.main`` (or ``market-producer``)."""

from __future__ import annotations

import signal
import sys
import threading
import time
from datetime import UTC, datetime
from types import FrameType

from prometheus_client import start_http_server

from market_producer.calibration import Calibration
from market_producer.config import ProducerSettings
from market_producer.metrics import ProducerMetrics
from market_producer.providers.base import MarketDataProvider
from market_producer.providers.replay import HistoricalReplayProvider
from market_producer.providers.simulated import SimulatorProvider, aligned_start
from market_producer.publisher import KafkaMarketPublisher, QueueFullError
from market_producer.runtime import ProducerRuntime
from market_producer.simulator import AnomalyConfig, MarketSimulator, SymbolSimulator
from shared.config import KafkaSettings, LogSettings
from shared.kafka.client_config import producer_config
from shared.kafka.serde import JsonEventSerde
from shared.observability.logs import configure_logging, get_logger, kafka_error_logger

log = get_logger(__name__)


def build_provider(settings: ProducerSettings, *, now: datetime) -> MarketDataProvider:
    calibration = (
        Calibration.load(settings.calibration_file)
        if settings.calibration_file.exists()
        else Calibration.empty()
    )
    symbols = calibration.universe(settings.symbols or None, settings.num_symbols)

    if settings.mode == "replay":
        snapshot_dir = settings.replay_snapshot_dir
        if snapshot_dir is None:  # also enforced by settings validation
            raise ValueError("replay mode requires a snapshot directory")
        return HistoricalReplayProvider(
            snapshot_dir,
            symbols,
            bars_per_second=settings.replay_bars_per_second,
            start_at=now.timestamp(),
            limit=settings.replay_limit,
            source=f"replay:{snapshot_dir.parent.name}",
        )

    anomalies = AnomalyConfig(
        price_jump_probability=settings.price_jump_probability,
        jump_min=settings.jump_min,
        jump_max=settings.jump_max,
        volume_spike_probability=settings.volume_spike_probability,
    )
    simulator = MarketSimulator(
        [
            SymbolSimulator(
                symbol,
                calibration.for_symbol(symbol),
                interval=settings.interval,
                seed=settings.seed,
                volatility_multiplier=settings.volatility_multiplier,
                drift_annual=None if settings.use_calibrated_drift else settings.drift_annual,
                anomalies=anomalies,
            )
            for symbol in symbols
        ]
    )
    log.info(
        "simulator.configured",
        symbols=symbols,
        interval=settings.interval.value,
        calibration=calibration.source,
        seed=settings.seed,
    )
    return SimulatorProvider(
        simulator, start=aligned_start(now, settings.interval, settings.backfill_bars)
    )


def main() -> int:
    log_settings = LogSettings()
    configure_logging("market-producer", level=log_settings.level, fmt=log_settings.format)
    settings = ProducerSettings()
    kafka = KafkaSettings()

    stop = threading.Event()

    def _request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("producer.shutdown_requested", signal=signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    metrics = ProducerMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)

    from confluent_kafka import Producer  # noqa: PLC0415 - keep import cost off tests

    _on_error = kafka_error_logger(log)
    producer = Producer(producer_config(kafka, client_id=settings.client_id, error_cb=_on_error))
    publisher = KafkaMarketPublisher(
        producer, topic=settings.topic, serde=JsonEventSerde(), metrics=metrics
    )
    provider = build_provider(settings, now=datetime.fromtimestamp(time.time(), tz=UTC))
    runtime = ProducerRuntime(
        provider, publisher, metrics, stop=stop, flush_timeout_s=settings.flush_timeout_s
    )
    try:
        summary = runtime.run()
    except QueueFullError as exc:
        log.error("producer.aborted", error=str(exc))
        return 1
    return 0 if summary.failed == 0 and summary.undelivered_at_exit == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
