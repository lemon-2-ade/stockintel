"""Entry point: ``python -m stream_processor.main``."""

from __future__ import annotations

import signal
import sys
import threading
from types import FrameType

from prometheus_client import start_http_server

from shared.config import KafkaSettings, LogSettings
from shared.kafka.client_config import consumer_config, producer_config
from shared.observability.logs import configure_logging, get_logger, kafka_error_logger
from stream_processor.app import DeliveryError, StreamProcessorApp
from stream_processor.config import StreamSettings
from stream_processor.metrics import StreamMetrics
from stream_processor.processor import BarProcessor

log = get_logger(__name__)


def main() -> int:
    log_settings = LogSettings()
    configure_logging("stream-processor", level=log_settings.level, fmt=log_settings.format)
    settings = StreamSettings()
    kafka = KafkaSettings()

    stop = threading.Event()

    def _request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("stream_processor.shutdown_requested", signal=signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    metrics = StreamMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)

    from confluent_kafka import Consumer, Producer  # noqa: PLC0415

    _on_error = kafka_error_logger(log)
    consumer = Consumer(
        consumer_config(
            kafka, group_id=settings.group_id, client_id=settings.client_id, error_cb=_on_error
        )
    )
    producer = Producer(producer_config(kafka, client_id=settings.client_id, error_cb=_on_error))
    app = StreamProcessorApp(
        consumer,
        producer,
        BarProcessor(detectors=settings.detector_config(), max_symbols=settings.max_symbols),
        metrics,
        settings.app_config(),
        stop=stop,
    )
    try:
        app.run()
    except DeliveryError as exc:
        log.error("stream_processor.aborted", error=str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
