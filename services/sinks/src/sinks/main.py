"""Entry point: ``python -m sinks.main postgres`` or ``python -m sinks.main redis``."""

from __future__ import annotations

import argparse
import signal
import sys
import threading
from types import FrameType

from prometheus_client import start_http_server

from shared.config import KafkaSettings, LogSettings, PostgresSettings, RedisSettings
from shared.kafka.client_config import consumer_config, producer_config
from shared.observability.logs import configure_logging, get_logger, kafka_error_logger
from shared.utils.retry import BackoffPolicy
from sinks.config import SinkSettings
from sinks.metrics import SinkMetrics
from sinks.runner import BatchSinkRunner, DeadLetterDeliveryError, RunnerConfig, SinkHandler

log = get_logger(__name__)

GROUPS = {"postgres": "persistence", "redis": "cache"}


def build_handler(kind: str) -> SinkHandler:
    if kind == "postgres":
        from shared.db.session import create_db_engine  # noqa: PLC0415
        from sinks.postgres_sink import PostgresSink  # noqa: PLC0415

        engine = create_db_engine(PostgresSettings(), application_name="sink-postgres")
        return PostgresSink(engine)

    import redis  # noqa: PLC0415

    from sinks.redis_sink import RedisCacheSink  # noqa: PLC0415

    return RedisCacheSink(redis.Redis(**RedisSettings().client_kwargs()))  # type: ignore[arg-type]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Kafka -> storage sink")
    parser.add_argument("sink", choices=sorted(GROUPS))
    args = parser.parse_args(argv)

    log_settings = LogSettings()
    configure_logging(f"sink-{args.sink}", level=log_settings.level, fmt=log_settings.format)
    settings = SinkSettings()
    kafka = KafkaSettings()
    group = GROUPS[args.sink]

    stop = threading.Event()

    def _request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("sink.shutdown_requested", signal=signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    metrics = SinkMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)

    from confluent_kafka import Consumer, Producer  # noqa: PLC0415

    on_error = kafka_error_logger(log)
    client_id = f"sink-{args.sink}"
    runner = BatchSinkRunner(
        Consumer(consumer_config(kafka, group_id=group, client_id=client_id, error_cb=on_error)),
        Producer(producer_config(kafka, client_id=client_id, error_cb=on_error)),
        build_handler(args.sink),
        metrics,
        RunnerConfig(
            group_id=group,
            batch_size=settings.batch_size,
            poll_timeout_s=settings.poll_timeout_s,
            flush_timeout_s=settings.flush_timeout_s,
            retry=BackoffPolicy(
                max_attempts=settings.retry_attempts, base_delay_s=0.5, max_delay_s=15.0
            ),
        ),
        stop=stop,
    )
    try:
        runner.run()
    except DeadLetterDeliveryError as exc:
        log.error("sink.aborted", error=str(exc))
        return 1
    except Exception:
        log.exception("sink.failed")  # storage down past the retry budget, etc.
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
