"""Entry point: ``python -m prediction_pipeline.main``."""

from __future__ import annotations

import signal
import sys
import threading
from types import FrameType

from prometheus_client import start_http_server
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from prediction_pipeline.app import DeliveryError, PipelineConfig, PredictionPipelineApp
from prediction_pipeline.client import InferenceClient
from prediction_pipeline.features import FeatureStore
from prediction_pipeline.metrics import PipelineMetrics
from shared.config import KafkaSettings, LogSettings
from shared.kafka.client_config import consumer_config, producer_config
from shared.observability.logs import configure_logging, get_logger, kafka_error_logger

log = get_logger(__name__)


class PipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PIPELINE_", extra="ignore")

    group_id: str = "prediction-pipeline"
    client_id: str = "prediction-pipeline"
    batch_size: int = Field(default=500, ge=1, le=10_000)
    poll_timeout_s: float = Field(default=0.5, gt=0, le=10)
    flush_timeout_s: float = Field(default=10.0, gt=0)
    warmup_messages: int = Field(default=2_000, ge=0, le=1_000_000)
    predict_every_n_bars: int = Field(default=1, ge=1)
    max_symbols: int = Field(default=10_000, ge=1)
    metrics_port: int = Field(default=8004, ge=1, le=65_535)
    inference_url: str = "http://localhost:8010"
    inference_timeout_s: float = Field(default=2.0, gt=0)
    inference_retries: int = Field(default=2, ge=0, le=10)

    def app_config(self) -> PipelineConfig:
        return PipelineConfig(
            group_id=self.group_id,
            batch_size=self.batch_size,
            poll_timeout_s=self.poll_timeout_s,
            flush_timeout_s=self.flush_timeout_s,
            warmup_messages=self.warmup_messages,
            predict_every_n_bars=self.predict_every_n_bars,
        )


def main() -> int:
    log_settings = LogSettings()
    configure_logging("prediction-pipeline", level=log_settings.level, fmt=log_settings.format)
    settings = PipelineSettings()
    kafka = KafkaSettings()
    stop = threading.Event()

    def _request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("prediction_pipeline.shutdown_requested", signal=signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    metrics = PipelineMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)

    from confluent_kafka import Consumer, Producer  # noqa: PLC0415

    on_error = kafka_error_logger(log)
    consumer = Consumer(
        consumer_config(
            kafka, group_id=settings.group_id, client_id=settings.client_id, error_cb=on_error
        )
    )
    producer = Producer(producer_config(kafka, client_id=settings.client_id, error_cb=on_error))
    client = InferenceClient(
        settings.inference_url,
        timeout_s=settings.inference_timeout_s,
        retries=settings.inference_retries,
    )
    app = PredictionPipelineApp(
        consumer,
        producer,
        client,
        metrics,
        settings.app_config(),
        stop=stop,
        features=FeatureStore(settings.max_symbols),
    )
    try:
        app.run()
    except DeliveryError as exc:
        log.error("prediction_pipeline.aborted", error=str(exc))
        return 1
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
