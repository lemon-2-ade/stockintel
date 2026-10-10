"""Prometheus metrics for the inference service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

if TYPE_CHECKING:
    from inference.model import LoadedModel


class InferenceMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = r = registry or CollectorRegistry()
        self.requests = Counter(
            "sip_inference_requests_total", "Predict requests", ["outcome"], registry=r
        )
        self.predictions = Counter(
            "sip_inference_predictions_total", "Predictions served", ["model_version"], registry=r
        )
        self.model_latency = Histogram(
            "sip_inference_model_seconds",
            "Model call latency per request (feature frame build + predict)",
            buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
            registry=r,
        )
        self.batch_size = Histogram(
            "sip_inference_batch_size",
            "Instances per request",
            buckets=(1, 2, 4, 8, 16, 32, 64, 128, 256),
            registry=r,
        )
        self.model_info = Gauge(
            "sip_inference_model_info",
            "1 for the model version currently served",
            ["model", "version"],
            registry=r,
        )
        self.load_failures = Counter(
            "sip_inference_model_load_failures_total",
            "Failed attempts to load or validate a model",
            registry=r,
        )

    def set_model(self, name: str, version: str, previous: LoadedModel | None) -> None:
        if previous is not None:
            self.model_info.labels(previous.name, previous.version).set(0)
        self.model_info.labels(name, version).set(1)
