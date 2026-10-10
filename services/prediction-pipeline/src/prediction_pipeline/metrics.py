"""Prometheus metrics for the prediction pipeline."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class PipelineMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = r = registry or CollectorRegistry()
        self.messages = Counter(
            "sip_pipeline_messages_total",
            "Input bars by outcome (warmup, not_ready, stale, sampled_out, scored, invalid)",
            ["outcome"],
            registry=r,
        )
        self.predictions = Counter(
            "sip_pipeline_predictions_total",
            "Predictions published",
            ["model_version"],
            registry=r,
        )
        self.skipped = Counter(
            "sip_pipeline_predictions_skipped_total",
            "Bars that were ready but got no prediction",
            ["reason"],
            registry=r,
        )
        self.inference_seconds = Histogram(
            "sip_pipeline_inference_request_seconds",
            "Round trip of one /predict call",
            buckets=(0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
            registry=r,
        )
        self.feature_gaps = Counter(
            "sip_pipeline_feature_resets_total",
            "Feature state resets caused by gaps in a symbol's bars",
            registry=r,
        )
        self.tracked_symbols = Gauge(
            "sip_pipeline_tracked_symbols", "Symbols with feature state", registry=r
        )
        self.consumer_lag = Gauge(
            "sip_pipeline_consumer_lag",
            "Messages behind the end of the partition",
            ["topic", "partition"],
            registry=r,
        )
