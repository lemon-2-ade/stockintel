"""Prometheus metrics for the stream processor (bounded label sets only)."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

_LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)


class StreamMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = r = registry or CollectorRegistry()
        self.messages = Counter(
            "sip_stream_messages_total",
            "Input messages by outcome (processed, duplicate, late, warmup, invalid, error)",
            ["outcome"],
            registry=r,
        )
        self.dead_lettered = Counter(
            "sip_stream_dead_lettered_total", "Messages sent to the DLQ", ["reason"], registry=r
        )
        self.anomalies = Counter(
            "sip_stream_anomalies_total",
            "Anomalies published",
            ["anomaly_type", "severity"],
            registry=r,
        )
        self.detector_outcomes = Counter(
            "sip_stream_detector_outcomes_total",
            "Detections scored against simulator ground truth",
            ["detector", "outcome"],
            registry=r,
        )
        self.missing_bars = Counter(
            "sip_stream_missing_bars_total", "Gaps detected in per-symbol bar sequences", registry=r
        )
        self.compute_seconds = Histogram(
            "sip_stream_bar_compute_seconds",
            "CPU time to update indicators + detectors for one bar",
            buckets=(1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 5e-4, 1e-3, 2.5e-3, 1e-2),
            registry=r,
        )
        self.end_to_end = Histogram(
            "sip_stream_end_to_end_latency_seconds",
            "Raw event produced_at -> enriched event built (processing time)",
            buckets=_LATENCY_BUCKETS,
            registry=r,
        )
        self.batch_size = Histogram(
            "sip_stream_batch_size",
            "Messages per consumed batch",
            buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
            registry=r,
        )
        self.flush_seconds = Histogram(
            "sip_stream_flush_seconds",
            "Time waiting for output deliveries before committing a batch",
            buckets=_LATENCY_BUCKETS,
            registry=r,
        )
        self.consumer_lag = Gauge(
            "sip_stream_consumer_lag",
            "High watermark minus consumer position",
            ["topic", "partition"],
            registry=r,
        )
        self.tracked_symbols = Gauge(
            "sip_stream_tracked_symbols", "Symbols with in-memory state", registry=r
        )
