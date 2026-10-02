"""Prometheus metrics shared by both sinks (distinguished by the ``job`` label)."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Histogram


class SinkMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = r = registry or CollectorRegistry()
        self.messages = Counter("sip_sink_messages_total", "Input messages consumed", registry=r)
        self.written = Counter(
            "sip_sink_written_total", "Rows/keys written per target", ["target"], registry=r
        )
        self.skipped = Counter(
            "sip_sink_skipped_total",
            "Writes skipped as duplicates or stale (idempotency at work)",
            ["target"],
            registry=r,
        )
        self.dead_lettered = Counter(
            "sip_sink_dead_lettered_total", "Messages sent to the DLQ", ["reason"], registry=r
        )
        self.write_seconds = Histogram(
            "sip_sink_batch_write_seconds",
            "Time to write one batch to storage (incl. retries)",
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
            registry=r,
        )
