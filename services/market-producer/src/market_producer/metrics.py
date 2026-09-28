"""Prometheus metrics for the producer.

Label cardinality is bounded: ``symbol`` only takes the configured universe
(tens of values), never free-form input.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class ProducerMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.bars_generated = Counter(
            "sip_producer_bars_generated_total",
            "Bars handed to Kafka by the provider",
            ["symbol"],
            registry=r,
        )
        self.delivered = Counter(
            "sip_producer_events_delivered_total",
            "Events acknowledged by Kafka (acks=all)",
            ["topic"],
            registry=r,
        )
        self.delivery_failures = Counter(
            "sip_producer_delivery_failures_total",
            "Events that could not be delivered after all client retries",
            ["topic", "reason"],
            registry=r,
        )
        self.buffer_full = Counter(
            "sip_producer_buffer_full_total",
            "Times the local producer queue was full (backpressure from Kafka)",
            registry=r,
        )
        self.delivery_latency = Histogram(
            "sip_producer_delivery_latency_seconds",
            "Time from produce() to broker acknowledgement",
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
            registry=r,
        )
        self.publish_lag = Histogram(
            "sip_producer_publish_lag_seconds",
            "How late a batch was published relative to its due time (pacing health)",
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
            registry=r,
        )
        self.injected_anomalies = Counter(
            "sip_producer_injected_anomalies_total",
            "Anomalies injected by the simulator (ground truth for detector evaluation)",
            ["type"],
            registry=r,
        )
        self.queue_length = Gauge(
            "sip_producer_queue_messages",
            "Messages waiting in the local producer queue",
            registry=r,
        )
        self.last_delivery = Gauge(
            "sip_producer_last_delivery_timestamp_seconds",
            "Unix time of the last successful delivery (staleness alerting)",
            registry=r,
        )
