"""Broker-side consumer-group lag exporter.

Services already export the lag of their own consumer, but that signal vanishes
exactly when it matters (a crashed or wedged consumer reports nothing), and the
sinks export none. This process asks the broker instead: for every watched
group it reads the committed offsets and the log-end offsets and publishes

    sip_kafka_group_lag{group,topic,partition}
    sip_kafka_group_committed_offset{group,topic,partition}
    sip_kafka_topic_end_offset{topic,partition}
    sip_kafka_exporter_up, sip_kafka_exporter_last_success_timestamp_seconds

Run: ``python -m shared.observability.kafka_lag``. Env: ``KAFKA_*`` (see
KafkaSettings), ``LAG_EXPORTER_GROUPS`` (comma separated), ``LAG_EXPORTER_PORT``
(default 8006), ``LAG_EXPORTER_INTERVAL_S`` (default 10).

Lag is ``end - committed`` (floored at 0). A group that has never committed on
a partition is reported with lag = end - low watermark (everything retained is
unread), never silently as 0.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from prometheus_client import CollectorRegistry, Gauge, start_http_server

from shared.config import KafkaSettings
from shared.kafka.client_config import consumer_config
from shared.kafka.topics import Topic
from shared.observability.logs import configure_logging, get_logger

log = get_logger(__name__)

DEFAULT_GROUPS = ("stream-processor", "persistence", "cache", "prediction-pipeline")
NO_OFFSET = -1001  # librdkafka OFFSET_INVALID: nothing committed


@dataclass(frozen=True, slots=True)
class PartitionOffsets:
    low: int
    end: int


@dataclass(frozen=True, slots=True)
class LagSample:
    group: str
    topic: str
    partition: int
    committed: int | None
    end: int
    lag: int


def compute_lag(
    group: str,
    committed: Mapping[tuple[str, int], int],
    offsets: Mapping[tuple[str, int], PartitionOffsets],
) -> list[LagSample]:
    """Lag per partition the group has committed on or that exists on its topics."""
    topics = {t for t, _ in committed}
    samples: list[LagSample] = []
    for (topic, partition), bounds in sorted(offsets.items()):
        if topic not in topics:
            continue
        pos = committed.get((topic, partition))
        pos = None if pos is None or pos < 0 else pos
        start = bounds.low if pos is None else pos
        samples.append(
            LagSample(group, topic, partition, pos, bounds.end, max(0, bounds.end - start))
        )
    return samples


class OffsetSource(Protocol):
    def committed(self, group: str) -> dict[tuple[str, int], int]: ...
    def offsets(self, topics: Iterable[str]) -> dict[tuple[str, int], PartitionOffsets]: ...


class KafkaOffsetSource:
    """confluent-kafka implementation of :class:`OffsetSource`."""

    def __init__(self, settings: KafkaSettings, timeout_s: float = 5.0) -> None:
        from confluent_kafka import Consumer  # noqa: PLC0415
        from confluent_kafka.admin import AdminClient  # noqa: PLC0415

        conf = consumer_config(settings, group_id="lag-exporter", client_id="lag-exporter")
        conf["enable.auto.commit"] = False
        self._consumer: Any = Consumer(conf)
        self._admin: Any = AdminClient(
            {k: v for k, v in conf.items() if k.startswith(("bootstrap", "security", "sasl"))}
        )
        self._timeout = timeout_s

    def committed(self, group: str) -> dict[tuple[str, int], int]:
        from confluent_kafka import ConsumerGroupTopicPartitions  # noqa: PLC0415

        futures = self._admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])
        result = futures[group].result(timeout=self._timeout)
        return {(tp.topic, tp.partition): tp.offset for tp in result.topic_partitions}

    def offsets(self, topics: Iterable[str]) -> dict[tuple[str, int], PartitionOffsets]:
        from confluent_kafka import TopicPartition  # noqa: PLC0415

        meta = self._consumer.list_topics(timeout=self._timeout)
        out: dict[tuple[str, int], PartitionOffsets] = {}
        for topic in topics:
            if topic not in meta.topics:
                continue
            for partition in meta.topics[topic].partitions:
                low, high = self._consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=self._timeout
                )
                out[(topic, partition)] = PartitionOffsets(low, high)
        return out


class LagMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = r = registry or CollectorRegistry()
        gp = ["group", "topic", "partition"]
        self.lag = Gauge("sip_kafka_group_lag", "Log-end minus committed offset", gp, registry=r)
        self.committed = Gauge(
            "sip_kafka_group_committed_offset", "Committed offset", gp, registry=r
        )
        self.end = Gauge(
            "sip_kafka_topic_end_offset", "Log-end offset", ["topic", "partition"], registry=r
        )
        self.up = Gauge("sip_kafka_exporter_up", "1 if the last poll succeeded", registry=r)
        self.last_success = Gauge(
            "sip_kafka_exporter_last_success_timestamp_seconds",
            "Unix time of the last successful poll",
            registry=r,
        )
        self._lag_keys: set[tuple[str, str, str]] = set()
        self._end_keys: set[tuple[str, str]] = set()

    def publish(self, samples: list[LagSample], *, now: float | None = None) -> None:
        lag_keys = {(s.group, s.topic, str(s.partition)) for s in samples}
        end_keys = {(s.topic, str(s.partition)) for s in samples}
        for key in self._lag_keys - lag_keys:  # dropped partitions / groups
            self.lag.remove(*key)
            self.committed.remove(*key)
        for end_key in self._end_keys - end_keys:
            self.end.remove(*end_key)
        for s in samples:
            key = (s.group, s.topic, str(s.partition))
            self.lag.labels(*key).set(s.lag)
            if s.committed is not None:
                self.committed.labels(*key).set(s.committed)
            elif key in self._lag_keys:
                self.committed.remove(*key)
            self.end.labels(s.topic, str(s.partition)).set(s.end)
        self._lag_keys, self._end_keys = lag_keys, end_keys
        self.up.set(1)
        self.last_success.set(time.time() if now is None else now)


def poll_once(source: OffsetSource, groups: Iterable[str], metrics: LagMetrics) -> None:
    topics = [t.value for t in Topic]
    offsets = source.offsets(topics)
    samples: list[LagSample] = []
    for group in groups:
        samples += compute_lag(group, source.committed(group), offsets)
    metrics.publish(samples)


def main() -> int:
    configure_logging("kafka-lag-exporter", level=os.getenv("LOG_LEVEL", "INFO"), fmt="json")
    groups = [g for g in os.getenv("LAG_EXPORTER_GROUPS", ",".join(DEFAULT_GROUPS)).split(",") if g]
    interval = float(os.getenv("LAG_EXPORTER_INTERVAL_S", "10"))
    metrics = LagMetrics()
    start_http_server(int(os.getenv("LAG_EXPORTER_PORT", "8006")), registry=metrics.registry)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    source = KafkaOffsetSource(KafkaSettings())
    log.info("lag_exporter.started", groups=groups)
    while not stop.is_set():
        try:
            poll_once(source, groups, metrics)
        except Exception as exc:
            metrics.up.set(0)
            log.warning("lag_exporter.poll_failed", error=str(exc))
        stop.wait(interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
