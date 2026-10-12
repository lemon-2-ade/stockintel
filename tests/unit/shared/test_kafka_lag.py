from __future__ import annotations

from prometheus_client import CollectorRegistry

from shared.observability.kafka_lag import (
    LagMetrics,
    PartitionOffsets,
    compute_lag,
    poll_once,
)

OFFSETS = {
    ("market.raw", 0): PartitionOffsets(low=10, end=110),
    ("market.raw", 1): PartitionOffsets(low=0, end=50),
    ("market.enriched", 0): PartitionOffsets(low=0, end=500),
}


def test_lag_is_end_minus_committed() -> None:
    out = compute_lag("g", {("market.raw", 0): 100, ("market.raw", 1): 50}, OFFSETS)
    assert [(s.partition, s.lag) for s in out] == [(0, 10), (1, 0)]
    assert all(s.topic == "market.raw" for s in out)


def test_uncommitted_partition_counts_everything_retained() -> None:
    out = compute_lag("g", {("market.raw", 0): -1001}, OFFSETS)
    assert out[0].committed is None
    assert out[0].lag == 100  # end 110 - low 10
    assert out[1].lag == 50  # partition never seen by the group


def test_lag_never_negative() -> None:
    out = compute_lag("g", {("market.raw", 0): 999}, OFFSETS)
    assert out[0].lag == 0


def test_unwatched_topics_ignored() -> None:
    assert compute_lag("g", {}, OFFSETS) == []


class FakeSource:
    def __init__(self) -> None:
        self.groups = {"a": {("market.raw", 0): 90}}

    def committed(self, group: str) -> dict[tuple[str, int], int]:
        return self.groups[group]

    def offsets(self, topics: object) -> dict[tuple[str, int], PartitionOffsets]:
        return OFFSETS


def test_metrics_published_and_stale_series_removed() -> None:
    reg = CollectorRegistry()
    m = LagMetrics(reg)
    src = FakeSource()
    poll_once(src, ["a"], m)
    assert (
        reg.get_sample_value(
            "sip_kafka_group_lag", {"group": "a", "topic": "market.raw", "partition": "0"}
        )
        == 20
    )
    assert reg.get_sample_value("sip_kafka_exporter_up") == 1
    src.groups = {"a": {("market.enriched", 0): 400}}
    poll_once(src, ["a"], m)
    assert (
        reg.get_sample_value(
            "sip_kafka_group_lag", {"group": "a", "topic": "market.raw", "partition": "0"}
        )
        is None
    )
    assert (
        reg.get_sample_value(
            "sip_kafka_group_lag", {"group": "a", "topic": "market.enriched", "partition": "0"}
        )
        == 100
    )
