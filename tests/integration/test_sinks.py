"""Sinks against real PostgreSQL and Redis (no Kafka needed)."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import redis
from factories import make_bar
from sqlalchemy import Engine, text

from shared import cache
from shared.schemas import (
    AnomalyEvent,
    AnomalyType,
    BarInterval,
    Direction,
    EnrichedBarEvent,
    IndicatorSnapshot,
    MarketBarEvent,
    PredictionEvent,
    PredictionTask,
    Severity,
    deterministic_prediction_id,
)
from sinks.postgres_sink import PostgresSink
from sinks.redis_sink import RedisCacheSink

pytestmark = pytest.mark.integration
T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def unique_symbol() -> str:
    return "Z" + uuid.uuid4().hex[:8].upper()


def bars(symbol: str, n: int = 3) -> list[MarketBarEvent]:
    return [make_bar(symbol=symbol, timestamp=T0 + timedelta(minutes=i)) for i in range(n)]


def enriched(bar: MarketBarEvent) -> EnrichedBarEvent:
    return EnrichedBarEvent.from_bar(
        bar, indicators=IndicatorSnapshot(sma_20=100.5), indicator_version="1.0.0", source="t"
    )


def anomaly(bar: MarketBarEvent) -> AnomalyEvent:
    return AnomalyEvent(
        source="t",
        symbol=bar.symbol,
        timestamp=bar.timestamp,
        interval=bar.interval,
        anomaly_type=AnomalyType.VOLUME_SPIKE,
        severity=Severity.HIGH,
        observed_value=9.0,
        expected_value=1.0,
        score=9.0,
        threshold=6.0,
        detector="volume_median",
        detector_version="1.0.0",
        source_event_id=bar.event_id,
        anomaly_id=uuid.uuid5(bar.event_id, "a"),
    )


def prediction(bar: MarketBarEvent) -> PredictionEvent:
    return PredictionEvent(
        source="t",
        prediction_id=deterministic_prediction_id(bar.event_id, "m", "1", 5),
        symbol=bar.symbol,
        timestamp=bar.timestamp,
        interval=BarInterval.M1,
        horizon_bars=5,
        target_timestamp=bar.timestamp + timedelta(minutes=5),
        task=PredictionTask.DIRECTION,
        predicted_direction=Direction.UP,
        class_probabilities={Direction.UP: 0.6, Direction.DOWN: 0.4},
        confidence=0.6,
        model_name="m",
        model_version="1",
        feature_set_version="fs1",
        source_event_id=bar.event_id,
    )


class TestPostgresSink:
    def test_writes_every_event_type_idempotently(self, migrated_engine: Engine) -> None:
        symbol = unique_symbol()
        raw = bars(symbol)
        events = [*raw, *(enriched(b) for b in raw), anomaly(raw[-1]), prediction(raw[0])]
        sink = PostgresSink(migrated_engine)

        first = sink.write(events)
        assert first.written == {
            "market_bars": 3,
            "bar_indicators": 3,
            "anomalies": 1,
            "predictions": 1,
        }
        again = PostgresSink(migrated_engine).write(events)  # e.g. redelivery after restart
        assert sum(again.written.values()) == 0
        assert again.skipped["market_bars"] == 3

        with migrated_engine.connect() as conn:
            assert (
                conn.execute(
                    text("SELECT count(*) FROM symbols WHERE symbol = :s"), {"s": symbol}
                ).scalar_one()
                == 1
            )
            indicators: dict[str, float] = conn.execute(
                text("SELECT indicators FROM bar_indicators WHERE symbol = :s LIMIT 1"),
                {"s": symbol},
            ).scalar_one()
            assert indicators["sma_20"] == 100.5
            probs: dict[str, float] = conn.execute(
                text("SELECT class_probabilities FROM predictions WHERE symbol = :s"),
                {"s": symbol},
            ).scalar_one()
            assert probs == {"up": 0.6, "down": 0.4}

    def test_rows_the_database_refuses_are_rejected_individually(
        self, migrated_engine: Engine
    ) -> None:
        symbol = unique_symbol()
        good = bars(symbol, 2)
        # Bypass schema validation to simulate a bad row reaching the sink.
        bad = good[0].model_copy(
            update={"timestamp": T0 + timedelta(hours=1), "event_id": uuid.uuid4(), "volume": -5}
        )
        result = PostgresSink(migrated_engine).write([*good, bad])
        assert result.written["market_bars"] == 2
        ((rejected, reason),) = result.rejected
        assert rejected is bad
        assert "non_negative_volume" in reason

    def test_unreachable_database_is_transient(self) -> None:
        from shared.config import PostgresSettings  # noqa: PLC0415
        from shared.db.session import create_db_engine  # noqa: PLC0415
        from sinks.runner import TransientSinkError  # noqa: PLC0415

        engine = create_db_engine(PostgresSettings(host="127.0.0.1", port=1), application_name="t")
        with pytest.raises(TransientSinkError):
            PostgresSink(engine).write(bars(unique_symbol(), 1))


class TestRedisSink:
    def test_latest_state_moves_forward_only_and_publishes(self, redis_client: redis.Redis) -> None:
        symbol = unique_symbol()
        pubsub = redis_client.pubsub()  # type: ignore[no-untyped-call]
        pubsub.subscribe(cache.channel(symbol))
        pubsub.get_message(timeout=1)  # subscription confirmation
        sink = RedisCacheSink(redis_client)
        b0, b1 = bars(symbol, 2)

        assert sink.write([enriched(b1)]).written == {"latest_bar": 1}
        result = sink.write([enriched(b0), enriched(b1)])  # older + duplicate
        assert result.skipped == {"latest_bar": 2}

        stored = json.loads(redis_client.hget(cache.latest_bar_key(symbol), "data"))  # type: ignore[arg-type]
        assert stored["timestamp"] == b1.timestamp.isoformat().replace("+00:00", "Z")
        assert symbol in redis_client.smembers(cache.ACTIVE_SYMBOLS)

        messages = []
        while (msg := pubsub.get_message(timeout=0.5)) is not None:
            messages.append(json.loads(msg["data"]))
        assert [m["type"] for m in messages] == ["bar"], "stale/duplicate writes are not published"
        pubsub.close()

    def test_anomalies_are_deduplicated_and_capped(self, redis_client: redis.Redis) -> None:
        symbol = unique_symbol()
        sink = RedisCacheSink(redis_client)
        raw = bars(symbol, 3)
        events = [anomaly(b) for b in raw]
        assert sink.write(events).written == {"anomalies": 3}
        # Same anomalies re-processed later (new produced_at): no new members.
        reprocessed = [a.model_copy(update={"produced_at": datetime.now(UTC)}) for a in events]
        assert sink.write(reprocessed).skipped == {"anomalies": 3}
        members = redis_client.zrevrange(cache.symbol_anomalies_key(symbol), 0, -1)
        assert len(members) == 3
        newest = json.loads(str(members[0]))
        assert newest["timestamp"].startswith("2026-01-05T14:32")

    def test_prediction_expires_after_horizon(self, redis_client: redis.Redis) -> None:
        symbol = unique_symbol()
        RedisCacheSink(redis_client).write([prediction(bars(symbol, 1)[0])])
        ttl = redis_client.pttl(cache.latest_prediction_key(symbol))
        assert 0 < ttl <= 5 * 60 * 1000


def test_runner_with_real_sink_is_safe_to_redeliver(migrated_engine: Engine) -> None:
    """Same batch twice through the runner: second pass writes nothing and commits."""
    from kafka_fakes import FakeConsumer, FakeProducer, Msg  # noqa: PLC0415
    from prometheus_client import CollectorRegistry  # noqa: PLC0415

    from shared.kafka.serde import JsonEventSerde  # noqa: PLC0415
    from sinks.metrics import SinkMetrics  # noqa: PLC0415
    from sinks.runner import BatchSinkRunner, RunnerConfig  # noqa: PLC0415

    serde = JsonEventSerde()
    symbol = unique_symbol()
    msgs = [Msg(serde.serialize(b), i, _key=b.symbol.encode()) for i, b in enumerate(bars(symbol))]
    consumer, metrics = FakeConsumer(), SinkMetrics(CollectorRegistry())
    runner = BatchSinkRunner(
        consumer,
        FakeProducer(),
        PostgresSink(migrated_engine),
        metrics,
        RunnerConfig(group_id="persistence"),
        stop=threading.Event(),
    )
    runner.handle_batch(msgs)
    runner.handle_batch(msgs)
    assert (
        metrics.registry.get_sample_value("sip_sink_written_total", {"target": "market_bars"}) == 3
    )
    assert (
        metrics.registry.get_sample_value("sip_sink_skipped_total", {"target": "market_bars"}) == 3
    )
    assert consumer.stored[-1] == (0, 2)
