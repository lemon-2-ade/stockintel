"""Prediction pipeline: online/offline feature parity, batching, degradation."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx2 as httpx
import numpy as np
import pytest
from kafka_fakes import TP, FakeConsumer, FakeProducer, Msg
from ml_fixtures import cleaned_frame
from prometheus_client import CollectorRegistry

from prediction_pipeline.app import PipelineConfig, PredictionPipelineApp
from prediction_pipeline.client import (
    InferenceClient,
    InferenceRejectedError,
    InferenceUnavailableError,
)
from prediction_pipeline.features import FeatureStore, missing_bars, weekdays_between
from prediction_pipeline.metrics import PipelineMetrics
from shared.features import FEATURE_NAMES, FEATURE_SET_VERSION
from shared.features.computer import WARMUP_BARS
from shared.kafka.serde import JsonEventSerde
from shared.kafka.topics import Topic
from shared.schemas import (
    BarInterval,
    Direction,
    EnrichedBarEvent,
    IndicatorSnapshot,
    MarketBarEvent,
    PredictionEvent,
    PredictionTask,
    deterministic_prediction_id,
)
from shared.schemas.inference import ModelInfo, PredictInstance, PredictResponse
from stockml.features.dataset import compute_features

serde = JsonEventSerde()
T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def enriched(
    i: int,
    *,
    symbol: str = "AAPL",
    close: float | None = None,
    interval: BarInterval = BarInterval.M1,
    timestamp: datetime | None = None,
) -> EnrichedBarEvent:
    price = close if close is not None else 100 * (1 + 0.01 * np.sin(i / 3))
    bar = MarketBarEvent(
        source="test",
        symbol=symbol,
        timestamp=timestamp or T0 + timedelta(minutes=i),
        interval=interval,
        open=price,
        high=price * 1.002,
        low=price * 0.998,
        close=price,
        volume=1_000 + i,
    )
    return EnrichedBarEvent.from_bar(
        bar, indicators=IndicatorSnapshot(), indicator_version="t", source="stream-processor"
    )


def msg(event: EnrichedBarEvent, offset: int, partition: int = 0) -> Msg:
    return Msg(serde.serialize(event), offset, partition, "market.enriched", event.symbol.encode())


class FakePredictor:
    """Answers like the inference service: P(up) = 0.6 for every instance."""

    def __init__(self, fail: Exception | None = None) -> None:
        self.fail = fail
        self.batches: list[int] = []

    def predict(self, instances: list[PredictInstance]) -> PredictResponse:
        self.batches.append(len(instances))
        if self.fail is not None:
            raise self.fail
        events = []
        for inst in instances:
            pid = deterministic_prediction_id(inst.source_event_id, "m", "1", 5)
            events.append(
                PredictionEvent(
                    event_id=pid,
                    source="inference",
                    prediction_id=pid,
                    symbol=inst.symbol,
                    timestamp=inst.timestamp,
                    interval=inst.interval,
                    horizon_bars=5,
                    target_timestamp=inst.timestamp + 5 * inst.interval.duration,
                    task=PredictionTask.DIRECTION,
                    predicted_direction=Direction.UP,
                    class_probabilities={Direction.UP: 0.6, Direction.DOWN: 0.4},
                    confidence=0.6,
                    model_name="m",
                    model_version="1",
                    feature_set_version=inst.feature_set_version,
                    source_event_id=inst.source_event_id,
                )
            )
        model = ModelInfo(
            name="m",
            version="1",
            alias="champion",
            feature_set_version=FEATURE_SET_VERSION,
            horizon_bars=5,
            loaded_at=T0,
        )
        return PredictResponse(model=model, predictions=events)


def build(
    predictor: FakePredictor, consumer: FakeConsumer | None = None, **cfg: Any
) -> tuple[PredictionPipelineApp, FakeConsumer, FakeProducer, PipelineMetrics]:
    consumer = consumer or FakeConsumer()
    producer = FakeProducer()
    metrics = PipelineMetrics(CollectorRegistry())
    app = PredictionPipelineApp(
        consumer, producer, predictor, metrics, PipelineConfig(**cfg), stop=threading.Event()
    )
    return app, consumer, producer, metrics


def counter(metrics: PipelineMetrics, name: str, **labels: str) -> float:
    value = metrics.registry.get_sample_value(name, labels)
    return value or 0.0


# --------------------------------------------------------------------------- features


class TestFeatures:
    def test_live_features_equal_the_training_features(self) -> None:
        """The serving path must produce the numbers the model was trained on."""
        cleaned = cleaned_frame(150, ("AAA",), seed=11)
        offline = compute_features(cleaned).set_index("session_date")
        store = FeatureStore()
        compared = 0
        columns = [cleaned[c].to_numpy() for c in ("open", "high", "low", "close", "volume")]
        timestamps = list(cleaned["timestamp"].dt.to_pydatetime())
        sessions = list(cleaned["session_date"])
        for ts, session, o, h, lo, c, v in zip(timestamps, sessions, *columns, strict=True):
            bar = MarketBarEvent(
                source="test",
                symbol="AAA",
                timestamp=ts,
                interval=BarInterval.D1,
                open=float(o),
                high=float(h),
                low=float(lo),
                close=float(c),
                volume=int(v),
            )
            update = store.update(
                EnrichedBarEvent.from_bar(
                    bar, indicators=IndicatorSnapshot(), indicator_version="t", source="sp"
                )
            )
            if update.features is None:
                continue
            expected = offline.loc[session, list(FEATURE_NAMES)].to_numpy(dtype=float)
            got = np.array([update.features[f] for f in FEATURE_NAMES])
            np.testing.assert_allclose(got, expected, rtol=1e-12, atol=1e-15)
            compared += 1
        assert compared == 150 - WARMUP_BARS + 1

    def test_weekends_are_not_gaps_for_daily_bars(self) -> None:
        friday, monday = date(2026, 1, 2), date(2026, 1, 5)
        assert weekdays_between(friday, monday) == 1
        assert weekdays_between(friday, friday + timedelta(days=14)) == 10
        fri = datetime(2026, 1, 2, 21, tzinfo=UTC)
        assert missing_bars(fri, fri + timedelta(days=3), BarInterval.D1) == 0
        assert missing_bars(fri, fri + timedelta(days=4), BarInterval.D1) == 1  # Tuesday
        assert missing_bars(T0, T0 + timedelta(minutes=3), BarInterval.M1) == 2

    def test_stale_bars_are_ignored_and_gaps_reset(self) -> None:
        store = FeatureStore()
        for i in range(WARMUP_BARS):
            store.update(enriched(i))
        assert store.update(enriched(WARMUP_BARS)).features is not None
        assert store.update(enriched(3)).stale
        after_gap = store.update(enriched(WARMUP_BARS + 10))
        assert after_gap.gap == 9
        assert after_gap.features is None, "warm-up starts again after a gap"


# --------------------------------------------------------------------------- app


def stream(n: int, symbols: tuple[str, ...] = ("AAPL",)) -> list[Msg]:
    messages, offset = [], 0
    for i in range(n):
        for s in symbols:
            messages.append(msg(enriched(i, symbol=s), offset))
            offset += 1
    return messages


class TestApp:
    def test_predicts_once_warm_and_attaches_the_feature_snapshot(self) -> None:
        predictor = FakePredictor()
        app, consumer, producer, metrics = build(predictor)
        messages = stream(WARMUP_BARS + 4)
        app.handle_batch(messages)
        predictions = [s for s in producer.sent if s.topic == "market.predictions"]
        assert len(predictions) == 5  # bars 61..65 of 65
        event = serde.deserialize(predictions[0].value, PredictionEvent)
        assert event.features is not None
        assert set(event.features) == set(FEATURE_NAMES)
        assert consumer.stored == [(0, messages[-1].offset())]
        assert counter(metrics, "sip_pipeline_messages_total", outcome="not_ready") == 60

    def test_large_batches_are_split_for_the_inference_service(self) -> None:
        predictor = FakePredictor()
        symbols = tuple(f"S{i:03d}" for i in range(300))
        app, *_ = build(predictor)
        app.handle_batch(stream(WARMUP_BARS, symbols))
        assert predictor.batches == [256, 44]

    @pytest.mark.parametrize(
        ("error", "reason"),
        [
            (InferenceUnavailableError("down"), "unavailable"),
            (InferenceRejectedError("bad features"), "rejected"),
        ],
    )
    def test_inference_failures_skip_predictions_but_market_data_moves_on(
        self, error: Exception, reason: str
    ) -> None:
        app, consumer, producer, metrics = build(FakePredictor(fail=error))
        messages = stream(WARMUP_BARS + 1)
        app.handle_batch(messages)
        assert producer.sent == []
        assert consumer.stored == [(0, messages[-1].offset())], "offsets still committed"
        assert counter(metrics, "sip_pipeline_predictions_skipped_total", reason=reason) == 2

    def test_warm_up_replay_rebuilds_state_without_predicting(self) -> None:
        messages = stream(WARMUP_BARS + 2)
        consumer = FakeConsumer(committed={0: WARMUP_BARS})
        app, _, producer, _ = build(FakePredictor(), consumer, warmup_messages=1_000)
        app.on_assign(consumer, [TP("market.enriched", 0)])
        assert consumer.assigned[0].offset == 0, "seeks back to rebuild the features"
        app.handle_batch(messages)
        sent = [s for s in producer.sent if s.topic == "market.predictions"]
        assert len(sent) == 2, "only bars at/after the committed offset are predicted"

    def test_sampling_and_invalid_messages(self) -> None:
        app, consumer, producer, _ = build(FakePredictor(), predict_every_n_bars=2)
        messages = stream(WARMUP_BARS + 3)
        messages.append(Msg(b"{not json", len(messages), 0, "market.enriched"))
        app.handle_batch(messages)
        topics = [s.topic for s in producer.sent]
        assert topics.count("market.predictions") == 2  # bars 62 and 64
        assert topics.count(Topic.DEAD_LETTER) == 1
        assert consumer.stored[-1] == (0, messages[-1].offset()), "the bad message is committed"


# --------------------------------------------------------------------------- client


def instance() -> PredictInstance:
    return PredictInstance(
        symbol="AAPL",
        timestamp=T0,
        interval=BarInterval.M1,
        source_event_id=uuid.uuid4(),
        feature_set_version=FEATURE_SET_VERSION,
        features=dict.fromkeys(FEATURE_NAMES, 0.0),
    )


def client_with(responses: list[httpx.Response | Exception]) -> tuple[InferenceClient, list[int]]:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(len(json.loads(request.content)["instances"]))
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    client = InferenceClient(
        "http://inference", retries=2, backoff_s=0, transport=httpx.MockTransport(handler)
    )
    return client, calls


class TestClient:
    def test_retries_transient_failures(self) -> None:
        body = FakePredictor().predict([instance()]).model_dump_json()
        client, calls = client_with(
            [
                httpx.ConnectError("refused"),
                httpx.Response(503),
                httpx.Response(200, content=body),
            ]
        )
        assert len(client.predict([instance()]).predictions) == 1
        assert calls == [1, 1, 1]

    def test_gives_up_or_refuses(self) -> None:
        client, _ = client_with([httpx.Response(503)] * 3)
        with pytest.raises(InferenceUnavailableError):
            client.predict([instance()])
        client, calls = client_with([httpx.Response(422, json={"detail": "x"})])
        with pytest.raises(InferenceRejectedError):
            client.predict([instance()])
        assert calls == [1], "422 is not retried"
