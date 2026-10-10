"""Inference service: request contract, prediction records, model swapping."""

from __future__ import annotations

import asyncio
import json
import math
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pytest
from fastapi.testclient import TestClient

from inference.config import InferenceSettings
from inference.main import create_app
from inference.metrics import InferenceMetrics
from inference.model import LoadedModel, ModelHolder
from inference.predictor import target_timestamp
from shared.features import FEATURE_NAMES, FEATURE_SET_VERSION
from shared.schemas import BarInterval, Direction, PredictionEvent
from shared.schemas.inference import MAX_BATCH

NAMES = tuple(FEATURE_NAMES)


class LinearStub:
    """P(up) = sigmoid(10 * ret_5): deterministic and order-sensitive."""

    def __init__(self) -> None:
        self.calls = 0

    def predict_matrix(self, matrix: np.ndarray, columns: tuple[str, ...]) -> Any:
        self.calls += 1
        x = matrix[:, columns.index("ret_5")]
        return 1 / (1 + np.exp(-10 * x))


def stub_model(version: str = "1", fsv: str = FEATURE_SET_VERSION) -> LoadedModel:
    return LoadedModel(
        name="m",
        version=version,
        alias="champion",
        feature_set_version=fsv,
        feature_names=NAMES,
        horizon_bars=5,
        predictor=LinearStub(),
    )


class FakeSource:
    def __init__(self, models: dict[str, LoadedModel] | None = None) -> None:
        self.models = models or {}
        self.alias_target: str | None = None
        self.fail = False

    def resolve(self, name: str, alias: str) -> str | None:
        if self.fail:
            raise ConnectionError("registry down")
        return self.alias_target

    def load(self, name: str, version: str, alias: str) -> LoadedModel:
        return self.models[version]


def features(ret_5: float = 0.02) -> dict[str, float]:
    values = dict.fromkeys(NAMES, 0.0)
    values["ret_5"] = ret_5
    return values


def instance(**overrides: Any) -> dict[str, Any]:
    body = {
        "symbol": "AAPL",
        "timestamp": "2026-09-18T20:00:00Z",  # a Friday
        "interval": "1d",
        "source_event_id": "6f1c1f7e-3a3e-4b55-9d3b-0b5c2c7d9a10",
        "feature_set_version": FEATURE_SET_VERSION,
        "features": features(),
    }
    return body | overrides


@pytest.fixture(name="source")
def source_fixture() -> FakeSource:
    source = FakeSource({"1": stub_model("1"), "2": stub_model("2")})
    source.alias_target = "1"
    return source


@pytest.fixture(name="client")
def client_fixture(source: FakeSource) -> Iterator[TestClient]:
    app = create_app(InferenceSettings(model_name="m"), source=source, refresh=False)
    with TestClient(app) as client:
        yield client


class TestPredict:
    def test_response_is_a_valid_prediction_event(self, client: TestClient) -> None:
        response = client.post("/predict", json={"instances": [instance()]})
        assert response.status_code == 200
        body = response.json()
        assert body["model"]["version"] == "1"
        event = PredictionEvent.model_validate(body["predictions"][0])
        p_up = 1 / (1 + math.exp(-10 * 0.02))
        assert event.class_probabilities is not None
        assert event.class_probabilities[Direction.UP] == pytest.approx(p_up)
        assert event.predicted_direction == "up"
        assert event.confidence == pytest.approx(p_up)
        assert event.feature_set_version == FEATURE_SET_VERSION
        assert event.event_id == event.prediction_id
        assert event.source == "inference"

    def test_ids_are_deterministic_per_input_and_model(self, client: TestClient) -> None:
        def ids() -> list[str]:
            body = client.post("/predict", json={"instances": [instance()]}).json()
            return [p["prediction_id"] for p in body["predictions"]]

        assert ids() == ids()

    def test_feature_order_in_the_request_does_not_matter(self, client: TestClient) -> None:
        shuffled = dict(reversed(list(features(-0.03).items())))
        a = client.post("/predict", json={"instances": [instance(features=features(-0.03))]})
        b = client.post("/predict", json={"instances": [instance(features=shuffled)]})
        assert (
            a.json()["predictions"][0]["class_probabilities"]
            == (b.json()["predictions"][0]["class_probabilities"])
        )
        assert a.json()["predictions"][0]["predicted_direction"] == "down"

    def test_batch(self, client: TestClient) -> None:
        batch = [
            instance(source_event_id=str(uuid.uuid4()), features=features(r))
            for r in (-0.05, 0.0, 0.05)
        ]
        body = client.post("/predict", json={"instances": batch}).json()
        assert [p["predicted_direction"] for p in body["predictions"]] == ["down", "up", "up"]

    @pytest.mark.parametrize(
        ("change", "fragment"),
        [
            ({"feature_set_version": "fs-0.9.0"}, "does not match"),
            ({"features": {"ret_5": 0.1}}, "missing features"),
        ],
    )
    def test_incompatible_input_is_rejected(
        self, client: TestClient, change: dict[str, Any], fragment: str
    ) -> None:
        response = client.post("/predict", json={"instances": [instance(**change)]})
        assert response.status_code == 422
        assert fragment in response.json()["detail"]

    def test_non_finite_and_oversized_requests_fail_validation(self, client: TestClient) -> None:
        bad = features()
        bad["vol_10"] = float("inf")
        response = client.post(
            "/predict",
            content=json.dumps({"instances": [instance(features=bad)]}),  # writes Infinity
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 422
        too_many = {"instances": [instance()] * (MAX_BATCH + 1)}
        assert client.post("/predict", json=too_many).status_code == 422


class TestOperations:
    def test_ready_and_model_info(self, client: TestClient) -> None:
        assert client.get("/ready").json() == {"status": "ready", "model": "m/1"}
        assert client.get("/model").json()["alias"] == "champion"
        metrics = client.get("/metrics").text
        assert 'sip_inference_model_info{model="m",version="1"} 1.0' in metrics

    def test_without_a_model_the_service_is_not_ready(self) -> None:
        app = create_app(InferenceSettings(model_name="m"), source=FakeSource(), refresh=False)
        with TestClient(app) as client:
            assert client.get("/health").status_code == 200
            assert client.get("/ready").status_code == 503
            assert client.post("/predict", json={"instances": [instance()]}).status_code == 503


class TestModelHolder:
    def holder(self, source: FakeSource) -> ModelHolder:
        return ModelHolder(
            source,
            name="m",
            alias="champion",
            feature_set_version=FEATURE_SET_VERSION,
            metrics=InferenceMetrics(),
        )

    def test_swaps_when_the_alias_moves(self, source: FakeSource) -> None:
        holder = self.holder(source)
        assert asyncio.run(holder.refresh())
        assert not asyncio.run(holder.refresh()), "same version: no reload"
        source.alias_target = "2"
        assert asyncio.run(holder.refresh())
        assert holder.current is not None
        assert holder.current.version == "2"

    def test_incompatible_or_failed_loads_keep_the_current_model(self, source: FakeSource) -> None:
        holder = self.holder(source)
        asyncio.run(holder.refresh())
        source.models["3"] = stub_model("3", fsv="fs-9.9.9")
        source.alias_target = "3"
        assert not asyncio.run(holder.refresh())
        source.fail = True
        assert not asyncio.run(holder.refresh())
        assert holder.current is not None
        assert holder.current.version == "1"
        assert holder.metrics.load_failures._value.get() == 2


class TestTargetTimestamp:
    def test_daily_counts_weekdays(self) -> None:
        friday = datetime(2026, 9, 18, 20, tzinfo=UTC)
        assert target_timestamp(friday, BarInterval.D1, 5) == datetime(2026, 9, 25, 20, tzinfo=UTC)
        assert target_timestamp(friday, BarInterval.D1, 1) == datetime(2026, 9, 21, 20, tzinfo=UTC)

    def test_intraday_uses_the_bar_duration(self) -> None:
        t = datetime(2026, 9, 18, 14, 30, tzinfo=UTC)
        assert target_timestamp(t, BarInterval.M1, 5) == datetime(2026, 9, 18, 14, 35, tzinfo=UTC)
