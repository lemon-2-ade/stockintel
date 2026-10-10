"""HTTP client for the inference service."""

from __future__ import annotations

import time
from typing import Protocol

import httpx2 as httpx

from shared.observability.logs import get_logger
from shared.schemas.inference import MAX_BATCH, PredictInstance, PredictRequest, PredictResponse

log = get_logger(__name__)


class InferenceUnavailableError(RuntimeError):
    """No usable answer after retries (service down, 5xx, timeouts)."""


class InferenceRejectedError(RuntimeError):
    """The service refused the input (HTTP 422). Retrying cannot help."""


class Predictor(Protocol):
    def predict(self, instances: list[PredictInstance]) -> PredictResponse: ...


class InferenceClient:
    """Sends batches of at most ``MAX_BATCH`` instances; retries transient failures.

    Retries are few and short on purpose: predictions are only useful while
    fresh, and the caller degrades to "no prediction" rather than stalling
    the market-data path.
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 2.0,
        retries: int = 2,
        backoff_s: float = 0.2,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout_s, transport=transport)
        self.retries = retries
        self.backoff_s = backoff_s

    def close(self) -> None:
        self._client.close()

    def predict(self, instances: list[PredictInstance]) -> PredictResponse:
        if not 1 <= len(instances) <= MAX_BATCH:
            raise ValueError(f"batch size must be 1..{MAX_BATCH}")
        body = PredictRequest(instances=instances).model_dump(mode="json")
        last_error = "no attempt"
        for attempt in range(self.retries + 1):
            try:
                response = self._client.post("/predict", json=body)
            except httpx.HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code == 200:
                    return PredictResponse.model_validate_json(response.content)
                if response.status_code == 422:
                    raise InferenceRejectedError(response.text[:500])
                last_error = f"HTTP {response.status_code}"
            if attempt < self.retries:
                time.sleep(self.backoff_s * (2**attempt))
        log.warning("inference.unavailable", error=last_error, attempts=self.retries + 1)
        raise InferenceUnavailableError(last_error)
