"""Locust scenario for the REST API.

    uv run --group loadtest locust -f loadtests/locustfile.py --headless \
        --host http://localhost:8000 -u 50 -r 10 -t 60s --csv data/loadtest/api

The task weights approximate the dashboard: it polls quotes and the pipeline
status, loads history once per symbol, and occasionally reads indicators,
anomalies and predictions. Symbols are discovered from ``/api/v1/stocks`` so
the test works against any data set.

The API rate-limits per client (default 20 req/s, burst 60). A load test from
one address would measure the limiter, not the service: start the API under
test with a high ``API_RATE_LIMIT_PER_S`` / ``API_RATE_LIMIT_BURST`` (see
docs/PERFORMANCE.md), or use ``--tags limiter`` to test the limiter itself.
"""

from __future__ import annotations

import random
from typing import Any, ClassVar

from locust import HttpUser, between, tag, task


class DashboardUser(HttpUser):
    wait_time = between(0.05, 0.25)
    symbols: ClassVar[list[str]] = []

    def on_start(self) -> None:
        if not DashboardUser.symbols:
            resp = self.client.get("/api/v1/stocks", name="/api/v1/stocks")
            body: Any = resp.json() if resp.ok else []
            DashboardUser.symbols = [s["symbol"] for s in body] or ["AAPL"]

    def _symbol(self) -> str:
        return random.choice(DashboardUser.symbols)  # noqa: S311 - load generation, not security

    @task(10)
    def quotes(self) -> None:
        self.client.get("/api/v1/stocks", name="/api/v1/stocks")

    @task(6)
    def pipeline_status(self) -> None:
        self.client.get("/api/v1/status", name="/api/v1/status")

    @task(8)
    def detail(self) -> None:
        self.client.get(f"/api/v1/stocks/{self._symbol()}", name="/api/v1/stocks/{symbol}")

    @task(4)
    def history(self) -> None:
        self.client.get(
            f"/api/v1/stocks/{self._symbol()}/history?limit=500",
            name="/api/v1/stocks/{symbol}/history",
        )

    @task(3)
    def indicators(self) -> None:
        self.client.get(
            f"/api/v1/stocks/{self._symbol()}/indicators?limit=500",
            name="/api/v1/stocks/{symbol}/indicators",
        )

    @task(3)
    def anomalies(self) -> None:
        self.client.get("/api/v1/anomalies?limit=50", name="/api/v1/anomalies")

    @task(2)
    def predictions(self) -> None:
        self.client.get(
            f"/api/v1/stocks/{self._symbol()}/predictions",
            name="/api/v1/stocks/{symbol}/predictions",
        )

    @task(1)
    def model(self) -> None:
        self.client.get("/api/v1/model", name="/api/v1/model")


class LimiterUser(HttpUser):
    """Hammers one cheap endpoint to show the rate limiter shedding load (429s).

    Select it explicitly: `locust ... LimiterUser`.
    """

    wait_time = between(0, 0.01)

    @tag("limiter")
    @task
    def hammer(self) -> None:
        with self.client.get("/api/v1/status", name="limiter", catch_response=True) as resp:
            if resp.status_code in (200, 429):
                resp.success()
