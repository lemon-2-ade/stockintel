"""HTTP/WebSocket metrics. Routes are labelled by template (``/stocks/{symbol}``),
never by raw path, to keep label cardinality bounded."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class ApiMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = r = registry or CollectorRegistry()
        self.requests = Counter(
            "sip_api_requests_total",
            "HTTP requests",
            ["method", "route", "status"],
            registry=r,
        )
        self.latency = Histogram(
            "sip_api_request_duration_seconds",
            "HTTP request latency",
            ["method", "route"],
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
            registry=r,
        )
        self.in_flight = Gauge("sip_api_requests_in_flight", "Requests in progress", registry=r)
        self.rate_limited = Counter(
            "sip_api_rate_limited_total", "Requests rejected by the rate limiter", registry=r
        )
        self.ws_connections = Gauge(
            "sip_api_websocket_connections", "Open WebSocket connections", registry=r
        )
        self.ws_messages = Counter(
            "sip_api_websocket_messages_total",
            "Messages sent to WebSocket clients",
            ["type"],
            registry=r,
        )
        self.ws_coalesced = Counter(
            "sip_api_websocket_coalesced_total",
            "Bar updates dropped by throttling because a newer one superseded them",
            registry=r,
        )
