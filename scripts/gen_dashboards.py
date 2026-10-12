"""Generate the Grafana dashboards in infrastructure/grafana/dashboards.

    uv run python scripts/gen_dashboards.py          # write
    uv run python scripts/gen_dashboards.py --check  # fail if files are stale

The JSON is generated so panels stay consistent (datasource, units, layout)
and reviewable as Python; the committed JSON is what Grafana provisions.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parents[1] / "infrastructure" / "grafana" / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}


class Board:
    def __init__(self, uid: str, title: str, description: str) -> None:
        self.uid, self.title, self.description = uid, title, description
        self.panels: list[dict[str, Any]] = []
        self._id, self._x, self._y, self._rowh = 0, 0, 0, 0

    def row(self, title: str) -> None:
        self._flush()
        self._id += 1
        self.panels.append(
            {
                "id": self._id,
                "type": "row",
                "title": title,
                "collapsed": False,
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": self._y},
                "panels": [],
            }
        )
        self._y += 1

    def _flush(self) -> None:
        if self._x:
            self._y += self._rowh
            self._x, self._rowh = 0, 0

    def _place(self, w: int, h: int) -> dict[str, int]:
        if self._x + w > 24:
            self._flush()
        pos = {"h": h, "w": w, "x": self._x, "y": self._y}
        self._x += w
        self._rowh = max(self._rowh, h)
        if self._x >= 24:
            self._flush()
        return pos

    def panel(
        self,
        kind: str,
        title: str,
        queries: list[tuple[str, str]],
        *,
        unit: str = "short",
        w: int = 8,
        h: int = 8,
        description: str = "",
        options: dict[str, Any] | None = None,
        defaults: dict[str, Any] | None = None,
    ) -> None:
        self._id += 1
        self.panels.append(
            {
                "id": self._id,
                "type": kind,
                "title": title,
                "description": description,
                "datasource": DS,
                "gridPos": self._place(w, h),
                "targets": [
                    {"refId": chr(65 + i), "expr": e, "legendFormat": legend, "datasource": DS}
                    for i, (e, legend) in enumerate(queries)
                ],
                "fieldConfig": {"defaults": {"unit": unit, **(defaults or {})}, "overrides": []},
                "options": options or {},
            }
        )

    def ts(self, title: str, queries: list[tuple[str, str]], **kw: Any) -> None:
        self.panel("timeseries", title, queries, **kw)

    def stat(self, title: str, expr: str, **kw: Any) -> None:
        kw.setdefault("w", 4)
        kw.setdefault("h", 4)
        self.panel("stat", title, [(expr, "")], **kw)

    def build(self) -> dict[str, Any]:
        self._flush()
        return {
            "uid": self.uid,
            "title": self.title,
            "description": self.description,
            "tags": ["stockintel"],
            "schemaVersion": 41,
            "version": 1,
            "editable": False,
            "timezone": "utc",
            "refresh": "15s",
            "time": {"from": "now-1h", "to": "now"},
            "templating": {"list": []},
            "annotations": {"list": []},
            "panels": self.panels,
        }


def hist(metric: str, q: float, by: str = "le", window: str = "$__rate_interval") -> str:
    return f"histogram_quantile({q}, sum by ({by}) (rate({metric}_bucket[{window}])))"


def pipeline() -> Board:
    b = Board("sip-pipeline", "Pipeline", "Ingestion, stream processing and sinks.")
    b.row("Health")
    b.stat("Targets up", "sum(up)", description="Scrape targets currently reachable")
    b.stat(
        "Targets down",
        "count(up == 0) or vector(0)",
        defaults={
            "thresholds": {
                "mode": "absolute",
                "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}],
            }
        },
    )
    b.stat(
        "Seconds since last delivery",
        "time() - max(sip_producer_last_delivery_timestamp_seconds)",
        unit="s",
        description="Market-data freshness at the producer",
    )
    b.stat("Tracked symbols", "max(sip_stream_tracked_symbols)")
    b.stat(
        "DLQ (1h)",
        'sum(increase({__name__=~"sip_(stream|sink)_dead_lettered_total"}[1h])) or vector(0)',
    )
    b.stat("Consumer lag (all groups)", "sum(sip_kafka_group_lag)")
    b.row("Producer")
    b.ts(
        "Events delivered / s",
        [
            (
                "sum by (topic) (rate(sip_producer_events_delivered_total[$__rate_interval]))",
                "{{topic}}",
            )
        ],
        unit="ops",
    )
    b.ts(
        "Delivery latency (broker ack)",
        [
            (hist("sip_producer_delivery_latency_seconds", q), f"p{int(q * 100)}")
            for q in (0.5, 0.99)
        ],
        unit="s",
    )
    b.ts(
        "Delivery failures and backpressure",
        [
            ("sum(rate(sip_producer_delivery_failures_total[$__rate_interval]))", "failures/s"),
            ("sum(rate(sip_producer_buffer_full_total[$__rate_interval]))", "queue full/s"),
            ("max(sip_producer_queue_messages)", "queue depth"),
        ],
    )
    b.row("Stream processor")
    b.ts(
        "Messages by outcome / s",
        [("sum by (outcome) (rate(sip_stream_messages_total[$__rate_interval]))", "{{outcome}}")],
        unit="ops",
    )
    b.ts(
        "Processing latency (produced_at to enriched)",
        [
            (hist("sip_stream_end_to_end_latency_seconds", q), f"p{int(q * 100)}")
            for q in (0.5, 0.95, 0.99)
        ],
        unit="s",
        description="Includes time spent in Kafka, so it grows with consumer lag.",
    )
    b.ts(
        "CPU time per bar",
        [(hist("sip_stream_bar_compute_seconds", q), f"p{int(q * 100)}") for q in (0.5, 0.99)],
        unit="s",
    )
    b.ts(
        "Anomalies / min",
        [
            (
                "sum by (anomaly_type, severity) (increase(sip_stream_anomalies_total[1m]))",
                "{{anomaly_type}} {{severity}}",
            )
        ],
    )
    b.ts("Missing bars", [("sum(increase(sip_stream_missing_bars_total[5m]))", "last 5m")])
    b.ts(
        "Batch size and flush time",
        [
            (
                "histogram_quantile(0.5, sum by (le) (rate(sip_stream_batch_size_bucket[$__rate_interval])))",
                "batch p50",
            ),
            (hist("sip_stream_flush_seconds", 0.99), "flush p99 (s)"),
        ],
    )
    b.row("Sinks")
    b.ts(
        "Rows written / s",
        [
            (
                "sum by (job, target) (rate(sip_sink_written_total[$__rate_interval]))",
                "{{job}} {{target}}",
            )
        ],
        unit="ops",
    )
    b.ts(
        "Skipped as duplicate or stale / s",
        [
            (
                "sum by (job, target) (rate(sip_sink_skipped_total[$__rate_interval]))",
                "{{job}} {{target}}",
            )
        ],
        unit="ops",
        description="Idempotency at work: non-zero after a restart is expected.",
    )
    b.ts(
        "Batch write time",
        [
            (hist("sip_sink_batch_write_seconds", q, "job, le"), f"p{int(q * 100)} " + "{{job}}")
            for q in (0.5, 0.99)
        ],
        unit="s",
    )
    return b


def kafka() -> Board:
    b = Board(
        "sip-kafka",
        "Kafka consumer lag",
        "Broker-side lag per consumer group (kafka-lag-exporter).",
    )
    b.row("Lag")
    b.stat(
        "Exporter up",
        "min(sip_kafka_exporter_up)",
        w=4,
        defaults={
            "mappings": [{"type": "value", "options": {"0": {"text": "DOWN"}, "1": {"text": "ok"}}}]
        },
    )
    b.ts(
        "Lag by group",
        [("sum by (group) (sip_kafka_group_lag)", "{{group}}")],
        w=10,
        description="Log-end offset minus committed offset, summed over partitions.",
    )
    b.ts(
        "Lag by topic",
        [("sum by (group, topic) (sip_kafka_group_lag)", "{{group}} / {{topic}}")],
        w=10,
    )
    b.row("Throughput")
    b.ts(
        "Topic append rate (msgs/s)",
        [("sum by (topic) (deriv(sip_kafka_topic_end_offset[$__rate_interval]))", "{{topic}}")],
        unit="ops",
        w=12,
    )
    b.ts(
        "Commit rate (msgs/s)",
        [
            (
                "sum by (group) (deriv(sip_kafka_group_committed_offset[$__rate_interval]))",
                "{{group}}",
            )
        ],
        unit="ops",
        w=12,
    )
    b.row("Consumer-reported lag (cross-check)")
    b.ts("Stream processor", [("sum by (topic) (sip_stream_consumer_lag)", "{{topic}}")], w=12)
    b.ts("Prediction pipeline", [("sum by (topic) (sip_pipeline_consumer_lag)", "{{topic}}")], w=12)
    return b


def api() -> Board:
    b = Board("sip-api", "API", "REST and WebSocket serving.")
    b.row("Traffic")
    b.ts(
        "Requests / s by status code",
        [("sum by (status) (rate(sip_api_requests_total[$__rate_interval]))", "{{status}}")],
        unit="reqps",
    )
    b.ts(
        "Latency",
        [
            (hist("sip_api_request_duration_seconds", q), f"p{int(q * 100)}")
            for q in (0.5, 0.95, 0.99)
        ],
        unit="s",
    )
    b.ts(
        "5xx ratio",
        [
            (
                'sum(rate(sip_api_requests_total{status=~"5.."}[$__rate_interval])) / sum(rate(sip_api_requests_total[$__rate_interval]))',
                "5xx",
            )
        ],
        unit="percentunit",
    )
    b.row("Routes")
    b.ts(
        "p95 latency by route",
        [(hist("sip_api_request_duration_seconds", 0.95, "route, le"), "{{route}}")],
        unit="s",
        w=12,
    )
    b.ts(
        "Requests / s by route",
        [("sum by (route) (rate(sip_api_requests_total[$__rate_interval]))", "{{route}}")],
        unit="reqps",
        w=12,
    )
    b.row("Protection and WebSocket")
    b.ts(
        "In flight / rate limited",
        [
            ("sum(sip_api_requests_in_flight)", "in flight"),
            ("sum(rate(sip_api_rate_limited_total[$__rate_interval]))", "rate limited/s"),
        ],
    )
    b.ts("WebSocket connections", [("sum(sip_api_websocket_connections)", "open")])
    b.ts(
        "WebSocket messages / s",
        [
            (
                "sum by (type) (rate(sip_api_websocket_messages_total[$__rate_interval]))",
                "{{type}}",
            ),
            (
                "sum(rate(sip_api_websocket_coalesced_total[$__rate_interval]))",
                "coalesced (dropped, superseded)",
            ),
        ],
        unit="ops",
    )
    return b


def model() -> Board:
    b = Board(
        "sip-model",
        "Model and monitoring",
        "Inference serving, prediction flow and the model monitor. The model has no demonstrated edge over the base rate; see docs/MONITORING.md.",
    )
    b.row("Serving")
    b.stat(
        "Served model",
        "sip_inference_model_info",
        w=6,
        options={"textMode": "name", "reduceOptions": {"calcs": ["lastNotNull"], "fields": ""}},
        description="Legend shows model and version",
    )
    b.ts(
        "Inference requests / s",
        [
            (
                "sum by (outcome) (rate(sip_inference_requests_total[$__rate_interval]))",
                "{{outcome}}",
            )
        ],
        unit="ops",
        w=9,
    )
    b.ts(
        "Model latency",
        [(hist("sip_inference_model_seconds", q), f"p{int(q * 100)}") for q in (0.5, 0.99)],
        unit="s",
        w=9,
    )
    b.row("Prediction pipeline")
    b.ts(
        "Bars by outcome / s",
        [("sum by (outcome) (rate(sip_pipeline_messages_total[$__rate_interval]))", "{{outcome}}")],
        unit="ops",
    )
    b.ts(
        "Predictions published / s",
        [
            (
                "sum by (model_version) (rate(sip_pipeline_predictions_total[$__rate_interval]))",
                "v{{model_version}}",
            )
        ],
        unit="ops",
    )
    b.ts(
        "Skipped predictions / s",
        [
            (
                "sum by (reason) (rate(sip_pipeline_predictions_skipped_total[$__rate_interval]))",
                "{{reason}}",
            )
        ],
        unit="ops",
    )
    b.ts(
        "Inference round trip (pipeline side)",
        [
            (hist("sip_pipeline_inference_request_seconds", q), f"p{int(q * 100)}")
            for q in (0.5, 0.99)
        ],
        unit="s",
    )
    b.ts(
        "Feature state resets (gaps)",
        [("sum(increase(sip_pipeline_feature_resets_total[5m]))", "last 5m")],
    )
    b.ts(
        "Model load failures",
        [("sum(increase(sip_inference_model_load_failures_total[1h]))", "last 1h")],
    )
    b.row("Monitoring")
    b.ts(
        "Status counts (0 ok, 1 warning, 2 alert)",
        [
            ("count(sip_monitor_status == 0) or vector(0)", "ok"),
            ("count(sip_monitor_status == 1) or vector(0)", "warning"),
            ("count(sip_monitor_status == 2) or vector(0)", "alert"),
        ],
        w=8,
    )
    b.ts(
        "Drift by input (PSI)",
        [('sip_monitor_value{report_type="data_drift",metric="psi"}', "{{feature}}")],
        w=8,
        description="PSI above about 0.25 is conventionally a large shift.",
    )
    b.ts(
        "Live log loss minus base-rate forecast",
        [('sip_monitor_value{metric="log_loss_minus_null"}', "log loss - null")],
        w=8,
        description="Above 0 means worse than always predicting the base rate.",
    )
    b.ts(
        "Outcomes resolved and monitor runs",
        [
            ("sum(increase(sip_monitor_outcomes_resolved_total[10m]))", "outcomes resolved (10m)"),
            ("sum by (result) (increase(sip_monitor_runs_total[10m]))", "runs {{result}} (10m)"),
        ],
        w=12,
    )
    return b


BOARDS = {"pipeline": pipeline, "kafka": kafka, "api": api, "model": model}


def render() -> dict[str, str]:
    return {f"{n}.json": json.dumps(f().build(), indent=2) + "\n" for n, f in BOARDS.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    stale = []
    for name, text in render().items():
        path = OUT / name
        if args.check:
            if not path.exists() or path.read_text() != text:
                stale.append(name)
        else:
            path.write_text(text)
    if stale:
        print(
            f"stale dashboards: {', '.join(stale)}; run scripts/gen_dashboards.py", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
