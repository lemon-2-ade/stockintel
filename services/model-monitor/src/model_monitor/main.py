"""Model monitor (``python -m model_monitor.main``): a single scheduled worker.

Every ``MONITOR_INTERVAL_S``:

1. resolve outcomes of predictions whose target time has passed;
2. run drift and performance checks over the last ``MONITOR_WINDOW_HOURS``;
3. write the results to ``monitoring_reports`` and expose them as metrics;
4. log every ``alert`` (Prometheus alert rules over these metrics: Phase 11).

``--once`` runs a single cycle and exits (cron, CI, tests).
"""

from __future__ import annotations

import argparse
import signal
import sys
import threading
from datetime import UTC, datetime, timedelta
from types import FrameType

from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import Engine

from model_monitor.checks import CheckConfig, Report, run_checks
from model_monitor.outcomes import resolve_outcomes
from model_monitor.store import MlflowProfileSource, ProfileSource, load_window, write_reports
from shared.config import LogSettings, PostgresSettings
from shared.observability.logs import configure_logging, get_logger

log = get_logger(__name__)

_STATUS_VALUE = {"ok": 0, "warning": 1, "alert": 2}


class MonitorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MONITOR_", extra="ignore")

    interval_s: float = Field(default=300.0, gt=0)
    window_hours: float = Field(default=24.0, gt=0)
    outcome_lookback_hours: float = Field(default=24.0 * 14, gt=0)
    max_rows: int = Field(default=200_000, ge=1)
    min_samples: int = Field(default=200, ge=10)
    min_outcomes: int = Field(default=100, ge=10)
    metrics_port: int = Field(default=8005, ge=1, le=65_535)


class MonitorMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = r = registry or CollectorRegistry()
        self.value = Gauge(
            "sip_monitor_value",
            "Latest value of each monitoring metric",
            ["report_type", "metric", "feature"],
            registry=r,
        )
        self.status = Gauge(
            "sip_monitor_status",
            "Latest status of each monitoring metric (0 ok, 1 warning, 2 alert)",
            ["report_type", "metric", "feature"],
            registry=r,
        )
        self.alerts = Counter(
            "sip_monitor_alerts_total", "Alert-level reports written", ["report_type"], registry=r
        )
        self.outcomes = Counter(
            "sip_monitor_outcomes_resolved_total", "Prediction outcomes resolved", registry=r
        )
        self.runs = Counter("sip_monitor_runs_total", "Monitor cycles", ["result"], registry=r)

    def publish(self, reports: list[Report]) -> None:
        for r in reports:
            labels = (r.report_type, r.metric_name, r.feature_name or "")
            self.value.labels(*labels).set(r.value)
            self.status.labels(*labels).set(_STATUS_VALUE[r.status])
            if r.status == "alert":
                self.alerts.labels(r.report_type).inc()


def run_cycle(
    engine: Engine,
    profiles: ProfileSource,
    settings: MonitorSettings,
    metrics: MonitorMetrics,
    *,
    now: datetime | None = None,
) -> list[Report]:
    now = now or datetime.now(UTC)
    resolved = resolve_outcomes(
        engine, now=now, lookback=timedelta(hours=settings.outcome_lookback_hours)
    )
    metrics.outcomes.inc(resolved)
    start = now - timedelta(hours=settings.window_hours)
    rows = load_window(engine, start, now, settings.max_rows)
    versions = {(r.model_name, r.model_version) for r in rows}
    references = {key: profiles.get(*key) for key in versions}
    reports = run_checks(rows, references, CheckConfig(settings.min_samples, settings.min_outcomes))
    write_reports(engine, reports, start, now)
    metrics.publish(reports)
    for r in reports:
        if r.status == "alert":
            log.warning(
                "monitor.alert",
                report_type=r.report_type,
                metric=r.metric_name,
                feature=r.feature_name,
                value=round(r.value, 4),
                model=f"{r.model_name}/{r.model_version}",
            )
    log.info(
        "monitor.cycle",
        outcomes_resolved=resolved,
        predictions_in_window=len(rows),
        reports=len(reports),
        alerts=sum(r.status == "alert" for r in reports),
    )
    return reports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    args = parser.parse_args(argv)
    log_settings = LogSettings()
    configure_logging("model-monitor", level=log_settings.level, fmt=log_settings.format)
    settings = MonitorSettings()
    metrics = MonitorMetrics()

    from shared.db.session import create_db_engine  # noqa: PLC0415

    engine = create_db_engine(PostgresSettings(), application_name="model-monitor")
    profiles = MlflowProfileSource()
    if args.once:
        run_cycle(engine, profiles, settings, metrics)
        return 0

    stop = threading.Event()

    def _request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("monitor.shutdown_requested", signal=signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    start_http_server(settings.metrics_port, registry=metrics.registry)
    while not stop.is_set():
        try:
            run_cycle(engine, profiles, settings, metrics)
            metrics.runs.labels("ok").inc()
        except Exception:  # one failed cycle (DB blip) must not kill the worker
            log.exception("monitor.cycle_failed")
            metrics.runs.labels("error").inc()
        stop.wait(settings.interval_s)
    engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
