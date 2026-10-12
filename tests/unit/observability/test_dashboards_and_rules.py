"""Dashboards and alert rules may only reference metrics the code really exports."""

from __future__ import annotations

import importlib.util
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[3]
DASHBOARDS = ROOT / "infrastructure" / "grafana" / "dashboards"
RULES = ROOT / "infrastructure" / "prometheus" / "rules" / "alerts.yml"
SUFFIXES = ("_bucket", "_sum", "_count", "_total", "_created")


def _defined_metrics() -> set[str]:
    names: set[str] = set()
    for path in ROOT.glob("**/*.py"):
        if any(p in path.parts for p in (".venv", "node_modules", "tests", ".git")):
            continue
        names |= set(re.findall(r'"(sip_[a-z0-9_]+)"', path.read_text()))
    return names


def _referenced(expr: str) -> set[str]:
    return set(re.findall(r"\b(sip_[a-z0-9_]+)", expr))


def _base(name: str) -> set[str]:
    out = {name}
    for s in SUFFIXES:
        if name.endswith(s):
            out.add(name.removesuffix(s))
            out.add(name.removesuffix(s) + "_total" if s != "_total" else name)
    return out


def _check(exprs: list[str]) -> list[str]:
    defined = _defined_metrics()
    # prometheus_client appends _total to counters declared without it
    defined |= {d.removesuffix("_total") for d in defined}
    return sorted(
        {
            m
            for e in exprs
            for m in _referenced(e)
            if not (_base(m) & defined or any(b + "_total" in defined for b in _base(m)))
        }
    )


def test_dashboard_metrics_exist() -> None:
    exprs = [
        t["expr"]
        for f in DASHBOARDS.glob("*.json")
        for p in json.loads(f.read_text())["panels"]
        for t in p.get("targets", [])
    ]
    assert exprs
    assert _check(exprs) == []


def test_alert_metrics_exist() -> None:
    doc = yaml.safe_load(RULES.read_text())
    exprs = [r["expr"] for g in doc["groups"] for r in g["rules"]]
    assert len(exprs) >= 15
    assert _check(exprs) == []


def test_dashboards_are_valid_and_unique() -> None:
    uids = set()
    for f in DASHBOARDS.glob("*.json"):
        d = json.loads(f.read_text())
        assert d["uid"] not in uids
        uids.add(d["uid"])
        ids = [p["id"] for p in d["panels"]]
        assert len(ids) == len(set(ids))
        assert all(p["gridPos"]["x"] + p["gridPos"]["w"] <= 24 for p in d["panels"])


def test_alerts_have_severity_and_annotations() -> None:
    doc = yaml.safe_load(RULES.read_text())
    for g in doc["groups"]:
        for r in g["rules"]:
            assert r["labels"]["severity"] in {"info", "warning", "critical"}, r["alert"]
            assert {"summary", "description"} <= set(r["annotations"]), r["alert"]


def test_committed_dashboards_match_generator() -> None:
    spec = importlib.util.spec_from_file_location("gen", ROOT / "scripts" / "gen_dashboards.py")
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for name, text in mod.render().items():
        assert (DASHBOARDS / name).read_text() == text, f"{name} is stale"


def test_scrape_config_covers_every_service() -> None:
    cfg = yaml.safe_load((ROOT / "infrastructure/prometheus/prometheus.yml").read_text())
    jobs = {j["job_name"] for j in cfg["scrape_configs"]}
    assert {
        "market-producer",
        "stream-processor",
        "sinks",
        "api",
        "inference",
        "prediction-pipeline",
        "model-monitor",
        "kafka-lag-exporter",
    } <= jobs


PROMTOOL = shutil.which("promtool")


@pytest.mark.skipif(PROMTOOL is None, reason="promtool not installed")
def test_promtool_rules() -> None:
    assert PROMTOOL is not None
    tests = ROOT / "infrastructure/prometheus/tests/alerts_test.yml"
    out = subprocess.run(  # noqa: S603
        [PROMTOOL, "test", "rules", str(tests)], capture_output=True, text=True, check=False
    )
    assert out.returncode == 0, out.stdout + out.stderr


def test_performance_doc_matches_raw_results() -> None:
    out = subprocess.run(  # noqa: S603
        [sys.executable, str(ROOT / "scripts" / "summarize_benchmarks.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0, out.stderr
