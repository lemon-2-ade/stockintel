"""Offline data pipeline: raw -> validated -> cleaned (+ quality report).

    python -m stockml.data.pipeline        # or: make data-quality

Inputs are the immutable, checksum-verified raw snapshot. Outputs go to
``data/processed/<dataset>/<revision>/``:

=====================  ==========================================================
``validated.parquet``  every raw row: raw strings, parsed values, rule codes
``cleaned.parquet``    canonical training input (errors removed, warnings flagged)
``removed_rows.csv``   audit log: what was removed and why
``quality_report.json`` machine-readable report (also rendered to markdown)
``manifest.json``      inputs (revision + checksums), config, versions, output hashes
=====================  ==========================================================

Re-running on the same snapshot with the same pipeline version and config
produces the same cleaned data; the manifest makes that checkable.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from shared.config import LogSettings
from shared.observability.logs import configure_logging, get_logger
from stockml.data.catalog import DEFAULT_RAW_ROOT, REPO_ROOT, DatasetSpec, LockFile
from stockml.data.cleaning import CleaningPolicy, CleaningResult, clean
from stockml.data.quality import RULES, QualityConfig, ValidationResult, read_raw_lenient, validate

log = get_logger(__name__)

PIPELINE_VERSION = "1.0.0"
DEFAULT_PROCESSED_ROOT = REPO_ROOT / "data" / "processed"
DEFAULT_REPORT_MD = REPO_ROOT / "docs" / "DATA_QUALITY.md"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_snapshot(spec: DatasetSpec, raw_root: Path, lock: LockFile) -> dict[str, pd.DataFrame]:
    snapshot = spec.snapshot_dir(raw_root)
    frames: dict[str, pd.DataFrame] = {}
    for symbol in spec.symbols:
        path = snapshot / f"{symbol}.csv"
        if not path.exists():
            raise FileNotFoundError(f"{path} missing; run `make data` first")
        expected = lock.files.get(symbol, {}).get("sha256")
        if expected and _sha256(path) != expected:
            raise ValueError(f"{path} does not match the lock file; re-run `make data`")
        frames[symbol] = read_raw_lenient(path, symbol)
    return frames


def build_report(
    spec: DatasetSpec,
    validation: ValidationResult,
    cleaning: CleaningResult,
    cfg: QualityConfig,
) -> dict[str, Any]:
    frame, cleaned = validation.frame, cleaning.cleaned
    counts = validation.counts()
    per_rule = counts.groupby("code")["rows"].sum().to_dict() if not counts.empty else {}

    extremes = cleaned[cleaned["extreme_class"].notna()].merge(
        frame[["symbol", "timestamp", "log_return", "robust_z"]], on=["symbol", "timestamp"]
    )
    per_symbol = []
    for symbol, group in cleaned.groupby("symbol"):
        raw_rows = int((frame["symbol"] == symbol).sum())
        per_symbol.append(
            {
                "symbol": symbol,
                "raw_rows": raw_rows,
                "removed": raw_rows - len(group),
                "cleaned_rows": len(group),
                "first_session": group["session_date"].min().date().isoformat(),
                "last_session": group["session_date"].max().date().isoformat(),
                "missing_sessions": len(validation.gaps.get(str(symbol), [])),
                "extreme_moves": int(group["extreme_class"].notna().sum()),
                "zero_volume": int(group["flag_zero_volume"].sum()),
                "irregular_time": int(group["flag_irregular_time"].sum()),
            }
        )
    by_class = extremes["extreme_class"].value_counts().to_dict()
    return {
        "dataset": spec.name,
        "revision": spec.revision,
        "pipeline_version": PIPELINE_VERSION,
        "config": dataclasses.asdict(cfg),
        "totals": {
            "raw_rows": len(frame),
            "removed_rows": len(cleaning.removed),
            "cleaned_rows": len(cleaned),
            "symbols": int(cleaned["symbol"].nunique()),
        },
        "calendar": {
            "sessions": len(validation.calendar),
            "first": validation.calendar.min().date().isoformat(),
            "last": validation.calendar.max().date().isoformat(),
        },
        "rules": [
            {
                "code": rule.code,
                "name": rule.name,
                "severity": rule.severity.value,
                "action": rule.action,
                "rows": int(per_rule.get(rule.code, 0)),
            }
            for rule in RULES.values()
        ],
        "per_symbol": per_symbol,
        "extreme_moves_by_class": {str(k): int(v) for k, v in by_class.items()},
        "extreme_moves": [
            {
                "symbol": str(row["symbol"]),
                "date": pd.Timestamp(row["session_date"]).date().isoformat(),
                "return_pct": round(float(row["log_return"]) * 100, 2),
                "robust_z": round(float(row["robust_z"]), 1),
                "class": str(row["extreme_class"]),
            }
            for row in extremes.sort_values(["session_date", "symbol"]).to_dict("records")
        ],
        "gaps": validation.gaps,
        "removed": cleaning.removed.astype(str).to_dict(orient="records"),
    }


def render_markdown(report: dict[str, Any]) -> str:
    t = report["totals"]
    lines = [
        "# Data Quality Report",
        "",
        "<!-- Generated by `make data-quality` (stockml.data.pipeline). Do not edit by hand. -->",
        "",
        f"Dataset `{report['dataset']}` at revision `{report['revision'][:12]}`, "
        f"pipeline {report['pipeline_version']}.",
        "",
        f"- Raw rows: **{t['raw_rows']:,}**; removed: **{t['removed_rows']:,}**; "
        f"cleaned: **{t['cleaned_rows']:,}** across {t['symbols']} symbols.",
        f"- Consensus trading calendar: {report['calendar']['sessions']:,} sessions, "
        f"{report['calendar']['first']} to {report['calendar']['last']}.",
        "",
        "## Rules",
        "",
        "| Code | Rule | Severity | Action | Rows |",
        "| --- | --- | --- | --- | ---: |",
        *(
            f"| {r['code']} | `{r['name']}` | {r['severity']} | {r['action']} | {r['rows']:,} |"
            for r in report["rules"]
        ),
        "",
        "## Per symbol",
        "",
        "| Symbol | Raw | Removed | Cleaned | First | Last | Missing sessions | Extreme moves |",
        "| --- | ---: | ---: | ---: | --- | --- | ---: | ---: |",
        *(
            f"| {s['symbol']} | {s['raw_rows']:,} | {s['removed']} | {s['cleaned_rows']:,} | "
            f"{s['first_session']} | {s['last_session']} | {s['missing_sessions']} | "
            f"{s['extreme_moves']} |"
            for s in report["per_symbol"]
        ),
        "",
        "## Extreme moves",
        "",
        "Flagged (kept) moves by class: "
        + (
            ", ".join(f"{k} {v}" for k, v in sorted(report["extreme_moves_by_class"].items()))
            or "none"
        )
        + ".",
        "",
        "| Date | Symbol | Return | Robust z | Class |",
        "| --- | --- | ---: | ---: | --- |",
        *(
            f"| {e['date']} | {e['symbol']} | {e['return_pct']:+.2f}% | {e['robust_z']:+.1f} "
            f"| {e['class']} |"
            for e in report["extreme_moves"]
        ),
        "",
    ]
    if report["gaps"]:
        lines += ["## Missing sessions", ""]
        lines += [f"- **{s}**: {', '.join(d)}" for s, d in sorted(report["gaps"].items())]
        lines.append("")
    if report["removed"]:
        lines += [
            "## Removed rows",
            "",
            "| Symbol | Line | Codes | Reason |",
            "| --- | ---: | --- | --- |",
        ]
        lines += [
            f"| {r['symbol']} | {r['row']} | {r['codes']} | {r['reason']} |"
            for r in report["removed"]
        ]
        lines.append("")
    return "\n".join(lines)


def run(
    spec: DatasetSpec,
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    processed_root: Path = DEFAULT_PROCESSED_ROOT,
    report_md: Path | None = DEFAULT_REPORT_MD,
    config: QualityConfig | None = None,
    policy: CleaningPolicy | None = None,
    now: pd.Timestamp | None = None,
) -> dict[str, Any]:
    cfg = config or QualityConfig()
    lock = LockFile.read(spec.lock_path, spec)
    validation = validate(load_snapshot(spec, raw_root, lock), config=cfg, now=now)
    cleaning = clean(validation, policy)
    report = build_report(spec, validation, cleaning, cfg)

    out = processed_root / spec.name / spec.revision
    out.mkdir(parents=True, exist_ok=True)
    validated = validation.frame.assign(issues=validation.frame["issues"].map(";".join))
    validated.to_parquet(out / "validated.parquet", index=False)
    cleaning.cleaned.to_parquet(out / "cleaned.parquet", index=False)
    cleaning.removed.to_csv(out / "removed_rows.csv", index=False)
    (out / "quality_report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")

    manifest = {
        "dataset": spec.name,
        "revision": spec.revision,
        "pipeline_version": PIPELINE_VERSION,
        "config": dataclasses.asdict(cfg),
        "policy": dataclasses.asdict(policy or CleaningPolicy()),
        "inputs": {s: lock.files[s]["sha256"] for s in spec.symbols},
        "outputs": {
            name: _sha256(out / name)
            for name in ("cleaned.parquet", "removed_rows.csv", "quality_report.json")
        },
        "rows": report["totals"],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if report_md is not None:
        report_md.write_text(render_markdown(report))
    log.info("data_quality.done", output=str(out), **report["totals"])
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate and clean a raw dataset snapshot")
    parser.add_argument("--dataset", default="us-equities-daily")
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--processed-root", type=Path, default=DEFAULT_PROCESSED_ROOT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument("--drop-suspect-reversals", action="store_true")
    args = parser.parse_args(argv)

    configure_logging("data-quality", level=LogSettings().level, fmt="console")
    run(
        DatasetSpec.load(args.dataset),
        raw_root=args.raw_root,
        processed_root=args.processed_root,
        report_md=args.report,
        policy=CleaningPolicy(drop_suspect_reversals=args.drop_suspect_reversals),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
