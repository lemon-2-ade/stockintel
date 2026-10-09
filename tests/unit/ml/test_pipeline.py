"""End-to-end raw -> validated -> cleaned run on a tiny on-disk snapshot."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from ohlcv_fixtures import NOW, frame

from stockml.data.acquire import sha256
from stockml.data.catalog import DatasetSpec, LockFile
from stockml.data.pipeline import run

REV = "c" * 40


@pytest.fixture
def snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[DatasetSpec, Path]:
    raw_root = tmp_path / "raw"
    spec = DatasetSpec(
        name="tiny",
        description="",
        provider="huggingface",
        repo_id="org/repo",
        revision=REV,
        license="CC0-1.0",
        frequency="1d",
        path_template="{shard}/{symbol}.csv",
        symbols=("AAA", "BBB"),
    )
    snap = spec.snapshot_dir(raw_root)
    snap.mkdir(parents=True)
    bad = frame(90, seed=1)
    bad.loc[5, "close"] = ""  # one unusable row
    frame(90, seed=0).to_csv(snap / "AAA.csv", index=False)
    bad.to_csv(snap / "BBB.csv", index=False)

    lock = LockFile(
        dataset="tiny",
        revision=REV,
        files={s: {"sha256": sha256((snap / f"{s}.csv").read_bytes())} for s in spec.symbols},
    )
    lock_path = tmp_path / "tiny.lock.json"
    lock.write(lock_path)
    monkeypatch.setattr(DatasetSpec, "lock_path", property(lambda _: lock_path))
    return spec, raw_root


def test_pipeline_writes_all_stages_and_is_reproducible(
    snapshot: tuple[DatasetSpec, Path], tmp_path: Path
) -> None:
    spec, raw_root = snapshot
    processed, md = tmp_path / "processed", tmp_path / "REPORT.md"
    report = run(spec, raw_root=raw_root, processed_root=processed, report_md=md, now=NOW)

    out = processed / "tiny" / REV
    assert report["totals"] == {
        "raw_rows": 180,
        "removed_rows": 1,
        "cleaned_rows": 179,
        "symbols": 2,
    }
    cleaned = pd.read_parquet(out / "cleaned.parquet")
    validated = pd.read_parquet(out / "validated.parquet")
    assert len(cleaned) == 179
    assert len(validated) == 180
    removed = pd.read_csv(out / "removed_rows.csv")
    assert removed[["symbol", "row", "codes"]].values.tolist() == [["BBB", 7, "E001"]]

    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["revision"] == REV
    assert manifest["rows"]["cleaned_rows"] == 179
    assert "# Data Quality Report" in md.read_text()
    assert "| BBB | 7 | E001 | missing_value |" in md.read_text()

    first = manifest["outputs"]["cleaned.parquet"]
    run(spec, raw_root=raw_root, processed_root=processed, report_md=None, now=NOW)
    again = json.loads((out / "manifest.json").read_text())["outputs"]["cleaned.parquet"]
    assert again == first, "same snapshot + config -> byte-identical cleaned data"


def test_pipeline_refuses_tampered_raw_data(
    snapshot: tuple[DatasetSpec, Path], tmp_path: Path
) -> None:
    spec, raw_root = snapshot
    path = spec.snapshot_dir(raw_root) / "AAA.csv"
    path.write_text(path.read_text().replace("1", "2", 1))
    with pytest.raises(ValueError, match="lock file"):
        run(spec, raw_root=raw_root, processed_root=tmp_path / "p", report_md=None, now=NOW)
