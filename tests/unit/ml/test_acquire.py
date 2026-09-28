from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from shared.utils.retry import BackoffPolicy
from stockml.data.acquire import (
    ChecksumMismatchError,
    TransientFetchError,
    acquire,
    huggingface_url,
    sha256,
)
from stockml.data.catalog import DatasetSpec, LockFile

REV = "a" * 40
FAST = BackoffPolicy(max_attempts=3, base_delay_s=0.0, max_delay_s=0.0)
FILES = {"AAPL": b"timestamp,open\n1,2\n", "MSFT": b"timestamp,open\n1,3\n"}


def make_spec(**overrides: object) -> DatasetSpec:
    kwargs: dict[str, object] = {
        "name": "test-ds",
        "description": "",
        "provider": "huggingface",
        "repo_id": "org/repo",
        "revision": REV,
        "license": "CC0-1.0",
        "frequency": "1d",
        "path_template": "{shard}/{symbol}.csv",
        "symbols": ("AAPL", "MSFT"),
    } | overrides
    return DatasetSpec(**kwargs)  # type: ignore[arg-type]


class FakeRemote:
    def __init__(self, files: dict[str, bytes], transient_failures: int = 0) -> None:
        self.files = files
        self.calls: list[str] = []
        self.transient_failures = transient_failures

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        if self.transient_failures:
            self.transient_failures -= 1
            raise TransientFetchError("503")
        symbol = url.rsplit("/", 1)[-1].removesuffix(".csv")
        return self.files[symbol]


def locked(spec: DatasetSpec, files: dict[str, bytes]) -> LockFile:
    return LockFile(
        dataset=spec.name,
        revision=spec.revision,
        files={s: {"sha256": sha256(b), "bytes": len(b)} for s, b in files.items()},
    )


def test_urls_are_pinned_to_the_revision() -> None:
    spec = make_spec()
    assert huggingface_url(spec, spec.remote_path("MSFT")) == (
        f"https://huggingface.co/datasets/org/repo/resolve/{REV}/M/MSFT.csv"
    )


def test_downloads_verified_readonly_snapshot(tmp_path: Path) -> None:
    spec = make_spec()
    remote = FakeRemote(FILES)
    result = acquire(spec, raw_root=tmp_path, lock=locked(spec, FILES), fetch=remote)

    assert result.snapshot_dir == tmp_path / "test-ds" / REV
    assert set(result.downloaded) == {"AAPL", "MSFT"}
    target = result.snapshot_dir / "AAPL.csv"
    assert target.read_bytes() == FILES["AAPL"]
    assert not target.stat().st_mode & stat.S_IWUSR, "raw files must be read-only"
    manifest = json.loads((result.snapshot_dir / "_manifest.json").read_text())
    assert manifest["revision"] == REV
    assert manifest["license"] == "CC0-1.0"
    assert not list(result.snapshot_dir.glob(".*.tmp")), "no temp files left behind"


def test_second_run_is_a_noop(tmp_path: Path) -> None:
    spec = make_spec()
    lock = locked(spec, FILES)
    acquire(spec, raw_root=tmp_path, lock=lock, fetch=FakeRemote(FILES))
    remote = FakeRemote(FILES)
    result = acquire(spec, raw_root=tmp_path, lock=lock, fetch=remote)
    assert remote.calls == []
    assert set(result.skipped) == {"AAPL", "MSFT"}


def test_checksum_mismatch_aborts_without_writing(tmp_path: Path) -> None:
    spec = make_spec()
    tampered = FILES | {"MSFT": b"tampered"}
    with pytest.raises(ChecksumMismatchError, match="MSFT"):
        acquire(spec, raw_root=tmp_path, lock=locked(spec, FILES), fetch=FakeRemote(tampered))
    assert not (tmp_path / "test-ds" / REV / "MSFT.csv").exists()


def test_corrupted_local_file_is_replaced(tmp_path: Path) -> None:
    spec = make_spec()
    lock = locked(spec, FILES)
    result = acquire(spec, raw_root=tmp_path, lock=lock, fetch=FakeRemote(FILES))
    target = result.snapshot_dir / "AAPL.csv"
    target.chmod(0o644)
    target.write_bytes(b"corrupt")
    again = acquire(spec, raw_root=tmp_path, lock=lock, fetch=FakeRemote(FILES))
    assert again.downloaded == ("AAPL",)
    assert target.read_bytes() == FILES["AAPL"]


def test_unlocked_symbols_require_explicit_relock(tmp_path: Path) -> None:
    spec = make_spec()
    partial = LockFile(dataset=spec.name, revision=REV)
    with pytest.raises(ChecksumMismatchError, match="update-lock"):
        acquire(spec, raw_root=tmp_path, lock=partial, fetch=FakeRemote(FILES))


def test_update_lock_records_checksums(tmp_path: Path) -> None:
    spec = make_spec()
    lock = LockFile(dataset=spec.name, revision=REV)
    acquire(spec, raw_root=tmp_path, lock=lock, update_lock=True, fetch=FakeRemote(FILES))
    assert lock.files["AAPL"] == {"sha256": sha256(FILES["AAPL"]), "bytes": len(FILES["AAPL"])}
    path = tmp_path / "lock.json"
    lock.write(path)
    assert LockFile.read(path, spec).files == lock.files


def test_transient_errors_are_retried(tmp_path: Path) -> None:
    spec = make_spec(symbols=("AAPL",))
    remote = FakeRemote(FILES, transient_failures=2)
    acquire(spec, raw_root=tmp_path, lock=locked(spec, FILES), fetch=remote, retry_policy=FAST)
    assert len(remote.calls) == 3


def test_lock_for_another_revision_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "lock.json"
    LockFile(dataset="test-ds", revision="b" * 40).write(path)
    with pytest.raises(ValueError, match="revision"):
        LockFile.read(path, make_spec())


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"revision": "main"}, "commit sha"),
        ({"symbols": ("aapl",)}, "invalid symbols"),
        ({"symbols": ("AAPL", "AAPL")}, "duplicate"),
        ({"provider": "ftp"}, "provider"),
    ],
)
def test_spec_validation(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        make_spec(**overrides)


def test_committed_default_spec_is_valid_and_locked() -> None:
    spec = DatasetSpec.load("us-equities-daily")
    lock = LockFile.read(spec.lock_path, spec)
    assert set(lock.files) == set(spec.symbols)
    assert all(len(entry["sha256"]) == 64 for entry in lock.files.values())
