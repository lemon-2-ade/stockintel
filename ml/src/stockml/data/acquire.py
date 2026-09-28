"""Download an immutable, checksum-verified raw snapshot of a dataset.

    python -m stockml.data.acquire                    # verify against the lock file
    python -m stockml.data.acquire --update-lock      # (re)record checksums deliberately

Guarantees
----------
* **Pinned**: files are fetched from an exact commit, never a branch.
* **Verified**: every file must match the SHA-256 in the committed lock file;
  a mismatch aborts and nothing is written.
* **Immutable**: files land in ``data/raw/<dataset>/<revision>/`` and are made
  read-only. A new revision is a new directory; old snapshots are never
  rewritten, so every downstream artefact can name the exact bytes it used.
* **Idempotent**: files already present with the right checksum are skipped.
* **Atomic**: written to a temp file and renamed, so an interrupted run never
  leaves a truncated file that looks valid.
"""

from __future__ import annotations

import argparse
import functools
import hashlib
import json
import os
import stat
import sys
import tempfile
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from shared.config import LogSettings
from shared.observability.logs import configure_logging, get_logger
from shared.schemas.base import utcnow
from shared.utils.retry import BackoffPolicy, retry_call
from stockml.data.catalog import DEFAULT_RAW_ROOT, DatasetSpec, LockFile

log = get_logger(__name__)

Fetcher = Callable[[str], bytes]
_USER_AGENT = "stockintel-platform-data-acquisition/0.1"
_TIMEOUT_S = 60
_MAX_BYTES = 200 * 1024 * 1024  # sanity cap per file


class ChecksumMismatchError(RuntimeError):
    """Downloaded bytes differ from the committed lock file."""


class TransientFetchError(RuntimeError):
    """Network/server error worth retrying (timeouts, 5xx, 429)."""


def http_fetch(url: str) -> bytes:
    """GET ``url``; transient failures raise :class:`TransientFetchError`."""
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})  # noqa: S310
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:  # noqa: S310
            body: bytes = response.read(_MAX_BYTES + 1)
    except urllib.error.HTTPError as exc:
        if exc.code == 429 or exc.code >= 500:
            raise TransientFetchError(f"HTTP {exc.code} for {url}") from exc
        raise
    except (urllib.error.URLError, TimeoutError) as exc:
        raise TransientFetchError(f"{exc} for {url}") from exc
    if len(body) > _MAX_BYTES:
        raise RuntimeError(f"{url} exceeds {_MAX_BYTES} bytes")
    return body


def huggingface_url(spec: DatasetSpec, remote_path: str) -> str:
    return f"https://huggingface.co/datasets/{spec.repo_id}/resolve/{spec.revision}/{remote_path}"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_readonly(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


@dataclass(frozen=True, slots=True)
class AcquisitionResult:
    snapshot_dir: Path
    downloaded: tuple[str, ...]
    skipped: tuple[str, ...]


def acquire(
    spec: DatasetSpec,
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    lock: LockFile,
    update_lock: bool = False,
    fetch: Fetcher = http_fetch,
    retry_policy: BackoffPolicy | None = None,
) -> AcquisitionResult:
    """Materialise ``spec`` under ``raw_root``. Mutates ``lock`` only if ``update_lock``."""
    if not update_lock:
        missing = [s for s in spec.symbols if s not in lock.files]
        if missing:
            raise ChecksumMismatchError(
                f"no locked checksum for {missing}; run with --update-lock to add them"
            )

    policy = retry_policy or BackoffPolicy(max_attempts=5, base_delay_s=1.0, max_delay_s=20.0)
    snapshot = spec.snapshot_dir(raw_root)
    downloaded: list[str] = []
    skipped: list[str] = []

    for symbol in spec.symbols:
        target = snapshot / f"{symbol}.csv"
        expected = lock.files.get(symbol, {}).get("sha256")
        if target.exists() and expected and sha256(target.read_bytes()) == expected:
            skipped.append(symbol)
            continue

        url = huggingface_url(spec, spec.remote_path(symbol))
        data = retry_call(
            functools.partial(fetch, url),
            policy=policy,
            retry_on=(TransientFetchError,),
            operation=f"download:{symbol}",
        )
        digest = sha256(data)
        if update_lock:
            lock.files[symbol] = {"sha256": digest, "bytes": len(data)}
        elif digest != expected:
            raise ChecksumMismatchError(
                f"{symbol}: sha256 {digest[:16]}... != locked {str(expected)[:16]}... "
                "(upstream changed or download corrupted); nothing was written"
            )
        if target.exists():  # present but stale/corrupt: replace the read-only file
            target.chmod(stat.S_IRUSR | stat.S_IWUSR)
        _write_readonly(target, data)
        downloaded.append(symbol)
        log.info("dataset.file_acquired", symbol=symbol, bytes=len(data), sha256=digest[:16])

    manifest = {
        "dataset": spec.name,
        "source": f"https://huggingface.co/datasets/{spec.repo_id}",
        "revision": spec.revision,
        "license": spec.license,
        "frequency": spec.frequency,
        "symbols": list(spec.symbols),
        "files": {s: lock.files[s] for s in spec.symbols},
        "acquired_at": utcnow().isoformat(),
    }
    manifest_path = snapshot / "_manifest.json"
    if manifest_path.exists():
        manifest_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    _write_readonly(manifest_path, (json.dumps(manifest, indent=2) + "\n").encode())
    return AcquisitionResult(snapshot, tuple(downloaded), tuple(skipped))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--dataset", default="us-equities-daily")
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument(
        "--update-lock",
        action="store_true",
        help="record checksums of what is downloaded (deliberate re-lock after a revision bump)",
    )
    args = parser.parse_args(argv)

    settings = LogSettings()
    configure_logging("data-acquire", level=settings.level, fmt="console")
    spec = DatasetSpec.load(args.dataset)
    lock = (
        LockFile(dataset=spec.name, revision=spec.revision)
        if args.update_lock
        else LockFile.read(spec.lock_path, spec)
    )
    try:
        result = acquire(spec, raw_root=args.raw_root, lock=lock, update_lock=args.update_lock)
    except ChecksumMismatchError as exc:
        log.error("dataset.checksum_mismatch", error=str(exc))
        return 2
    if args.update_lock:
        lock.write(spec.lock_path)
        log.info("dataset.lock_updated", path=str(spec.lock_path))
    log.info(
        "dataset.snapshot_ready",
        path=str(result.snapshot_dir),
        downloaded=len(result.downloaded),
        already_present=len(result.skipped),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
