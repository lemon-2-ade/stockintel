"""Dataset specifications (``ml/datasets/*.toml``) and their checksum lock files.

A spec says *what* to fetch (source, pinned revision, symbols). The lock file
(``<name>.lock.json``, committed) records the SHA-256 of every file so that a
fresh clone downloads byte-identical data or fails loudly.
"""

from __future__ import annotations

import json
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from shared.schemas.base import SYMBOL_PATTERN

REPO_ROOT = Path(__file__).resolve().parents[4]
DATASETS_DIR = REPO_ROOT / "ml" / "datasets"
DEFAULT_RAW_ROOT = REPO_ROOT / "data" / "raw"

_SYMBOL_RE = re.compile(SYMBOL_PATTERN)
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    name: str
    description: str
    provider: str
    repo_id: str
    revision: str
    license: str
    frequency: str
    path_template: str
    symbols: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.provider != "huggingface":
            raise ValueError(f"unsupported provider {self.provider!r}")
        if not _REVISION_RE.match(self.revision):
            raise ValueError("revision must be a full 40-char commit sha (branches are mutable)")
        bad = [s for s in self.symbols if not _SYMBOL_RE.match(s)]
        if bad:
            raise ValueError(f"invalid symbols: {bad}")
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("duplicate symbols")

    def remote_path(self, symbol: str) -> str:
        return self.path_template.format(shard=symbol[0], symbol=symbol)

    def snapshot_dir(self, raw_root: Path) -> Path:
        """``data/raw/<name>/<revision>/``: one immutable directory per snapshot."""
        return raw_root / self.name / self.revision

    @property
    def lock_path(self) -> Path:
        return DATASETS_DIR / f"{self.name}.lock.json"

    @classmethod
    def load(cls, name_or_path: str | Path) -> DatasetSpec:
        path = Path(name_or_path)
        if not path.suffix:
            path = DATASETS_DIR / f"{name_or_path}.toml"
        with path.open("rb") as fh:
            raw: dict[str, Any] = tomllib.load(fh)
        raw["symbols"] = tuple(raw["symbols"])
        return cls(**raw)


@dataclass(slots=True)
class LockFile:
    """``{symbol: {"sha256": ..., "bytes": ...}}`` for one dataset revision."""

    dataset: str
    revision: str
    files: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def read(cls, path: Path, spec: DatasetSpec) -> LockFile:
        if not path.exists():
            return cls(dataset=spec.name, revision=spec.revision)
        data = json.loads(path.read_text())
        lock = cls(dataset=data["dataset"], revision=data["revision"], files=data["files"])
        if lock.revision != spec.revision:
            raise ValueError(
                f"lock file is for revision {lock.revision[:12]} but spec pins "
                f"{spec.revision[:12]}; run with --update-lock to re-lock deliberately"
            )
        return lock

    def write(self, path: Path) -> None:
        payload = {
            "dataset": self.dataset,
            "revision": self.revision,
            "files": dict(sorted(self.files.items())),
        }
        path.write_text(json.dumps(payload, indent=2) + "\n")
