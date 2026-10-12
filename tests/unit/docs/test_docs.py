"""Documentation hygiene: relative links resolve, and docs avoid stale placeholders."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DOCS = [
    ROOT / "README.md",
    *sorted((ROOT / "docs").rglob("*.md")),
    ROOT / "infrastructure/terraform/README.md",
]
LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)\)|!\[[^\]]*\]\(([^)\s]+)\)")


def _targets(text: str) -> list[str]:
    text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
    return [a or b for a, b in LINK.findall(text)]


def test_relative_links_and_images_resolve() -> None:
    broken: list[str] = []
    for doc in DOCS:
        for target in _targets(doc.read_text()):
            if re.match(r"^[a-z]+:", target) or target.startswith("#"):
                continue
            path = target.split("#", 1)[0]
            if path and not (doc.parent / path).resolve().exists():
                broken.append(f"{doc.relative_to(ROOT)} -> {target}")
    assert broken == []


def test_status_text_has_no_stale_phase_markers() -> None:
    text = (ROOT / "README.md").read_text()
    assert "(planned)" not in text
    assert "Phase 1-10" not in text
