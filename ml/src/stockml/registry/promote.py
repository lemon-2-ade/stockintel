"""Explicit, audited model promotion.

    python -m stockml.registry.promote --version 3 --to champion \\
        --reason "first deployment to exercise the serving path" [--override-gates]
    python -m stockml.registry.promote --status

Promoting version V to a stage S:

1. reads V's current stage (its alias, or the ``archived`` tag);
2. checks the transition is allowed and evaluates the gates (stages.py);
3. if another version holds the ``challenger`` or ``champion`` alias, it is
   archived in the same step (one of each at a time);
4. moves the alias, updates the ``stage`` tags, and writes one audit row per
   version that changed, all through ``AuditSink.record``.

Refusals change nothing. The command prints what it did; it never prompts.
"""

from __future__ import annotations

import argparse
import getpass
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from shared.config import LogSettings, PostgresSettings
from shared.features import FEATURE_SET_VERSION
from shared.observability.logs import configure_logging, get_logger
from stockml.registry.audit import AuditEntry, AuditSink, JsonlAuditSink, PostgresAuditSink
from stockml.registry.stages import (
    ALIAS_STAGES,
    STAGE_TAG,
    PromotionError,
    Stage,
    blocking,
    check_transition,
    evaluate_gates,
)

log = get_logger(__name__)

DEFAULT_MODEL_NAME = "stockintel-direction-h5"


@dataclass(frozen=True, slots=True)
class PromotionResult:
    entries: list[AuditEntry]

    def summary(self) -> str:
        return "; ".join(
            f"v{e.model_version}: {e.from_stage or 'new'} -> {e.to_stage}" for e in self.entries
        )


def current_stage(client: MlflowClient, name: str, version: str) -> Stage | None:
    mv = client.get_model_version(name, version)
    for alias in mv.aliases:
        if alias in ALIAS_STAGES:
            return Stage(alias)
    tag = mv.tags.get(STAGE_TAG)
    return Stage(tag) if tag in {s.value for s in Stage} else None


def alias_holder(client: MlflowClient, name: str, stage: Stage) -> str | None:
    try:
        return str(client.get_model_version_by_alias(name, stage.value).version)
    except MlflowException:
        return None


def promote(
    client: MlflowClient,
    audit: AuditSink,
    *,
    name: str,
    version: str,
    to: Stage,
    reason: str,
    actor: str,
    override_gates: bool = False,
    serving_feature_set_version: str = FEATURE_SET_VERSION,
) -> PromotionResult:
    if not reason.strip():
        raise PromotionError("a reason is required")
    try:
        mv = client.get_model_version(name, version)
    except MlflowException as exc:
        raise PromotionError(f"{name} version {version} not found") from exc
    current = current_stage(client, name, version)
    check_transition(current, to)

    gates = evaluate_gates(
        dict(mv.tags), serving_feature_set_version=serving_feature_set_version, target=to
    )
    stopped = blocking(gates, override=override_gates)
    if stopped:
        details = "; ".join(
            f"{g.name} ({'hard' if g.hard else 'soft'}): {g.detail}" for g in stopped
        )
        raise PromotionError(f"gates failed: {details}")
    overridden = [g.name for g in gates if not g.passed]
    metrics: dict[str, Any] = {
        "gates": {g.name: g.as_dict() for g in gates},
        "overridden_gates": overridden,
        "evaluation": {k: v for k, v in mv.tags.items() if k.startswith("eval.")},
    }
    entries = [AuditEntry(name, version, current, to.value, reason, actor, metrics)]

    # One champion and one challenger at a time: the previous holder is archived.
    displaced: str | None = None
    if to in (Stage.CHALLENGER, Stage.CHAMPION):
        displaced = alias_holder(client, name, to)
        if displaced == version:
            displaced = None
        if displaced is not None:
            entries.append(
                AuditEntry(
                    name,
                    displaced,
                    to.value,
                    Stage.ARCHIVED.value,
                    f"replaced by version {version}: {reason}",
                    actor,
                )
            )

    def apply() -> None:
        if current is not None and current.value in mv.aliases:
            client.delete_registered_model_alias(name, current.value)
        if to in ALIAS_STAGES:
            # Moving an alias detaches it from the previous holder atomically.
            client.set_registered_model_alias(name, to.value, version)
        client.set_model_version_tag(name, version, STAGE_TAG, to.value)
        if displaced is not None:
            client.set_model_version_tag(name, displaced, STAGE_TAG, Stage.ARCHIVED.value)

    try:
        audit.record(entries, apply)
    except Exception:
        log.exception("promotion.failed", model=name, version=version, to=to.value)
        raise
    result = PromotionResult(entries)
    log.info("promotion.done", model=name, changes=result.summary(), overridden=overridden)
    return result


def status(client: MlflowClient, name: str) -> list[dict[str, Any]]:
    rows = []
    for found in client.search_model_versions(f"name='{name}'"):
        # Search results omit aliases on some backends; fetch each version.
        mv = client.get_model_version(name, found.version)
        rows.append(
            {
                "version": mv.version,
                "aliases": sorted(mv.aliases),
                "stage": mv.tags.get(STAGE_TAG),
                "model_type": mv.tags.get("model_type"),
                "feature_set_version": mv.tags.get("feature_set_version"),
                "test_log_loss": mv.tags.get("eval.test_log_loss"),
                "test_null_log_loss": mv.tags.get("eval.test_null_log_loss"),
            }
        )
    return sorted(rows, key=lambda r: int(r["version"]))


def make_audit_sink(audit_file: Path | None) -> AuditSink:
    if audit_file is not None:
        return JsonlAuditSink(audit_file)
    from shared.db.session import create_db_engine  # noqa: PLC0415

    return PostgresAuditSink(create_db_engine(PostgresSettings(), application_name="promote"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Move a model version between stages")
    parser.add_argument("--model", default=DEFAULT_MODEL_NAME)
    parser.add_argument("--status", action="store_true", help="list versions and stages")
    parser.add_argument("--version")
    parser.add_argument("--to", choices=[s.value for s in Stage])
    parser.add_argument("--reason", default="")
    parser.add_argument("--actor", default=None, help="defaults to the OS user")
    parser.add_argument(
        "--override-gates",
        action="store_true",
        help="promote despite failed soft gates (recorded in the audit log)",
    )
    parser.add_argument(
        "--audit-file",
        type=Path,
        default=None,
        help="append the audit log to this JSONL file instead of PostgreSQL",
    )
    args = parser.parse_args(argv)
    configure_logging("promote", level=LogSettings().level, fmt="console")
    client = MlflowClient()

    if args.status:
        print(json.dumps(status(client, args.model), indent=2))
        return 0
    if not args.version or not args.to:
        parser.error("--version and --to are required (or use --status)")
    try:
        result = promote(
            client,
            make_audit_sink(args.audit_file),
            name=args.model,
            version=args.version,
            to=Stage(args.to),
            reason=args.reason,
            actor=args.actor or getpass.getuser(),
            override_gates=args.override_gates,
        )
    except PromotionError as exc:
        log.error("promotion.refused", reason=str(exc))
        return 2
    print(result.summary())
    return 0


if __name__ == "__main__":
    sys.exit(main())
