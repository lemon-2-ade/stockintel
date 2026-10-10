"""Model lifecycle: stages, allowed transitions and promotion gates.

Stages are MLflow **aliases** on versions of one registered model
(``candidate``, ``challenger``, ``champion``); ``archived`` is a tag, because
an alias points at exactly one version and there can be many archived ones.
The inference service serves whatever ``@champion`` points at.

    training ──> candidate ──> challenger ──> champion ──> archived
                     │                           ▲
                     └───────────────────────────┘  (direct, still gated)

Nothing moves automatically: training registers a ``candidate`` and stops.
Every transition is an explicit command with a reason and an actor, recorded
in the audit log (``model_deployments`` table) and on the version's tags.

Gates are evaluated on every promotion and stored with the audit entry:

* **hard** gates can never be overridden (a model whose features the serving
  code cannot compute must not serve);
* **soft** gates encode "is this model any good"; they can be overridden with
  an explicit flag, and the override itself is recorded. That is the honest
  option for this project, whose best model does not beat the base-rate
  forecast: it can be deployed to exercise the platform, but the record says
  so.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final


class Stage(StrEnum):
    CANDIDATE = "candidate"
    CHALLENGER = "challenger"
    CHAMPION = "champion"
    ARCHIVED = "archived"


ALIAS_STAGES: Final = (Stage.CANDIDATE, Stage.CHALLENGER, Stage.CHAMPION)

ALLOWED: Final[dict[Stage | None, frozenset[Stage]]] = {
    None: frozenset({Stage.CANDIDATE}),
    Stage.CANDIDATE: frozenset({Stage.CHALLENGER, Stage.CHAMPION, Stage.ARCHIVED}),
    Stage.CHALLENGER: frozenset({Stage.CHAMPION, Stage.ARCHIVED}),
    Stage.CHAMPION: frozenset({Stage.ARCHIVED}),
    Stage.ARCHIVED: frozenset(),
}
"""Archived versions are never revived: retrain or re-register instead, so a
version's history reads in one direction."""

STAGE_TAG: Final = "stage"

EVAL_KEYS: Final = ("wf_log_loss", "wf_null_log_loss", "test_log_loss", "test_null_log_loss")
"""Evaluation tags written at registration: mean log loss on the untuned
walk-forward folds and on the held-out test, for the model and for the
constant base-rate forecast."""

MIN_LOG_LOSS_GAIN: Final = 0.001
"""Required log-loss improvement over the base-rate forecast, on *both* the
untuned walk-forward folds and the test set. Roughly 0.15% of the null log
loss: small enough for a real but weak weekly signal to pass, large enough
that a difference in the fourth decimal (noise at ~2,000 effective weekly
observations) does not."""


class PromotionError(Exception):
    """A transition was refused. Nothing was changed."""


@dataclass(frozen=True, slots=True)
class GateResult:
    name: str
    passed: bool
    hard: bool
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "hard": self.hard, "detail": self.detail}


def check_transition(current: Stage | None, target: Stage) -> None:
    if target not in ALLOWED[current]:
        allowed = ", ".join(sorted(ALLOWED[current])) or "none"
        raise PromotionError(
            f"cannot move from {current or 'unregistered'} to {target} (allowed: {allowed})"
        )


def evaluate_gates(
    tags: dict[str, str], *, serving_feature_set_version: str, target: Stage
) -> list[GateResult]:
    """Gates for moving a version (described by its MLflow tags) to ``target``.

    Demotions (to ``archived``) are never gated: taking a model out of service
    must always be possible.
    """
    if target is Stage.ARCHIVED:
        return []
    fsv = tags.get("feature_set_version", "")
    gates = [
        GateResult(
            "feature_set_compatible",
            fsv == serving_feature_set_version,
            hard=True,
            detail=f"model {fsv or 'unknown'} vs serving {serving_feature_set_version}",
        ),
    ]
    if target in (Stage.CHALLENGER, Stage.CHAMPION):
        required = [f"eval.{k}" for k in EVAL_KEYS]
        has_eval = all(k in tags for k in required)
        gates.append(
            GateResult(
                "has_evaluation",
                has_eval,
                hard=False,
                detail="held-out evaluation recorded" if has_eval else "missing evaluation tags",
            )
        )
        if has_eval:
            value = {k: float(tags[f"eval.{k}"]) for k in EVAL_KEYS}
            wf_gain = value["wf_null_log_loss"] - value["wf_log_loss"]
            test_gain = value["test_null_log_loss"] - value["test_log_loss"]
            passed = min(wf_gain, test_gain) >= MIN_LOG_LOSS_GAIN
            gates.append(
                GateResult(
                    "beats_base_rate",
                    passed,
                    hard=False,
                    detail=(
                        f"log loss gain over the base-rate forecast: walk-forward "
                        f"{wf_gain:+.4f}, test {test_gain:+.4f} "
                        f"(required >= {MIN_LOG_LOSS_GAIN} on both)"
                    ),
                )
            )
    return gates


def blocking(gates: list[GateResult], *, override: bool) -> list[GateResult]:
    """Failed gates that stop the transition (soft ones only without override)."""
    return [g for g in gates if not g.passed and (g.hard or not override)]
