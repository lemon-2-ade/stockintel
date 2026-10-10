"""Evaluation metrics, reported honestly.

* Accuracy is always shown next to the **base rate** (always predicting the
  majority class). On this data "up" weeks are ~55%: a 55% accurate model has
  learned nothing.
* Confidence intervals account for **overlapping labels**: with a 5-session
  horizon, consecutive labels share 4/5 of their window, so the effective
  sample size is roughly ``n / horizon``. Naive intervals would be ~2.2x too
  narrow.
* Predictive accuracy is not profitability: a model can be right 55% of the
  time and lose money after costs (see the backtest in Phase 7).
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    log_loss,
    mean_absolute_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
    root_mean_squared_error,
)

Metrics = dict[str, Any]


def accuracy_interval(accuracy: float, n: int, horizon: int) -> tuple[float, float]:
    """95% normal-approximation interval with an effective sample size of n / horizon."""
    n_eff = max(1.0, n / max(horizon, 1))
    half = 1.96 * math.sqrt(max(accuracy * (1 - accuracy), 1e-12) / n_eff)
    return max(0.0, accuracy - half), min(1.0, accuracy + half)


def classification_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_score: np.ndarray | None = None,
    *,
    horizon: int = 1,
    score_is_probability: bool = True,
) -> Metrics:
    """Binary metrics with "up" (1) as the positive class.

    ``y_score`` ranks rows (higher = more likely up) for ROC-AUC; log loss is
    only reported when it is a calibrated-looking probability in [0, 1].
    """
    accuracy = float(accuracy_score(y_true, y_pred))
    low, high = accuracy_interval(accuracy, len(y_true), horizon)
    base_rate = float(max(np.mean(y_true), 1 - np.mean(y_true)))
    metrics: Metrics = {
        "n": len(y_true),
        "accuracy": accuracy,
        "accuracy_ci95": [low, high],
        "base_rate": base_rate,
        "accuracy_minus_base_rate": accuracy - base_rate,
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "precision_up": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall_up": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1_up": float(f1_score(y_true, y_pred, zero_division=0)),
        "f1_macro": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
        "confusion_matrix": confusion_matrix(y_true, y_pred, labels=[0, 1]).tolist(),
    }
    if y_score is not None:
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_score))
        if score_is_probability:
            clipped = np.clip(y_score, 1e-6, 1 - 1e-6)
            metrics["log_loss"] = float(log_loss(y_true, clipped, labels=[0, 1]))
    return metrics


def regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Metrics:
    """MAE/RMSE/R² plus directional accuracy (sign agreement, zeros excluded)."""
    nonzero = (y_true != 0) & (y_pred != 0)
    directional = (
        float(np.mean(np.sign(y_true[nonzero]) == np.sign(y_pred[nonzero])))
        if nonzero.any()
        else float("nan")
    )
    return {
        "n": len(y_true),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(root_mean_squared_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
        "directional_accuracy": directional,
    }
