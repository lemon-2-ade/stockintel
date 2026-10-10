"""Distribution profiles and drift statistics (PSI, Kolmogorov-Smirnov).

A **reference profile** summarises the training distribution of every model
input (and of the model's own output) compactly enough to store next to the
model: decile bin edges for PSI and 101 quantiles for an approximate KS test.
It is computed once at registration; the monitor compares live, served
inputs against it.

PSI (population stability index) over the reference deciles:

    PSI = sum_i (a_i - r_i) * ln(a_i / r_i)

with conventional reading < 0.1 stable, 0.1-0.25 moderate shift, > 0.25 major
shift. PSI ignores sample size, so it is paired with a KS statistic and its
asymptotic p-value, and nothing is reported below a minimum sample count.

Discrete inputs (``day_of_week``, ``month``) produce repeated quantiles; edges
are de-duplicated, so they get fewer, wider bins. KS is conservative for them;
read their PSI instead.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Final, Literal

import numpy as np

PSI_WARNING: Final = 0.1
PSI_ALERT: Final = 0.25
KS_ALERT_P: Final = 0.001
_EPS: Final = 1e-4
_QUANTILE_LEVELS = np.linspace(0.0, 1.0, 101)

Status = Literal["ok", "warning", "alert"]


@dataclass(frozen=True, slots=True)
class Distribution:
    edges: tuple[float, ...]
    """Inner bin edges (deciles, de-duplicated); bins are (-inf, e1], (e1, e2], ..., (ek, inf)."""
    proportions: tuple[float, ...]
    quantiles: tuple[float, ...]
    """101 quantiles (0%, 1%, ..., 100%) for the KS reference CDF."""
    n: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "edges": list(self.edges),
            "proportions": list(self.proportions),
            "quantiles": list(self.quantiles),
            "n": self.n,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Distribution:
        return cls(
            edges=tuple(float(x) for x in data["edges"]),
            proportions=tuple(float(x) for x in data["proportions"]),
            quantiles=tuple(float(x) for x in data["quantiles"]),
            n=int(data["n"]),
        )


def _clean(values: Any) -> np.ndarray:
    array = np.asarray(values, dtype=float).ravel()
    return array[np.isfinite(array)]


def _bin_proportions(values: np.ndarray, edges: tuple[float, ...]) -> np.ndarray:
    # side="left": a value equal to an edge falls in the bin that edge closes.
    index = np.searchsorted(np.asarray(edges), values, side="left")
    counts = np.bincount(index, minlength=len(edges) + 1)
    return counts / max(1, len(values))


def profile(values: Any, bins: int = 10) -> Distribution:
    data = _clean(values)
    if data.size == 0:
        raise ValueError("cannot profile an empty sample")
    inner = np.quantile(data, np.linspace(0, 1, bins + 1)[1:-1])
    edges = tuple(float(e) for e in np.unique(inner))
    return Distribution(
        edges=edges,
        proportions=tuple(float(p) for p in _bin_proportions(data, edges)),
        quantiles=tuple(float(q) for q in np.quantile(data, _QUANTILE_LEVELS)),
        n=int(data.size),
    )


def psi(reference: Distribution, values: Any) -> float:
    data = _clean(values)
    if data.size == 0:
        return float("nan")
    expected = np.clip(np.asarray(reference.proportions), _EPS, None)
    actual = np.clip(_bin_proportions(data, reference.edges), _EPS, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def _kolmogorov_sf(lam: float) -> float:
    """P(K > lam) for the Kolmogorov distribution (asymptotic series)."""
    if lam < 0.2:
        return 1.0
    total = sum((-1) ** (k - 1) * math.exp(-2 * k * k * lam * lam) for k in range(1, 101))
    return float(min(1.0, max(0.0, 2 * total)))


def ks(reference: Distribution, values: Any) -> tuple[float, float]:
    """One-sample KS against the reference CDF interpolated from its quantiles."""
    data = np.sort(_clean(values))
    n = data.size
    if n == 0:
        return float("nan"), float("nan")
    quantiles = np.asarray(reference.quantiles)
    cdf = np.interp(data, quantiles, _QUANTILE_LEVELS, left=0.0, right=1.0)
    upper = np.arange(1, n + 1) / n - cdf
    lower = cdf - np.arange(0, n) / n
    statistic = float(max(upper.max(), lower.max()))
    root = math.sqrt(n)
    return statistic, _kolmogorov_sf((root + 0.12 + 0.11 / root) * statistic)


def psi_status(value: float) -> Status:
    if math.isnan(value) or value < PSI_WARNING:
        return "ok"
    return "warning" if value < PSI_ALERT else "alert"


@dataclass(frozen=True, slots=True)
class ReferenceProfile:
    """Training-time distributions stored with a model version."""

    feature_set_version: str
    features: dict[str, Distribution]
    prediction: Distribution
    """Distribution of the model's P(up) on recent training data."""
    base_rate: float
    """Share of up labels in the training data: the null forecast for log loss."""

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_set_version": self.feature_set_version,
            "features": {k: v.to_dict() for k, v in self.features.items()},
            "prediction": self.prediction.to_dict(),
            "base_rate": self.base_rate,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ReferenceProfile:
        return cls(
            feature_set_version=str(data["feature_set_version"]),
            features={k: Distribution.from_dict(v) for k, v in data["features"].items()},
            prediction=Distribution.from_dict(data["prediction"]),
            base_rate=float(data["base_rate"]),
        )
