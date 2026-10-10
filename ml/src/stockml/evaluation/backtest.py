"""A cost-aware backtest of out-of-sample direction scores.

Accuracy and AUC say whether a model ranks weeks correctly; they do not say
whether trading on it pays after costs. This backtest answers that question
for the simplest uses of a weekly score.

Mechanics (deliberately simple, every simplification stated):

* **Non-overlapping holding periods.** Rebalance every ``horizon`` sessions;
  each position is held exactly for the label window, so the period return of
  a symbol is ``exp(y_return) - 1`` (close at t to close at t+H).
* **Execution at the decision close.** Features use the close of t and the
  trade is assumed to fill at that close. In reality the fill is a little
  later; the cost assumption is meant to absorb part of that, not all of it.
* **Costs** are charged on turnover: ``cost_bps`` per unit of traded notional,
  one way (10 bps default: spread plus slippage for liquid large caps, no
  market impact). Turnover is measured against the previous target weights;
  price drift within the period is ignored.
* **Offsets.** Which session starts the weekly cycle is arbitrary, so the
  backtest is run for every offset ``0..H-1`` and the spread is reported:
  a result that only works for one offset is noise.
* No leverage, no borrow costs for shorts, no taxes, no capacity limits.

Strategies (score = higher means more likely up):

``buy_hold``   equal weight in every symbol with a prediction (the benchmark)
``top_k``      long-only, equal weight in the ``k`` highest scores
``long_short`` long the top ``k``, short the bottom ``k``, 0.5 gross per side
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd

Strategy = Literal["buy_hold", "top_k", "long_short"]
STRATEGIES: tuple[Strategy, ...] = ("buy_hold", "top_k", "long_short")


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    horizon: int = 5
    cost_bps: float = 10.0
    k: int = 3
    sessions_per_year: int = 252

    @property
    def periods_per_year(self) -> float:
        return self.sessions_per_year / self.horizon


def target_weights(scores: np.ndarray, strategy: Strategy, k: int) -> np.ndarray:
    """Weights for one rebalance date; ``scores`` has NaN for symbols without a prediction.

    Ties are broken by column (symbol) order, so results never depend on row order.
    """
    weights = np.zeros(len(scores))
    available = np.flatnonzero(~np.isnan(scores))
    n = len(available)
    if n == 0:
        return weights
    if strategy == "buy_hold":
        weights[available] = 1.0 / n
        return weights
    ranked = available[np.argsort(-scores[available], kind="stable")]
    k = min(k, n // 2 if strategy == "long_short" else n)
    if k == 0:
        return weights
    if strategy == "top_k":
        weights[ranked[:k]] = 1.0 / k
    elif strategy == "long_short":
        weights[ranked[:k]] = 0.5 / k
        weights[ranked[-k:]] = -0.5 / k
    else:
        raise ValueError(f"unknown strategy {strategy!r}")
    return weights


def run_backtest(
    predictions: pd.DataFrame,
    strategy: Strategy,
    cfg: BacktestConfig | None = None,
    *,
    offset: int = 0,
) -> pd.DataFrame:
    """One row per holding period: gross, cost, net return, turnover, gross exposure.

    ``predictions`` needs ``symbol``, ``session_date``, ``score``, ``y_return``.
    """
    cfg = cfg or BacktestConfig()
    wide = predictions.pivot(index="session_date", columns="symbol", values=["score", "y_return"])
    wide = wide.sort_index().sort_index(axis=1)
    scores, returns = wide["score"], wide["y_return"]
    rows = slice(offset, None, cfg.horizon)
    score_m = scores.to_numpy(dtype=float)[rows]
    simple_m = np.expm1(returns.to_numpy(dtype=float)[rows])
    dates = scores.index[rows]

    previous = np.zeros(score_m.shape[1])
    records = []
    for date, row_scores, row_returns in zip(dates, score_m, simple_m, strict=True):
        weights = target_weights(row_scores, strategy, cfg.k)
        gross = float(np.nansum(weights * row_returns))
        turnover = float(np.abs(weights - previous).sum())
        cost = turnover * cfg.cost_bps / 10_000
        records.append(
            {
                "session_date": date,
                "gross": gross,
                "cost": cost,
                "net": gross - cost,
                "turnover": turnover,
                "exposure": float(np.abs(weights).sum()),
            }
        )
        previous = weights
    return pd.DataFrame(records)


def summarize(periods: pd.DataFrame, cfg: BacktestConfig | None = None) -> dict[str, float]:
    """Annualised statistics of a period-return series (no risk-free rate)."""
    cfg = cfg or BacktestConfig()
    ppy = cfg.periods_per_year
    net = periods["net"].to_numpy()
    n = len(net)
    if n < 2:
        return {"periods": float(n)}
    growth = float(np.prod(1 + net))
    std = float(np.std(net, ddof=1))
    mean = float(np.mean(net))
    equity = np.cumprod(1 + net)
    drawdown = equity / np.maximum.accumulate(np.concatenate([[1.0], equity]))[1:] - 1
    return {
        "periods": float(n),
        "years": n / ppy,
        "ann_return": growth ** (ppy / n) - 1 if growth > 0 else -1.0,
        "ann_volatility": std * math.sqrt(ppy),
        "sharpe": mean / std * math.sqrt(ppy) if std > 0 else float("nan"),
        "t_stat": mean / std * math.sqrt(n) if std > 0 else float("nan"),
        "max_drawdown": float(drawdown.min()),
        "hit_rate": float(np.mean(net > 0)),
        "avg_turnover": float(periods["turnover"].mean()),
        "ann_cost": float(periods["cost"].mean()) * ppy,
        "avg_exposure": float(periods["exposure"].mean()),
    }


def backtest_all_offsets(
    predictions: pd.DataFrame, strategy: Strategy, cfg: BacktestConfig | None = None
) -> dict[str, Any]:
    """Offset-0 summary plus the min/median/max Sharpe over every offset."""
    cfg = cfg or BacktestConfig()
    summaries = [
        summarize(run_backtest(predictions, strategy, cfg, offset=o), cfg)
        for o in range(cfg.horizon)
    ]
    sharpes = [s["sharpe"] for s in summaries]
    returns = [s["ann_return"] for s in summaries]
    return {
        "offset_0": summaries[0],
        "sharpe_min": float(np.min(sharpes)),
        "sharpe_median": float(np.median(sharpes)),
        "sharpe_max": float(np.max(sharpes)),
        "ann_return_median": float(np.median(returns)),
    }
