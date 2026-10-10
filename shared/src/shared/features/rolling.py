"""Small O(1) rolling primitives (pure Python, no numpy) for the feature computer."""

from __future__ import annotations

import math
from collections import deque


class Window:
    """Fixed-size window with mean/std from shifted running sums.

    Sums are kept for ``x - shift`` (numerically stable when the variance is
    tiny relative to the level) and recomputed exactly every ``size`` updates
    so rounding error cannot accumulate.
    """

    __slots__ = ("_shift", "_since_resync", "_sum", "_sumsq", "size", "values")

    def __init__(self, size: int) -> None:
        if size < 1:
            raise ValueError("size must be >= 1")
        self.size = size
        self.values: deque[float] = deque(maxlen=size)
        self._shift: float | None = None
        self._sum = 0.0
        self._sumsq = 0.0
        self._since_resync = 0

    def push(self, value: float) -> None:
        if self._shift is None:
            self._shift = value
        if len(self.values) == self.size:
            old = self.values[0] - self._shift
            self._sum -= old
            self._sumsq -= old * old
        self.values.append(value)
        dev = value - self._shift
        self._sum += dev
        self._sumsq += dev * dev
        self._since_resync += 1
        if self._since_resync >= self.size:
            self._shift = math.fsum(self.values) / len(self.values)
            devs = [v - self._shift for v in self.values]
            self._sum = math.fsum(devs)
            self._sumsq = math.fsum(d * d for d in devs)
            self._since_resync = 0

    @property
    def full(self) -> bool:
        return len(self.values) == self.size

    def mean(self) -> float | None:
        if not self.full or self._shift is None:
            return None
        return self._shift + self._sum / self.size

    def std(self, ddof: int = 1) -> float | None:
        n = len(self.values)
        if not self.full or n - ddof <= 0:
            return None
        dev_mean = self._sum / n
        return math.sqrt(max((self._sumsq - n * dev_mean * dev_mean) / (n - ddof), 0.0))

    def sum(self) -> float | None:
        if not self.full or self._shift is None:
            return None
        return self._sum + self._shift * self.size


class Ema:
    """EMA seeded with the first value (pandas ``ewm(adjust=False)``).

    Valid once ``span`` values have been seen.
    """

    __slots__ = ("_alpha", "_count", "_value", "span")

    def __init__(self, span: int) -> None:
        self.span = span
        self._alpha = 2.0 / (span + 1)
        self._value: float | None = None
        self._count = 0

    def update(self, x: float) -> float | None:
        self._value = x if self._value is None else self._value + self._alpha * (x - self._value)
        self._count += 1
        return self._value if self._count >= self.span else None


class WilderAverage:
    """Wilder smoothing, seeded with the simple mean of the first ``period`` values."""

    __slots__ = ("_count", "_seed", "_value", "period")

    def __init__(self, period: int) -> None:
        self.period = period
        self._count = 0
        self._seed = 0.0
        self._value: float | None = None

    def update(self, x: float) -> float | None:
        self._count += 1
        if self._value is None:
            self._seed += x
            if self._count == self.period:
                self._value = self._seed / self.period
            return self._value
        self._value = (self._value * (self.period - 1) + x) / self.period
        return self._value
