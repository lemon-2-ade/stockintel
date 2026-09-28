from __future__ import annotations

import random

import pytest

from shared.utils.retry import BackoffPolicy, retry_call


class Flaky:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError(f"boom {self.calls}")
        return "ok"


def test_succeeds_after_transient_failures() -> None:
    sleeps: list[float] = []
    fn = Flaky(failures=2)
    result = retry_call(fn, policy=BackoffPolicy(max_attempts=3), sleep=sleeps.append)
    assert result == "ok"
    assert fn.calls == 3
    assert len(sleeps) == 2


def test_gives_up_after_max_attempts_and_reraises() -> None:
    fn = Flaky(failures=10)
    with pytest.raises(ConnectionError, match="boom 3"):
        retry_call(fn, policy=BackoffPolicy(max_attempts=3), sleep=lambda _: None)
    assert fn.calls == 3


def test_non_retryable_errors_propagate_immediately() -> None:
    def fail() -> None:
        raise KeyError("bug, not an outage")

    with pytest.raises(KeyError):
        retry_call(fail, policy=BackoffPolicy(), retry_on=(ConnectionError,), sleep=lambda _: None)


def test_delay_is_capped_exponential_with_full_jitter() -> None:
    policy = BackoffPolicy(base_delay_s=1.0, max_delay_s=5.0, multiplier=2.0)
    rng = random.Random(7)
    for attempt, cap in [(1, 1.0), (2, 2.0), (3, 4.0), (4, 5.0), (10, 5.0)]:
        for _ in range(50):
            assert 0.0 <= policy.delay_for(attempt, rng) <= cap


@pytest.mark.parametrize(
    "kwargs", [{"max_attempts": 0}, {"base_delay_s": -1.0}, {"base_delay_s": 5, "max_delay_s": 1}]
)
def test_policy_validation(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match=r"max_attempts|base_delay"):
        BackoffPolicy(**kwargs)  # type: ignore[arg-type]
