from __future__ import annotations

import pytest
from factories import BarFactory, KwargsFactory, bar_kwargs, make_bar


@pytest.fixture(name="bar_kwargs")
def bar_kwargs_fixture() -> KwargsFactory:
    return bar_kwargs


@pytest.fixture(name="make_bar")
def make_bar_fixture() -> BarFactory:
    return make_bar
