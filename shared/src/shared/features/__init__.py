"""Point-in-time model features, shared by offline training and online inference.

There is exactly **one** implementation (:class:`FeatureComputer`), updated
bar by bar. Training replays the cleaned history through it; the inference
path feeds it live bars. Offline/online skew is therefore impossible by
construction, and it is causal by construction: a feature at bar *t* can only
see bars up to *t*.
"""

from shared.features.computer import (
    FEATURE_NAMES,
    FEATURE_SET_VERSION,
    BarInput,
    FeatureComputer,
)

__all__ = ["FEATURE_NAMES", "FEATURE_SET_VERSION", "BarInput", "FeatureComputer"]
