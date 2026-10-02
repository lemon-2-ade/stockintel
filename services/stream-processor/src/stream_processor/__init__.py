"""Stream processor: incremental technical indicators and anomaly detection.

Consumes ``market.raw``, keeps bounded per-symbol state, and publishes
``market.enriched`` and ``market.anomalies``. The computational core
(indicators, detectors, state) is pure Python with no Kafka dependency.
"""
