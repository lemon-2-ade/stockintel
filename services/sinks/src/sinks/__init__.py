"""Kafka -> storage sinks.

Two independent consumer groups built on one batch runner:

* ``persistence``: market.raw/enriched/anomalies/predictions -> PostgreSQL
  (system of record; idempotent inserts).
* ``cache``: market.enriched/anomalies/predictions -> Redis latest state +
  pub/sub fan-out for the API's WebSockets.

Separate groups mean a slow database never delays the live dashboard, and
either sink can be rebuilt by resetting its offsets and replaying.
"""
