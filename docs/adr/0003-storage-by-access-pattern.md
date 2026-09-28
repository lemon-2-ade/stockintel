# ADR 0003: Storage by access pattern (Postgres, Redis, Kafka)

**Status:** Accepted (Phase 1)

## Context
The dashboard needs "latest value" reads at high frequency; analysis and
monitoring need historical range queries and joins; the pipeline needs an
ordered, replayable transport.

## Decision
- **PostgreSQL 16** is the system of record (bars, indicators, anomalies,
  predictions + outcomes, model promotions, monitoring, watchlists).
- **Redis** holds only derivable hot state (latest bar/indicators/prediction,
  recent anomalies) and provides pub/sub fan-out to API replicas. No
  persistence, LRU eviction.
- **Kafka** is transport + short-term replay buffer, not a query store.
- **MLflow** uses its own database in the same Postgres instance, with its own
  role.

## Alternatives considered
- **TimescaleDB** from day one: better at very large time series
  (compression, continuous aggregates), but an extra extension to operate and
  unnecessary at local scale. The schema stays hypertable-compatible.
- **Redis as history store**: memory-bound and not designed for range
  analytics; rejected.
- **Separate Postgres instance for MLflow**: cleaner isolation, more local
  RAM; a separate database + role gives enough isolation here.

## Consequences
- Each store does what it is good at; losing Redis costs a warm-up, not data.
- Writes to Postgres must be batched and idempotent to keep up with the stream.
- Two sources can briefly disagree (cache ahead of DB); the API documents
  which endpoints are cache-backed (latest) vs DB-backed (history).
