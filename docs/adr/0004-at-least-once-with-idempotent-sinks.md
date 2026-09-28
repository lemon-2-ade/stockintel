# ADR 0004: At-least-once delivery with idempotent sinks

**Status:** Accepted (Phase 1)

## Context
Consumers crash, rebalance and restart. "Exactly-once" in Kafka covers
Kafka-to-Kafka processing inside transactions, not side effects into Postgres
or Redis.

## Decision
- Producers: idempotent, `acks=all`.
- Consumers: store an offset only after the message is fully handled;
  background-commit stored offsets. Accept redelivery.
- Every sink is idempotent by construction: natural primary keys with
  `ON CONFLICT DO NOTHING`, deterministic ids (uuid5) for predictions,
  event-time guards for last-write-wins caches and for rolling-window state.

## Alternatives considered
- **Kafka transactions (EOS v2)** for `raw -> enriched`: atomic
  read-process-write within Kafka, but side effects still need idempotency;
  adds latency and complexity. Deferred; consumers already read with
  `isolation.level=read_committed` so it can be enabled later.
- **At-most-once** (commit before processing): simpler, loses data on crash.
  Unacceptable for a system of record.

## Consequences
- Simple, robust recovery: restart and replay.
- Every new sink must document its dedup key; code review checks it.
- Tests exercise redelivery (Phase 3/4 integration tests).
