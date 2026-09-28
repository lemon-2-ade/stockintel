# ADR 0001: Kafka (KRaft) as the event backbone

**Status:** Accepted (Phase 1)

## Context
Several independent consumers (analytics, persistence, feature pipeline,
cache writer, model monitor) need the same market events, each at its own
pace, with per-symbol ordering, and the ability to rebuild derived state by
replaying history.

## Decision
Use Apache Kafka as the event log, run in KRaft mode (no ZooKeeper), keyed by
symbol. Topics are declared in code and provisioned by a dedicated job; broker
auto-creation is disabled.

## Alternatives considered
- **RabbitMQ / a work queue**: good for task distribution, but messages are
  consumed-and-gone; replay and many independent readers of the same stream
  are not its model.
- **Redis Streams**: simple and fast, but memory-bound retention and weaker
  durability; we already use Redis as a *cache*, and conflating the two would
  make cache eviction a data-loss event.
- **Direct service-to-service calls**: couples availability and throughput of
  every stage; a slow database would slow the dashboard.
- **Redpanda** (Kafka API compatible): attractive operationally, but Apache
  Kafka is what the managed offerings (MSK, Confluent) run and what the
  interview discussion is about. The client code would work unchanged.

## Consequences
- Replayable, durable history of raw events; consumer groups scale and fail
  independently; per-key ordering.
- We must handle at-least-once redelivery (ADR 0004), partition sizing, and
  consumer-lag monitoring.
- Local footprint of a JVM broker (~0.5-1 GB RAM); acceptable.
