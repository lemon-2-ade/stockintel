# Architecture Decision Records

Short records of decisions that shape the system, in the format
*Context -> Decision -> Alternatives -> Consequences*. An ADR is never edited
to reverse a decision; a new ADR supersedes it.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-kafka-as-event-backbone.md) | Kafka (KRaft) as the event backbone | Accepted |
| [0002](0002-json-events-first.md) | JSON + Pydantic events, Schema Registry-ready | Accepted |
| [0003](0003-storage-by-access-pattern.md) | Storage by access pattern: Postgres, Redis, Kafka | Accepted |
| [0004](0004-at-least-once-with-idempotent-sinks.md) | At-least-once delivery with idempotent sinks | Accepted |
| [0005](0005-monorepo-uv-workspace.md) | Monorepo with a uv workspace and a shared contract package | Accepted |
| [0006](0006-explicit-audited-promotion.md) | Explicit, audited model promotion with MLflow aliases and gates | Accepted |
