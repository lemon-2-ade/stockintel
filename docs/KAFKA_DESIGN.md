# Kafka Design

The topology is declared once, in code, in
[`shared/src/shared/kafka/topics.py`](../shared/src/shared/kafka/topics.py), and
provisioned by `make topics` (`python -m shared.kafka.provision`). Broker-side
auto-creation is **disabled** so that a typo in a topic name fails loudly
instead of silently creating a one-partition topic with default retention.

## 1. Topics

| Topic | Key | Partitions (local) | Retention | Event type | Producers | Consumer groups |
| --- | --- | --- | --- | --- | --- | --- |
| `market.raw` | symbol | 6 | 7 d | `market.bar` v1 | market-producer | `stream-processor`, `persistence` |
| `market.enriched` | symbol | 6 | 3 d | `market.enriched` v1 | stream-processor | `feature-pipeline`, `persistence`, `api-cache` |
| `market.anomalies` | symbol | 3 | 30 d | `market.anomaly` v1 | stream-processor | `persistence`, `api-cache` |
| `market.predictions` | symbol | 3 | 30 d | `market.prediction` v1 | feature-pipeline | `persistence`, `api-cache`, `model-monitor` |
| `market.dead-letter` | original key or topic | 1 | 14 d | `dead_letter` v1 | any consumer | operators / replay tooling |

Why these values:

- **`market.raw` has the longest hot retention** because it is the only topic
  that cannot be regenerated. Every derived topic (and Postgres/Redis) can be
  rebuilt by resetting a consumer group to an earlier offset and replaying. A
  unit test enforces `retention(raw) >= retention(enriched)`.
- **Partition counts** cap consumer parallelism per group (at most one active
  member per partition). 6 on the high-volume topics allows scaling the stream
  processor to 6 instances locally; the low-volume topics use 3. Partitions are
  cheap to add but *adding them remaps keys* (see section 3), so production
  sizing should be done up front from the target throughput, e.g.
  `partitions >= max(target_MBps / per_partition_consumer_MBps, max_consumers)`.
- **Dead-letter topic has one partition**: its volume should be tiny, and a
  single global order makes incident triage easier.
- **`cleanup.policy=delete` everywhere.** A compacted "latest bar per symbol"
  topic was considered for cold-starting the cache, but Redis (rebuilt from
  `market.enriched`) and Postgres already cover that need; adding it would be
  a third copy of the same state.
- `message.timestamp.type=CreateTime`: the producer's timestamp is preserved,
  keeping Kafka's own timestamp aligned with event time for time-based offset
  lookups (`offsets_for_times`) during replay.

Replication: `KAFKA_TOPIC_REPLICATION_FACTOR` / `KAFKA_TOPIC_MIN_INSYNC_REPLICAS`
are `1/1` for the single local broker and should be `3/2` in production, where
`acks=all` + `min.insync.replicas=2` tolerates one broker loss without losing
acknowledged writes.

## 2. Event contract

All events share a versioned envelope (`shared.schemas.base.BaseEvent`):

```json
{
  "event_id": "7f1c2a4e-...",
  "event_type": "market.bar",
  "schema_version": 1,
  "produced_at": "2026-01-05T14:31:00.012Z",
  "source": "simulator",
  "trace_id": "b3c9...",
  "symbol": "AAPL",
  "timestamp": "2026-01-05T14:30:00Z",
  "interval": "1m",
  "open": 187.2, "high": 187.9, "low": 187.0, "close": 187.6,
  "volume": 18234
}
```

- `(event_type, schema_version)` identifies the Pydantic model
  (`shared.schemas.registry`). Both are also written as **Kafka headers** so
  infrastructure can route/count without parsing, while the payload stays
  self-describing if headers are lost (mirroring, bridges).
- Validation at the boundary: timezone-aware UTC timestamps, strict symbol
  pattern (no silent upper-casing: two producers must never disagree on a
  partition key), finite positive prices, non-negative volume, and the OHLC
  invariants `high >= max(open, close, low)` and `low <= min(open, close)`.
- **Evolution rules** (tolerant reader): adding an *optional* field is
  compatible and does not bump the version; consumers ignore unknown fields
  (`extra="ignore"`). Anything else (rename, removal, type or meaning change) is
  a new `schema_version` with its own model registered alongside the old one
  until all producers have moved.

### JSON vs Avro vs Protobuf

| | JSON + Pydantic (chosen) | Avro + Schema Registry | Protobuf + Schema Registry |
| --- | --- | --- | --- |
| Payload size | largest (field names repeated) | compact binary | compact binary |
| Human-readable in tooling | yes | needs registry-aware tools | needs registry-aware tools |
| Compatibility enforcement | in code + tests | enforced centrally by the registry | enforced centrally by the registry |
| Extra infrastructure | none | Schema Registry | Schema Registry |
| Python ergonomics | excellent (Pydantic) | good (fastavro) | codegen step |

JSON is the right trade-off at this scale and stage; see
[ADR 0002](adr/0002-json-events-first.md). The migration path is contained:
`shared.kafka.serde.EventSerde` is a protocol, so an `AvroEventSerde` using the
Confluent wire format (magic byte + schema id) can be added and selected by
configuration without touching producer/consumer logic. The Pydantic models
remain the application-level validation layer either way; the registry would
add *centralised* compatibility checks (e.g. `BACKWARD_TRANSITIVE`).

## 3. Partitioning and ordering

- **Key = symbol** on every market topic. Kafka guarantees order only within a
  partition, and all bars for one symbol hash (murmur2) to one partition, so a
  consumer sees each symbol's bars in produce order. That is exactly what
  rolling-window indicators need; there is no requirement for cross-symbol
  order.
- **Skew.** One very active symbol cannot be split across partitions. With the
  simulator this is controlled; with real data, an extreme hot key would call
  for key salting (`symbol#n`) plus a re-merge step, at the cost of losing
  per-symbol order. Not needed at this scale.
- **Changing the partition count remaps keys.** After growing a topic, a symbol
  may move to another partition while older events are still unconsumed on the
  old one, briefly breaking per-symbol order. The provisioner can grow topics
  but logs this, and the operational procedure is: pause producers, let
  consumers drain, grow, resume.
- **Producer-side ordering** is protected by `enable.idempotence=true` with
  `max.in.flight.requests.per.connection <= 5`: retries cannot reorder or
  duplicate messages within a partition.

## 4. Delivery semantics

Summary: **at-least-once end to end, made effectively-once where it matters by
idempotent sinks.** No component assumes exactly-once. See
[ADR 0004](adr/0004-at-least-once-with-idempotent-sinks.md).

**Producer** (`shared.kafka.client_config.producer_config`):
`acks=all`, idempotence on, `lz4` compression, `linger.ms=5` batching,
`delivery.timeout.ms=120s` as the upper bound on retries. Delivery reports are
checked; a message that ultimately fails is counted and logged with its
`event_id` (Phase 2).

**Consumer** (`consumer_config`): offsets are *stored* only after an event is
fully handled (`enable.auto.offset.store=false` + `store_offsets`), and the
background committer commits only stored offsets. So nothing is committed
before it is processed, but a crash or rebalance can redeliver a small window
of messages.

**Why duplicates cannot corrupt state:**

| Sink | Dedup mechanism |
| --- | --- |
| `market_bars` | PK `(symbol, bar_interval, ts)` + unique `event_id`, `INSERT ... ON CONFLICT DO NOTHING` |
| `bar_indicators` | PK `(symbol, bar_interval, ts)` |
| `anomalies` | unique `(source_event_id, detector, anomaly_type)` |
| `predictions` | `prediction_id = uuid5(source_event_id, model, version, horizon)`: the same input + model always yields the same id |
| Redis latest state | last-write-wins keyed by symbol, guarded by event-time comparison |
| Stream-processor rolling state | per-symbol "last seen event time": a bar at or before it is a duplicate/late event and is not folded into indicators twice |

Kafka transactions (read-process-write exactly-once) were considered for the
stream processor. They would make `market.raw -> market.enriched` atomic, but
not the side effects into Postgres/Redis, which need idempotency anyway, and
they add latency and operational complexity. They remain an option if a
downstream consumer ever needs strict exactly-once on derived topics
(consumers already use `isolation.level=read_committed`, so enabling
transactions later requires no consumer change).

## 5. Failure handling

| Failure | Handling |
| --- | --- |
| Broker unavailable at startup | bounded exponential backoff with full jitter (`shared.utils.retry`), then exit non-zero so the orchestrator restarts and alerts |
| Broker unavailable while running | librdkafka reconnects internally; the producer buffers up to `delivery.timeout.ms`, then reports failed deliveries; readiness probe turns unready |
| Malformed bytes / invalid schema / unknown type | classified by `JsonEventSerde` (`deserialization`, `validation`, `unknown_event_type`) and sent to `market.dead-letter` with original bytes (base64), topic, partition, offset, key and error; the offset is then committed so the partition keeps moving |
| Valid event, processing keeps failing | bounded in-process retries, then dead-letter with reason `processing` and the attempt count |
| Dead-letter publish itself fails | the consumer does **not** commit and stops: losing a message silently is worse than halting a partition, and the lag alert fires |
| Duplicate delivery | idempotent sinks (section 4) |
| Consumer crash / restart | resumes from the last committed offset; bounded redelivery window |
| Rebalance | `cooperative-sticky` assignment: only moved partitions pause; state for revoked partitions is flushed/dropped in the revoke callback |

**Poison messages never block a partition and are never silently dropped.**
Dead-lettered records can be inspected and, after a fix, replayed to their
original topic (tooling in Phase 12).

## 6. Consumer groups, scaling and backpressure

- Each logical consumer is its own group (`stream-processor`, `persistence`,
  `feature-pipeline`, `api-cache`, `model-monitor`), so each gets every event
  and progresses independently: a slow Postgres does not slow the dashboard.
- Scale a group horizontally up to the partition count. Rolling state in the
  stream processor is per symbol, and a symbol lives in one partition, so state
  can be partitioned the same way; on reassignment, state for a newly
  assigned partition is rebuilt by replaying a bounded window (Phase 3).
- **Backpressure** is natural in Kafka's pull model: a slow consumer simply
  falls behind (lag grows) without affecting producers or other groups.
  Consumers bound in-memory work (batch size, max buffered records) and use
  `max.poll.interval.ms` to be evicted if genuinely stuck. The frontend is
  protected separately by server-side throttling of WebSocket pushes.
- **Consumer lag** is the primary health signal: exported per group/partition
  (librdkafka statistics + a lag exporter in Phase 11) and alerted on.

## 7. Graceful shutdown

On `SIGTERM`/`SIGINT` every consumer: stops polling, finishes the in-flight
batch, stores its offsets, commits synchronously, closes the consumer (leaving
the group cleanly so partitions are reassigned immediately rather than after
`session.timeout.ms`). Producers `flush()` with a timeout and report anything
left undelivered. Implemented in the Phase 2/3 service runtime and covered by
tests.

## 8. Production mapping

| Local | Production |
| --- | --- |
| single KRaft broker, RF=1 | Amazon MSK / Confluent Cloud, 3 AZs, RF=3, `min.insync.replicas=2` |
| PLAINTEXT | `SASL_SSL` (SCRAM or IAM); settings already supported by `KafkaSettings` |
| topics from `topics.py` via provisioner job | same job in CI/CD, or Terraform `kafka_topic` resources generated from the same spec |
| JSON serde | JSON, or Avro + Schema Registry via a second `EventSerde` implementation |
