# ADR 0002: JSON + Pydantic events, Schema Registry-ready

**Status:** Accepted (Phase 1)

## Context
Events need a strongly defined, versioned contract, validation at every
boundary, and a path to stricter schema governance later.

## Decision
- Pydantic v2 models in `shared.schemas` are the contract; values are UTF-8
  JSON; `event_type` and `schema_version` are in the payload and mirrored into
  Kafka headers.
- A registry maps `(event_type, schema_version)` to a model class.
- Tolerant-reader evolution: optional additions don't bump the version;
  breaking changes create a new version side by side.
- Wire format sits behind the `EventSerde` protocol.

## Alternatives considered
- **Avro + Confluent Schema Registry**: compact, centrally enforced
  compatibility. Costs another service and makes messages opaque in ordinary
  tooling. Worth it with many teams/producers or high byte volume; neither
  applies yet.
- **Protobuf**: similar benefits; code generation adds a build step in both
  Python and TypeScript.

## Consequences
- Easy debugging (`kafka-console-consumer` shows readable events) and no extra
  infrastructure. Larger payloads (acceptable at local throughput;
  compression `lz4` recovers much of it).
- Compatibility is enforced by code review and tests rather than a registry.
- Migration path: implement `AvroEventSerde` (Confluent wire format), register
  schemas generated from the Pydantic models, switch by configuration.
