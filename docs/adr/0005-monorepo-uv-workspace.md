# ADR 0005: Monorepo with a uv workspace and a shared contract package

**Status:** Accepted (Phase 1)

## Context
Many Python services exchange events and share tables. A contract change must
reach producers and consumers together, while each service's container should
include only what it needs.

## Decision
- One repository. Python projects are members of a **uv workspace** with a
  single lockfile (`uv.lock`) for reproducible resolution.
- `shared/` (`stockintel-shared`, import name `shared`) owns everything that
  crosses a service boundary: event schemas, topic topology, DB models and
  migrations, configuration, logging, retry. It depends on no service.
- Heavy dependencies are optional extras (`kafka`, `db`), so e.g. the ML
  training image does not pull librdkafka unless it needs it.
- Each service gets its own `pyproject.toml` and multi-stage Dockerfile that
  installs only its dependency closure (`uv sync --package <svc>`).
- Tests live in the top-level `tests/` tree, organised by component.

## Alternatives considered
- **Polyrepo + published contract package**: realistic for large orgs, but
  version skew and release overhead add nothing here.
- **One big package**: simplest, but every image would carry every
  dependency (FastAPI, LightGBM, confluent-kafka, ...).
- **Poetry**: works, but uv's workspace support and speed are better suited.

## Consequences
- Atomic contract changes with one PR; CI tests everything together.
- Discipline required: `shared` must stay small and free of service logic.
