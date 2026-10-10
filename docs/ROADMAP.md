# Implementation Roadmap

The platform is built in phases. Each phase ends in a working, tested state
and one or more commits. "Exit criteria" are checked before a phase is marked
done; nothing is claimed that has not been run.

| Phase | Scope | Exit criteria | Status |
| --- | --- | --- | --- |
| 1 | Architecture, repo + tooling, Docker infrastructure, event contracts, topic topology, DB schema | lint/type/unit tests green; migrations verified against PostgreSQL 16; compose file validates. The Kafka integration test is written but has not yet run against a live broker: run `make dev && make test-integration` locally (CI runs it from Phase 13) | done |
| 2 | Historical dataset acquisition (pinned + checksummed) and simulator calibration; market simulator (GBM, configurable volatility/drift/seed, injected spikes/drops/volume bursts), provider abstraction, Kafka producer service with delivery reports, metrics, graceful shutdown | deterministic simulator tests (fixed seed); events validate against schema; service verified locally for pacing and graceful shutdown. The producer -> Kafka integration test is written but, like Phase 1's, awaits a live broker (`make dev && make test-integration`) | done |
| 3 | Stream processor: incremental SMA/EMA/RSI/MACD/Bollinger/volatility, bounded per-symbol state, event-time handling, late/duplicate policy, rule-based anomaly detection, DLQ handling, processing-latency metric | indicator values match a pandas reference implementation; redelivery does not change results; poison message lands in DLQ | done |
| 4 | Persistence consumer (batched idempotent upserts), Redis latest-state writer, FastAPI (REST, WebSocket, health/ready/metrics, pagination, rate limit, CORS) | API integration tests against Postgres/Redis; WebSocket throttling test | done |
| 5 | Validation and documented cleaning of the historical snapshot (acquisition landed early, in Phase 2) | data-quality report; tests for each validation rule | done |
| 6 | Shared offline/online feature library, leakage tests, chronological splits, baselines (naive, logistic/linear) | leakage test proves features at T use only data <= T; baseline metrics logged | done |
| 7 | Gradient-boosted models (LightGBM/XGBoost), walk-forward validation, optional sequence model experiment, cost-aware backtest | comparison table vs baselines, with honest interpretation (sequence model skipped, reason in ML_PIPELINE.md) | **done** |
| 8 | MLflow tracking + registry (candidate/challenger/champion aliases), explicit promotion CLI with audit log, inference service | model loads from registry; `/predict` contract tests; latency measured | next |
| 9 | Prediction streaming, outcome resolution, drift (PSI/KS), performance monitoring, alerts, retraining workflow that produces a *candidate* only | monitor detects synthetic drift in a test; no auto-promotion path exists | |
| 10 | React + TS + Tailwind dashboard: candlesticks + volume + overlays, indicators, ML panel, anomaly feed, watchlist, pipeline status, themes | component tests; manual QA checklist; screenshots | |
| 11 | Prometheus metrics across services, Kafka lag exporter, Grafana dashboards, alert rules | dashboards provisioned from code; alert rules unit-tested with promtool | |
| 12 | Load tests (Kafka producer benchmark, Locust/k6 for API), end-to-end latency, failure-injection tests, `PERFORMANCE.md` with measured results | reproducible benchmark scripts + raw results committed | |
| 13 | GitHub Actions (lint, types, tests, frontend, image builds, integration stage with compose), dependency/secret scanning, optional Terraform for AWS | green pipeline on PR and main | |
| 14 | Final documentation, diagrams, screenshots, limitations and future work | README complete; every number in docs traceable to a script | |

## ML problem definition

Decided in Phase 6: direction of the forward 5-session log return (binary
`y_up`, optional neutral band), with the return itself as a secondary
regression task. Chronological splits with purging and a 5-session embargo,
plus expanding walk-forward folds; the 2023+ test set is held out. Details and
baseline results: [ML_PIPELINE.md](ML_PIPELINE.md).

## Open questions, decided in the phase that needs them

1. ~~**Historical dataset.**~~ Decided in Phase 2: a pinned, checksum-locked
   snapshot of a CC0 Hugging Face dataset with daily bars for 12 US large
   caps (2010-2026). Details and the alternatives rejected are in
   [DATA_PIPELINE.md](DATA_PIPELINE.md).
2. **Interval mismatch between training and live data (Phase 5/6).** Free
   long-history data is daily, while the live demo streams sub-minute
   simulated bars. Plan: features are scale-free (returns, ratios, z-scores,
   normalised indicators) so the same pipeline applies to any bar interval,
   and the simulator is calibrated per symbol from historical drift and
   volatility. It will be stated plainly that a GBM simulator is a random walk
   by construction, so a model *cannot* beat chance on simulated data. The live
   demo shows the pipeline mechanics (serving, logging, drift detection), not
   predictive power; predictive evaluation is done offline on historical data.
3. **Anomaly ground truth (Phase 2/3).** The simulator knows which anomalies it
   injected; exposing that label (e.g. an optional field or a separate audit
   topic) allows measuring detector precision/recall.
