# Limitations and future work

What this project does not do, what was not verified, and what would come next.
It is meant to be read before trusting any claim elsewhere in the docs.

## The model has no demonstrated edge

* On held-out 2023-2026 data the LightGBM and XGBoost direction models score a
  log loss of 0.685, identical to always predicting the base rate, and a
  ROC-AUC of 0.505 and 0.508 ([MODELS.md](MODELS.md)). The weekly top-3
  strategy built on XGBoost has a lower Sharpe ratio than buy-and-hold in the
  same period after 10 bps costs (1.36 against 2.01), and the models'
  long/short variants have t-statistics between -0.7 and 0.2.
* The model is served only because promotion was forced with a recorded gate
  override; the dashboard says so in words next to every estimate
  ([MODEL_REGISTRY.md](MODEL_REGISTRY.md)).
* Live data is simulated (a calibrated random walk), so a model cannot show
  skill on it by construction. The live demo exercises serving, logging, drift
  detection and the dashboard, not predictive power.
* The training data is daily bars for 12 large US stocks that exist today, so
  it carries survivorship bias, and the model is applied to bars of other
  intervals in the demo, which the drift monitor flags correctly (24 of 28
  inputs on 1-minute bars; [MONITORING.md](MONITORING.md)).
* No sequence model was trained (reason in [ML_PIPELINE.md](ML_PIPELINE.md)).

## What was and was not run

Run for real: unit tests (349, one of which needs promtool), the 36 integration tests against a Kafka 4.0.0
broker, PostgreSQL 16 and Redis 7 as local processes, the dashboard's 28
tests and a manual browser run against its production build, promtool rule
tests, Terraform `validate`, and the benchmarks in [PERFORMANCE.md](PERFORMANCE.md).

Not run, in any environment I had:

* **The Docker stack.** No Docker daemon was available, so the images, the
  Compose file's wiring and healthchecks, nginx in front of the dashboard in
  Compose, and the Prometheus and Grafana containers have not been started. They
  are validated syntactically only.
* **The GitHub Actions workflows** (linted with `actionlint`; their commands
  pass on a clean clone; the first hosted run was not observed).
* **Grafana rendering.** Dashboard queries parse with `promtool`; the
  dashboards were never loaded into Grafana. No Alertmanager is configured.
* **The prediction pipeline, sinks, and model monitor against Kafka.** The
  stream processor and producer were (Phase 12); the other consumers were
  covered by unit tests and Kafka-free runs.
* **Terraform `plan` or `apply`** (no AWS account used).
* **Anything with more than one replica**, more than one broker, TLS or SASL.

## Known gaps

* **No authentication or authorization.** Watchlists use an opaque default
  user. Public exposure would need auth, per-user limits and TLS termination.
* **Throughput limits are unknown.** Measurements were on one 2-vCPU machine
  shared by the load generator and every dependency; they show behaviour, not
  capacity. The API's slowest routes (`/status`, symbol detail) were identified
  but not optimised.
* **End-to-end latency stops at Kafka.** Time from a bar to its appearance in
  the API or WebSocket was not measured.
* **Stream processor batching.** A shorter `STREAM_POLL_TIMEOUT_S` cut the
  median latency from about 300 ms to about 40 ms in the benchmark; its CPU cost
  was not measured, so the default stayed.
* **Market calendar.** Holidays and half-days are not modelled; weekends are
  handled for historical data, and gap handling treats missing bars generally.
* **Chart history.** The dashboard loads the latest 500 bars; older history is
  available from the API's keyset pagination but not wired to scrolling.
* **JSON on the wire.** Chosen for debuggability ([ADR 0002](adr/0002-json-events-first.md));
  there is no schema registry and no Avro/Protobuf.
* **Single-writer assumptions.** The Redis event-time guards protect against
  stale writes from one pipeline; multi-region or active-active setups were not
  designed.
* **No dead-letter replay tool.** Records in `market.dead-letter` can be
  inspected and re-published by hand; nothing automates it.
* **No end-to-end browser tests in CI.** The Playwright QA was run by hand.
* **Images are not pushed, scanned or signed**, and nothing deploys.

## Future work, roughly in order of value

1. Run the full stack under Docker once, fix what that shows, and put the
   result (screenshots of Grafana, `docker compose ps`, a CI run) in the docs.
2. Replace the simulator's random walk with a real live or delayed market-data
   provider, keeping the simulator for tests; then evaluate the daily model on
   bars it was actually trained for.
3. Add authentication and per-user watchlists; put Alertmanager behind the
   existing alert rules.
4. Measure bar-to-browser latency, and API capacity with more workers and a
   separate database host; cache the two slowest API routes.
5. Cheaper evaluation of alternatives to the current features: longer-horizon
   targets, cross-sectional ranking, and regime features, each judged against
   the same leakage-safe protocol and the base-rate null.
6. Release workflow: build and scan images, push to the Terraform-created ECR
   repositories, and a compute layer (ECS or Kubernetes) for the services.
7. Schema Registry with Avro or Protobuf once more than one team consumes the
   topics.
