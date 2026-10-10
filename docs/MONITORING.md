# Prediction Pipeline and Model Monitoring

Phase 9 closes the loop: live bars become predictions on Kafka, predictions
are checked against what happened, and a monitor watches the model's inputs,
outputs and accuracy. It can recommend retraining; it cannot retrain or deploy
anything by itself.

```mermaid
flowchart LR
    E[(market.enriched)] --> PP[prediction-pipeline<br/>FeatureComputer per symbol]
    PP -->|POST /predict<br/>≤256 per call| INF[inference<br/>@champion]
    INF --> PP
    PP -->|PredictionEvent<br/>+ feature snapshot| P[(market.predictions)]
    P --> SINK[sink-postgres] --> DB[(predictions)]
    BARS[(market_bars)] --> MON
    DB --> MON[model-monitor]
    MLF[MLflow: reference profile<br/>logged at registration] --> MON
    MON --> OUT[(prediction_outcomes)]
    MON --> REP[(monitoring_reports)]
    MON --> PROM[Prometheus metrics]
    REP -. retrain_recommended .-> RT[make retrain<br/>→ candidate only]
```

## Prediction pipeline (`services/prediction-pipeline`)

* **Input** is `market.enriched`, already de-duplicated and ordered per symbol
  by the stream processor. Each bar updates that symbol's `FeatureComputer`,
  the same class that built the training set. A unit test replays 150 daily
  bars through the live path and through the offline `compute_features`, and
  requires all 28 features to agree to 1e-12 on every row.
* **Gaps** reset a symbol's features (61 bars to warm up again). Daily bars
  count weekdays, so weekends are not gaps; exchange holidays still are.
* **Batching**: all bars that are ready in one Kafka batch go to the
  inference service together, in requests of at most 256.
* **Output**: the service's `PredictionEvent` records, with the feature
  snapshot attached (`features`, an optional field, so `schema_version` stays
  1) for drift monitoring. The sink stores it in `predictions.features`; the
  Redis cache leaves it out.
* **Delivery**: at-least-once like the stream processor: produce, flush, then
  store offsets. Prediction ids are deterministic, so a redelivered batch
  re-publishes identical records that the sinks de-duplicate.
* **Restart**: the consumer seeks back `PIPELINE_WARMUP_MESSAGES` (2,000)
  before the committed offset and replays them without predicting, so
  features are rebuilt without a state store.
* **Degradation**: if inference is down (after 2 quick retries) or rejects the
  input (422), those bars get **no prediction**, the skip is counted
  (`sip_pipeline_predictions_skipped_total{reason}`) and offsets are still
  committed. Market data never waits for the model; a prediction for a bar
  that is minutes old has little value.
* `PIPELINE_PREDICT_EVERY_N_BARS` thins predictions per symbol if needed.

## Outcome resolution

The monitor joins realised prices to predictions whose `target_ts` has
passed, in one idempotent SQL statement (`INSERT ... ON CONFLICT DO NOTHING`):

* actual return `ln(close at target / close at as-of)`, the training label;
* the target bar is the **first bar at or after** `target_ts`, accepted only
  within one horizon after the target. A prediction whose target bar never
  came (the stream stopped) stays unresolved instead of being scored against
  a much later price; an integration test covers this case;
* `is_correct` is NULL when the realised return is exactly zero.

## Checks

Every `MONITOR_INTERVAL_S` (300 s) the monitor reads the predictions of the
last `MONITOR_WINDOW_HOURS` (24 h) and, per served model version, writes rows
to `monitoring_reports` and Prometheus gauges (`sip_monitor_value`,
`sip_monitor_status`: 0 ok, 1 warning, 2 alert):

| Report | Metric | Compared with | Status |
| --- | --- | --- | --- |
| `data_drift` | PSI per input feature (+ KS statistic and p-value in details) | training distribution of that feature | ok < 0.1 ≤ warning < 0.25 ≤ alert |
| `prediction_drift` | PSI of P(up) | the model's P(up) on its last training year | same thresholds |
| `performance` | log loss minus the null forecast's | constant P(up) = training base rate | alert if worse by > 0.01 |
| `performance` | accuracy minus the live majority rate | always predicting the majority class | informational |
| `operational` | `retrain_recommended` | ≥ 30% of inputs in PSI alert, or a performance alert | warning |
| `operational` | `reference_profile_missing` | version registered before Phase 9 | warning, drift skipped |

The **reference profile** (decile edges and 101 quantiles per feature, the
P(up) distribution, the training base rate) is computed at registration and
logged with the model (`monitoring/reference_profile.json`), so the monitor
always compares against the data that model was trained on. Nothing is
reported below 200 inputs or 100 resolved outcomes. Discrete inputs
(`day_of_week`, `month`) get merged bins; read their PSI, not their KS.

## Retraining workflow

`make retrain REASON="..."` (`stockml.registry.retrain`):

1. rebuilds the labelled dataset from the latest cleaned data;
2. keeps the Phase 7 hyperparameters: retraining is not re-tuning. Re-tuning
   on every retrain would reuse the most recent data for selection over and
   over;
3. evaluates on the last 4 complete years with the purged, embargoed
   walk-forward procedure: 3 years give `eval.wf_*`, the last year gives
   `eval.test_*`, each next to the base-rate forecast from the same training
   fold;
4. fits on all data and registers a **candidate** with those tags (and
   `evaluation_protocol` saying how they were obtained).

The promotion gates then judge the candidate on this fresh evidence. No code
path in the monitor or the retraining job moves a model past `candidate`; a
unit test checks that the champion is unchanged after retraining.

## End-to-end check on simulated data

`make simulate-monitoring INTERVAL=1d|1m` (`scripts/simulate_monitoring.py`)
runs the production code for every stage except Kafka: calibrated simulator →
stream processor → feature store → inference service (`@champion`, XGBoost
version 3) → Postgres sink → one monitor cycle. 12 symbols × 400 bars, seed 7,
local PostgreSQL 16 and an MLflow registry on SQLite. Raw output:
[1d](benchmarks/monitoring_simulation_1d.json),
[1m](benchmarks/monitoring_simulation_1m.json).

| | Daily bars | 1-minute bars |
| --- | ---: | ---: |
| Predictions | 4,080 | 4,080 |
| Inputs ok / warning / alert | 13 / 7 / 8 | 2 / 2 / 24 |
| PSI `ret_5` | 0.067 (ok) | 5.33 (alert) |
| PSI `vol_10` | 0.372 (alert) | 6.35 (alert) |
| PSI of P(up) | 0.090 (ok) | 5.04 (alert) |
| Log loss minus null (n resolved) | +0.003 (3,996) | -0.009 (3,977) |
| Accuracy vs majority | 0.467 vs 0.533 | 0.485 vs 0.518 |
| Retraining recommended | no (8/28 inputs) | yes (24/28 inputs) |

How to read it:

* **1-minute bars are a different world for this model.** Returns,
  volatility, ranges and trend features are one to two orders of magnitude
  smaller than on daily data; `day_of_week` and `month` are constant. The
  features are scale-free across symbols, not across bar lengths. The monitor
  says so loudly (24 of 28 inputs, and the model's output, in alert) and
  recommends retraining. That is the correct verdict: the live demo stream
  should not be read as a test of the model.
* **Daily simulated bars are close, but not identical, to real ones.** The
  remaining alerts are explainable: `gap` (the simulator opens every bar at
  the previous close, real stocks gap overnight), `day_of_week` (the
  simulator emits bars on weekends), volatility features (constant-volatility
  GBM calibrated on the last 3 years vs 2010-2026 history), and the simulated
  volume and close-position patterns. The model's output distribution is
  stable (PSI 0.09).
* **Performance** is at the null forecast in both cases. That is expected:
  simulated prices are random walks, and the model has no skill on real data
  either ([MODELS.md](MODELS.md)).

**A bug the monitor found.** The first daily run showed `vol_10` at twice the
training median (PSI 3.29) and P(up) drifting (PSI 0.41). The cause was in
the market simulator: a `1d` bar received 24 hours of variance instead of one
6.5-hour trading session (σ too high by √(24/6.5) ≈ 1.9). It is fixed (a bar
never spans more than one session) and covered by a regression test; the
numbers above are after the fix.

## Limitations

* The Kafka path of the pipeline is covered by unit tests with fake
  consumers and producers; it has not run against a broker in this
  environment (no Docker daemon). The simulation above bypasses Kafka.
* Alerts are rows, log lines and metrics; routing them to people (Prometheus
  alert rules, Alertmanager) is Phase 11.
* PSI with 10 bins is a coarse, sample-size-blind measure; it is paired with
  KS and a minimum sample, but thresholds are conventions, not calibrated
  error rates.
* Exchange holidays are not modelled (one feature reset per holiday for daily
  bars, and weekday-based target times).
