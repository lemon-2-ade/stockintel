import type {
  Anomaly,
  ModelStatus,
  MonitoringSummary,
  PipelineStatus,
  Prediction,
  StockDetail,
  StockSummary,
} from "../api/types";

export const NOW = Date.parse("2026-10-11T12:00:00Z");

export const stock = (symbol: string, overrides: Partial<StockSummary> = {}): StockSummary => ({
  symbol,
  name: `${symbol} Inc.`,
  last_price: 100,
  change: 0.5,
  change_pct: 0.5,
  volume: 1000,
  timestamp: "2026-10-11T11:59:58Z",
  stale: false,
  ...overrides,
});

export const prediction = (overrides: Partial<Prediction> = {}): Prediction => ({
  kind: "model_estimate",
  prediction_id: crypto.randomUUID(),
  timestamp: "2026-10-11T11:59:00Z",
  target_timestamp: "2026-10-11T11:59:05Z",
  horizon_bars: 5,
  task: "direction",
  predicted_direction: "up",
  predicted_return: null,
  class_probabilities: { up: 0.58, down: 0.42 },
  confidence: 0.58,
  model_name: "stockintel-direction-h5",
  model_version: "3",
  outcome: null,
  disclaimer: "Statistical model estimate. Not investment advice.",
  ...overrides,
});

export const model: ModelStatus = {
  name: "stockintel-direction-h5",
  version: "3",
  stage: "champion",
  promoted_at: "2026-10-10T07:05:00Z",
  promoted_by: "dev",
  reason: "first deployment",
  overridden_gates: ["beats_base_rate"],
  evaluation: { test_base_rate: 0.5635, test_log_loss: 0.685, test_roc_auc: 0.508 },
  disclaimer:
    "Statistical model estimate for research and demonstration only. Not investment advice.",
};

export const monitoring: MonitoringSummary = {
  model_name: "stockintel-direction-h5",
  model_version: "3",
  window_start: "2026-10-10T12:00:00Z",
  window_end: "2026-10-11T12:00:00Z",
  computed_at: "2026-10-11T11:58:00Z",
  status_counts: { ok: 3, warning: 1, alert: 2 },
  retrain_recommended: true,
  items: [
    {
      report_type: "data_drift",
      metric: "psi",
      feature: "vol_10",
      value: 6.3,
      threshold: 0.25,
      status: "alert",
      details: {},
    },
    {
      report_type: "data_drift",
      metric: "psi",
      feature: "ret_5",
      value: 5.3,
      threshold: 0.25,
      status: "alert",
      details: {},
    },
    {
      report_type: "performance",
      metric: "log_loss_minus_null",
      feature: null,
      value: 0.0032,
      threshold: 0.01,
      status: "ok",
      details: {},
    },
  ],
};

export const anomaly = (overrides: Partial<Anomaly> = {}): Anomaly => ({
  anomaly_id: crypto.randomUUID(),
  symbol: "AAPL",
  timestamp: "2026-10-11T11:59:30Z",
  interval: "1s",
  anomaly_type: "price_spike",
  severity: "high",
  observed_value: 0.021,
  expected_value: 0,
  score: 0.021,
  threshold: 0.01,
  detector: "return_threshold",
  detector_version: "1.0.0",
  ...overrides,
});

export const detail = (symbol = "AAPL"): StockDetail => ({
  symbol,
  name: `${symbol} Inc.`,
  observation: {
    timestamp: "2026-10-11T11:59:58Z",
    interval: "1s",
    open: 100,
    high: 101,
    low: 99,
    close: 100.5,
    volume: 1200,
  },
  analytics: { rsi_14: 64.2, sma_20: 100.1, return_1: 0.004 },
  session: {
    open: 99,
    high: 102,
    low: 98,
    volume: 50_000,
    change: 1.5,
    change_pct: 1.52,
    bars: 600,
  },
  prediction: prediction(),
  freshness: { as_of: "2026-10-11T11:59:59Z", age_seconds: 1, stale: false, source: "cache" },
});

export const pipeline: PipelineStatus = {
  checked_at: "2026-10-11T12:00:00Z",
  stores: { postgres: "ok", redis: "ok" },
  market_data: { status: "ok", latest_at: "2026-10-11T11:59:59Z", age_seconds: 1, detail: null },
  predictions: {
    status: "stale",
    latest_at: "2026-10-11T11:50:00Z",
    age_seconds: 600,
    detail: null,
  },
  monitoring: {
    status: "missing",
    latest_at: null,
    age_seconds: null,
    detail: "no monitor run yet",
  },
  model,
  active_symbols: 12,
  stale_symbols: ["MSFT"],
};
