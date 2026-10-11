// Mirrors apps/api/src/api/schemas.py. Timestamps are ISO-8601 strings (UTC).

export type BarInterval = "1s" | "5s" | "1m" | "5m" | "15m" | "1h" | "1d";
export type Direction = "up" | "down" | "neutral";
export type Severity = "low" | "medium" | "high" | "critical";
export type Status = "ok" | "warning" | "alert";

export interface Bar {
  timestamp: string;
  interval: BarInterval;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface Indicators {
  return_1?: number | null;
  log_return_1?: number | null;
  price_change?: number | null;
  volume_change_pct?: number | null;
  sma_20?: number | null;
  ema_12?: number | null;
  ema_26?: number | null;
  volatility_20?: number | null;
  volume_sma_20?: number | null;
  volume_ratio?: number | null;
  rsi_14?: number | null;
  macd?: number | null;
  macd_signal?: number | null;
  macd_hist?: number | null;
  bb_upper?: number | null;
  bb_middle?: number | null;
  bb_lower?: number | null;
  bb_width?: number | null;
}

export interface Freshness {
  as_of: string;
  age_seconds: number;
  stale: boolean;
  source: "cache" | "database";
}

export interface StockSummary {
  symbol: string;
  name: string | null;
  last_price: number | null;
  change: number | null;
  change_pct: number | null;
  volume: number | null;
  timestamp: string | null;
  stale: boolean;
}

export interface SessionStats {
  open: number;
  high: number;
  low: number;
  volume: number;
  change: number;
  change_pct: number;
  bars: number;
}

export interface PredictionOutcome {
  actual_direction: Direction;
  actual_return: number;
  is_correct: boolean | null;
  resolved_at: string;
}

export interface Prediction {
  kind: "model_estimate";
  prediction_id: string;
  timestamp: string;
  target_timestamp: string;
  horizon_bars: number;
  task: "direction" | "return";
  predicted_direction: Direction | null;
  predicted_return: number | null;
  class_probabilities: Partial<Record<Direction, number>> | null;
  confidence: number | null;
  model_name: string;
  model_version: string;
  outcome?: PredictionOutcome | null;
  disclaimer: string;
}

export interface StockDetail {
  symbol: string;
  name: string | null;
  observation: Bar;
  analytics: Indicators | null;
  session: SessionStats | null;
  prediction: Prediction | null;
  freshness: Freshness;
}

export interface Anomaly {
  anomaly_id: string;
  symbol: string;
  timestamp: string;
  interval: BarInterval;
  anomaly_type: string;
  severity: Severity;
  observed_value: number;
  expected_value: number | null;
  score: number;
  threshold: number | null;
  detector: string;
  detector_version: string;
}

export interface Page<T> {
  items: T[];
  next_before: string | null;
}

export interface History {
  symbol: string;
  interval: BarInterval;
  bars: Bar[];
  next_before: string | null;
}

export interface IndicatorPoint {
  timestamp: string;
  indicator_version: string;
  values: Indicators;
}

export interface WatchlistItem {
  symbol: string;
  position: number;
  added_at: string;
  quote: StockSummary | null;
}

export interface ModelStatus {
  name: string;
  version: string;
  stage: "champion";
  promoted_at: string;
  promoted_by: string;
  reason: string;
  overridden_gates: string[];
  evaluation: Record<string, number>;
  disclaimer: string;
}

export interface MonitoringItem {
  report_type: "data_quality" | "data_drift" | "prediction_drift" | "performance" | "operational";
  metric: string;
  feature: string | null;
  value: number;
  threshold: number | null;
  status: Status;
  details: Record<string, unknown>;
}

export interface MonitoringSummary {
  model_name: string | null;
  model_version: string | null;
  window_start: string;
  window_end: string;
  computed_at: string;
  status_counts: Record<Status, number>;
  retrain_recommended: boolean;
  items: MonitoringItem[];
}

export interface StageStatus {
  status: "ok" | "stale" | "missing";
  latest_at: string | null;
  age_seconds: number | null;
  detail: string | null;
}

export interface PipelineStatus {
  checked_at: string;
  stores: Record<string, string>;
  market_data: StageStatus;
  predictions: StageStatus;
  monitoring: StageStatus;
  model: ModelStatus | null;
  active_symbols: number;
  stale_symbols: string[];
}

// ----------------------------------------------------------------- live (WebSocket)

/** An enriched bar as published on the live channel (event payload). */
export interface LiveBar extends Bar {
  symbol: string;
  indicators?: Indicators | null;
}

/** A prediction event as published on the live channel. */
export interface LivePrediction {
  prediction_id: string;
  symbol: string;
  timestamp: string;
  target_timestamp: string;
  horizon_bars: number;
  predicted_direction: Direction | null;
  class_probabilities: Partial<Record<Direction, number>> | null;
  confidence: number | null;
  model_name: string;
  model_version: string;
}

export type LiveItem =
  | { type: "bar"; symbol: string; data: LiveBar }
  | { type: "prediction"; symbol: string; data: LivePrediction }
  | { type: "anomaly"; symbol: string; data: Anomaly };

export type ServerMessage =
  | { type: "subscribed"; symbols: string[] }
  | { type: "snapshot"; items: LiveItem[] }
  | { type: "updates"; server_time: string; items: LiveItem[] }
  | { type: "heartbeat"; server_time: string }
  | { type: "pong" }
  | { type: "error"; message: string };
