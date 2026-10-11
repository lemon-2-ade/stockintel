import type { LivePrediction, Prediction } from "../api/types";

export interface Estimate {
  pUp: number | null;
  direction: string | null;
  horizonBars: number;
  timestamp: string;
  target: string;
  modelName: string;
  modelVersion: string;
}

/** The newest of the REST and live predictions, in one shape. */
export function latestEstimate(
  rest: Prediction | null | undefined,
  live: LivePrediction | undefined,
): Estimate | null {
  const useLive =
    live !== undefined && (!rest || Date.parse(live.timestamp) >= Date.parse(rest.timestamp));
  const source = useLive ? live : rest;
  if (!source) return null;
  return {
    pUp: source.class_probabilities?.up ?? null,
    direction: source.predicted_direction,
    horizonBars: source.horizon_bars,
    timestamp: source.timestamp,
    target: source.target_timestamp,
    modelName: source.model_name,
    modelVersion: source.model_version,
  };
}

export interface TrackRecord {
  resolved: number;
  pending: number;
  hitRate: number | null;
  /** Accuracy of always predicting the most common realised direction (the bar to beat). */
  majorityRate: number | null;
}

export function trackRecord(predictions: Prediction[]): TrackRecord {
  const scored = predictions.filter(
    (p) => p.outcome && p.outcome.actual_direction !== "neutral" && p.outcome.is_correct !== null,
  );
  const pending = predictions.filter((p) => !p.outcome).length;
  if (scored.length === 0) return { resolved: 0, pending, hitRate: null, majorityRate: null };
  const hits = scored.filter((p) => p.outcome?.is_correct === true).length;
  const ups = scored.filter((p) => p.outcome?.actual_direction === "up").length;
  return {
    resolved: scored.length,
    pending,
    hitRate: hits / scored.length,
    majorityRate: Math.max(ups, scored.length - ups) / scored.length,
  };
}
