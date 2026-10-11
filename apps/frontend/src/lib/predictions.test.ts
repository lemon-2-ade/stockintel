import { describe, expect, it } from "vitest";

import type { LivePrediction } from "../api/types";
import { prediction } from "../test/fixtures";
import { latestEstimate, trackRecord } from "./predictions";

const resolved = (predicted: "up" | "down", actual: "up" | "down" | "neutral") =>
  prediction({
    predicted_direction: predicted,
    outcome: {
      actual_direction: actual,
      actual_return: actual === "up" ? 0.01 : actual === "down" ? -0.01 : 0,
      is_correct: actual === "neutral" ? null : predicted === actual,
      resolved_at: "2026-10-11T12:00:00Z",
    },
  });

describe("trackRecord", () => {
  it("compares the hit rate with always predicting the majority outcome", () => {
    const record = trackRecord([
      resolved("up", "up"),
      resolved("up", "up"),
      resolved("up", "down"),
      resolved("down", "up"),
      resolved("up", "neutral"), // excluded: no direction
      prediction(), // pending
    ]);
    expect(record).toEqual({ resolved: 4, pending: 1, hitRate: 0.5, majorityRate: 0.75 });
  });

  it("reports nothing until outcomes exist", () => {
    expect(trackRecord([prediction()]).hitRate).toBeNull();
  });
});

describe("latestEstimate", () => {
  it("prefers the newer of the REST and live predictions", () => {
    const rest = prediction({ timestamp: "2026-10-11T11:00:00Z" });
    const live: LivePrediction = {
      prediction_id: "x",
      symbol: "AAPL",
      timestamp: "2026-10-11T11:30:00Z",
      target_timestamp: "2026-10-11T11:30:05Z",
      horizon_bars: 5,
      predicted_direction: "down",
      class_probabilities: { up: 0.4, down: 0.6 },
      confidence: 0.6,
      model_name: "m",
      model_version: "4",
    };
    expect(latestEstimate(rest, live)?.modelVersion).toBe("4");
    expect(latestEstimate(rest, { ...live, timestamp: "2026-10-11T10:00:00Z" })?.modelVersion).toBe(
      "3",
    );
    expect(latestEstimate(null, undefined)).toBeNull();
  });
});
