import { describe, expect, it } from "vitest";

import type { Bar, LiveBar } from "../api/types";
import { mergeSeries } from "./chartData";

const bar = (iso: string, close: number): Bar => ({
  timestamp: iso,
  interval: "1s",
  open: close,
  high: close,
  low: close,
  close,
  volume: 10,
});

describe("mergeSeries", () => {
  it("sorts, de-duplicates and lets the live bar replace the same timestamp", () => {
    const bars = [bar("2026-01-01T00:00:02Z", 2), bar("2026-01-01T00:00:01Z", 1)];
    const live: LiveBar = {
      ...bar("2026-01-01T00:00:02Z", 2.5),
      symbol: "AAPL",
      indicators: { sma_20: 1.5 },
    };
    const points = mergeSeries(
      bars,
      [{ timestamp: "2026-01-01T00:00:01Z", indicator_version: "1", values: { sma_20: 1 } }],
      live,
    );
    expect(points.map((p) => p.bar.close)).toEqual([1, 2.5]);
    expect(points.map((p) => p.indicators?.sma_20)).toEqual([1, 1.5]);
    expect(points[0]?.time).toBe(Date.parse("2026-01-01T00:00:01Z") / 1000);
  });

  it("appends a newer live bar", () => {
    const live: LiveBar = { ...bar("2026-01-01T00:00:05Z", 5), symbol: "AAPL" };
    expect(mergeSeries([bar("2026-01-01T00:00:01Z", 1)], [], live)).toHaveLength(2);
  });
});
