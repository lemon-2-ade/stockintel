import type { UTCTimestamp } from "lightweight-charts";

import type { Bar, IndicatorPoint, Indicators, LiveBar } from "../api/types";

export const toTime = (iso: string): UTCTimestamp =>
  Math.floor(Date.parse(iso) / 1000) as UTCTimestamp;

export interface ChartPoint {
  time: UTCTimestamp;
  bar: Bar;
  indicators: Indicators | null;
}

/** Merge history, indicator history and the live bar into one ascending, de-duplicated series. */
export function mergeSeries(
  bars: Bar[],
  indicators: IndicatorPoint[],
  liveBar: LiveBar | undefined,
): ChartPoint[] {
  const byTime = new Map<number, ChartPoint>();
  const values = new Map<number, Indicators>(
    indicators.map((p) => [toTime(p.timestamp), p.values]),
  );
  for (const bar of bars) {
    const time = toTime(bar.timestamp);
    byTime.set(time, { time, bar, indicators: values.get(time) ?? null });
  }
  if (liveBar) {
    const time = toTime(liveBar.timestamp);
    byTime.set(time, { time, bar: liveBar, indicators: liveBar.indicators ?? null });
  }
  return [...byTime.values()].sort((a, b) => a.time - b.time);
}
