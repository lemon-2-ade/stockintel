import { describe, expect, it } from "vitest";

import { anomaly } from "../test/fixtures";
import { MAX_ANOMALIES, backoffMs, initialLiveState, liveReducer } from "./live";
import type { LiveItem } from "./types";

const bar = (timestamp: string, close: number): LiveItem => ({
  type: "bar",
  symbol: "AAPL",
  data: {
    symbol: "AAPL",
    timestamp,
    interval: "1s",
    open: close,
    high: close,
    low: close,
    close,
    volume: 1,
  },
});

describe("liveReducer", () => {
  it("keeps the newest bar per symbol, even if messages arrive out of order", () => {
    let state = liveReducer(initialLiveState, {
      kind: "items",
      items: [bar("2026-01-01T00:00:02Z", 102)],
      at: 1,
    });
    state = liveReducer(state, { kind: "items", items: [bar("2026-01-01T00:00:01Z", 101)], at: 2 });
    expect(state.bars.AAPL?.close).toBe(102);
    state = liveReducer(state, { kind: "items", items: [bar("2026-01-01T00:00:03Z", 103)], at: 3 });
    expect(state.bars.AAPL?.close).toBe(103);
  });

  it("de-duplicates anomalies, sorts newest first and caps the feed", () => {
    const a = anomaly({ timestamp: "2026-01-01T00:00:01Z" });
    const items: LiveItem[] = [
      { type: "anomaly", symbol: "AAPL", data: a },
      { type: "anomaly", symbol: "AAPL", data: a },
      ...Array.from({ length: MAX_ANOMALIES + 5 }, (_, i): LiveItem => ({
        type: "anomaly",
        symbol: "MSFT",
        data: anomaly({
          symbol: "MSFT",
          timestamp: new Date(Date.UTC(2026, 0, 2, 0, 0, i)).toISOString(),
        }),
      })),
    ];
    const state = liveReducer(initialLiveState, { kind: "items", items, at: 1 });
    expect(state.anomalies).toHaveLength(MAX_ANOMALIES);
    expect(state.anomalies[0]?.symbol).toBe("MSFT");
    expect(new Set(state.anomalies.map((x) => x.anomaly_id)).size).toBe(MAX_ANOMALIES);
  });

  it("only changes status when it differs", () => {
    const open = liveReducer(initialLiveState, { kind: "status", status: "open" });
    expect(open.status).toBe("open");
    expect(liveReducer(open, { kind: "status", status: "open" })).toBe(open);
  });
});

describe("backoffMs", () => {
  it("grows exponentially to a 30 s cap with bounded jitter", () => {
    expect(backoffMs(0, () => 0.5)).toBe(1_000);
    expect(backoffMs(3, () => 0.5)).toBe(8_000);
    expect(backoffMs(10, () => 0.5)).toBe(30_000);
    expect(backoffMs(0, () => 0)).toBe(800);
    expect(backoffMs(0, () => 1)).toBe(1_200);
  });
});
