import { describe, expect, it } from "vitest";

import { ago, duration, pctPoints, percent, price, signed, signedPct, trend } from "./format";

describe("format", () => {
  it("formats signed values with a true minus sign and no sign for zero", () => {
    expect(signed(1.234)).toBe("+1.23");
    expect(signed(-0.5)).toBe("−0.50");
    expect(signed(0.001)).toBe("0.00");
    expect(signedPct(0.0123)).toBe("+1.23%");
    expect(pctPoints(-1.5)).toBe("−1.50%");
  });

  it("uses a dash for missing values", () => {
    expect(price(null)).toBe("–");
    expect(percent(undefined)).toBe("–");
    expect(signed(Number.NaN)).toBe("–");
  });

  it("describes ages and durations", () => {
    const now = Date.parse("2026-10-11T12:00:00Z");
    expect(ago("2026-10-11T11:59:50Z", now)).toBe("10s ago");
    expect(ago("2026-10-11T09:00:00Z", now)).toBe("3h ago");
    expect(duration(125)).toBe("2m");
    expect(trend(-0.1)).toBe("down");
    expect(trend(0)).toBe("flat");
  });
});
