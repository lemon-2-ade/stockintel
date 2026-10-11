const priceFmt = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const compactFmt = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

export const dash = "–";

export function price(value: number | null | undefined): string {
  return value === null || value === undefined ? dash : priceFmt.format(value);
}

/** Signed number with fixed decimals: +1.23 / −0.45 (true minus sign). */
export function signed(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return dash;
  const text = Math.abs(value).toFixed(digits);
  if (Number(text) === 0) return text;
  return value > 0 ? `+${text}` : `−${text}`;
}

/** A ratio as a signed percentage: 0.0123 -> +1.23%. */
export function signedPct(ratio: number | null | undefined, digits = 2): string {
  return ratio === null || ratio === undefined ? dash : `${signed(ratio * 100, digits)}%`;
}

/** A value already in percent (the API's change_pct): 1.23 -> +1.23%. */
export function pctPoints(value: number | null | undefined, digits = 2): string {
  return value === null || value === undefined ? dash : `${signed(value, digits)}%`;
}

export function percent(ratio: number | null | undefined, digits = 1): string {
  return ratio === null || ratio === undefined ? dash : `${(ratio * 100).toFixed(digits)}%`;
}

export function compact(value: number | null | undefined): string {
  return value === null || value === undefined ? dash : compactFmt.format(value);
}

export function fixed(value: number | null | undefined, digits = 2): string {
  return value === null || value === undefined ? dash : value.toFixed(digits);
}

/** "12s ago", "4m ago", "3h ago", "2d ago". */
export function ago(iso: string | null | undefined, now: number = Date.now()): string {
  if (!iso) return dash;
  const seconds = Math.max(0, Math.round((now - Date.parse(iso)) / 1000));
  if (seconds < 60) return `${String(seconds)}s ago`;
  if (seconds < 3_600) return `${String(Math.floor(seconds / 60))}m ago`;
  if (seconds < 86_400) return `${String(Math.floor(seconds / 3_600))}h ago`;
  return `${String(Math.floor(seconds / 86_400))}d ago`;
}

export function duration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return dash;
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${String(s)}s`;
  if (s < 3_600) return `${String(Math.floor(s / 60))}m`;
  if (s < 86_400) return `${String(Math.floor(s / 3_600))}h`;
  return `${String(Math.floor(s / 86_400))}d`;
}

export function time(iso: string | null | undefined): string {
  if (!iso) return dash;
  return new Date(iso).toLocaleString("en-GB", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZone: "UTC",
    hour12: false,
  });
}

export type Trend = "up" | "down" | "flat";

/** Direction of a value as it will be displayed: anything that rounds to 0 is flat. */
export function trend(value: number | null | undefined, digits = 2): Trend {
  if (value === null || value === undefined) return "flat";
  const shown = Number(value.toFixed(digits));
  if (shown === 0) return "flat";
  return shown > 0 ? "up" : "down";
}

export function humanize(identifier: string): string {
  const text = identifier.replaceAll("_", " ");
  return text.charAt(0).toUpperCase() + text.slice(1);
}
