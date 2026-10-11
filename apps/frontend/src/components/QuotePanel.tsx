import type { Indicators, LiveBar, StockDetail } from "../api/types";
import { ago, compact, fixed, pctPoints, percent, price, signedPct, trend } from "../lib/format";
import { Badge, Skeleton, Stat } from "./ui";

export interface QuoteProps {
  detail: StockDetail | undefined;
  liveBar: LiveBar | undefined;
  loading: boolean;
  now: number;
  /** Whether the live connection is up; without it a cached live bar is not "live". */
  connected?: boolean;
}

/** Headline quote: observation (what happened), clearly separate from analytics and the model. */
export function QuoteHeader({ detail, liveBar, loading, now, connected = true }: QuoteProps) {
  if (loading || !detail) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-7 w-48" />
        <Skeleton className="h-4 w-72" />
      </div>
    );
  }
  const live = liveBar && Date.parse(liveBar.timestamp) >= Date.parse(detail.observation.timestamp);
  const bar = live ? liveBar : detail.observation;
  const ret = live ? liveBar.indicators?.return_1 : detail.analytics?.return_1;
  const direction = trend(ret === null || ret === undefined ? ret : ret * 100, 3);
  const stale = live ? false : detail.freshness.stale;
  const session = detail.session;

  return (
    <div className="flex flex-wrap items-end justify-between gap-x-6 gap-y-2">
      <div>
        <div className="flex items-baseline gap-2">
          <h1 className="text-xl font-bold">{detail.symbol}</h1>
          <span className="truncate text-sm text-muted">{detail.name ?? ""}</span>
          {stale ? (
            <Badge tone="warn">stale</Badge>
          ) : !connected ? (
            <Badge tone="warn">not live</Badge>
          ) : (
            <Badge tone="ok">{live ? "live" : "fresh"}</Badge>
          )}
        </div>
        <div className="flex items-baseline gap-3">
          <span className="tabular text-3xl font-semibold">{price(bar.close)}</span>
          <span
            className={`tabular text-sm font-medium ${
              direction === "up" ? "text-up" : direction === "down" ? "text-down" : "text-muted"
            }`}
          >
            {direction === "up" ? "▲" : direction === "down" ? "▼" : ""} {signedPct(ret, 3)}{" "}
            <span className="text-muted">vs prev. bar</span>
          </span>
        </div>
        <p className="text-xs text-muted">
          {bar.interval} bar opened {ago(bar.timestamp, now)} · volume {compact(bar.volume)}
        </p>
      </div>
      {session && (
        <dl
          className="grid grid-cols-3 gap-x-4 gap-y-1 sm:grid-cols-5"
          aria-label="Session (UTC day)"
        >
          <Stat label="Open" value={price(session.open)} />
          <Stat label="High" value={price(session.high)} />
          <Stat label="Low" value={price(session.low)} />
          <Stat label="Day chg" value={pctPoints(session.change_pct)} />
          <Stat label="Volume" value={compact(session.volume)} />
        </dl>
      )}
    </div>
  );
}

function rsiTone(rsi: number): string {
  if (rsi >= 70) return "text-down";
  if (rsi <= 30) return "text-up";
  return "";
}

/** Deterministic analytics computed by the stream processor (not predictions). */
export function IndicatorGrid({ values }: { values: Indicators | null | undefined }) {
  if (!values)
    return <p className="text-sm text-muted">Indicators appear after the warm-up bars.</p>;
  const rsi = values.rsi_14;
  return (
    <div className="space-y-3">
      {rsi !== null && rsi !== undefined && (
        <div>
          <div className="mb-1 flex justify-between text-xs">
            <span className="text-muted">RSI 14</span>
            <span className={`tabular font-medium ${rsiTone(rsi)}`}>{rsi.toFixed(1)}</span>
          </div>
          <div
            className="relative h-2 rounded bg-line"
            role="meter"
            aria-label="RSI 14"
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={Math.round(rsi)}
          >
            <div className="absolute inset-y-0 left-[30%] w-[40%] bg-ink/10" aria-hidden />
            <div
              className="absolute top-1/2 size-3 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-panel bg-ink"
              style={{ left: `${String(Math.min(100, Math.max(0, rsi)))}%` }}
              aria-hidden
            />
          </div>
          <div className="mt-0.5 flex justify-between text-[10px] text-muted">
            <span>0</span>
            <span>30</span>
            <span>70</span>
            <span>100</span>
          </div>
        </div>
      )}
      <dl className="grid grid-cols-2 gap-x-4 gap-y-2 sm:grid-cols-3">
        <Stat label="SMA 20" value={price(values.sma_20)} />
        <Stat label="EMA 12 / 26" value={`${price(values.ema_12)} / ${price(values.ema_26)}`} />
        <Stat label="MACD" value={fixed(values.macd, 3)} hint="EMA12 - EMA26" />
        <Stat label="MACD signal" value={fixed(values.macd_signal, 3)} />
        <Stat label="MACD hist." value={fixed(values.macd_hist, 3)} />
        <Stat
          label="Volatility 20"
          value={percent(values.volatility_20, 3)}
          hint="Std-dev of 1-bar log returns (not annualised)"
        />
        <Stat label="BB upper" value={price(values.bb_upper)} />
        <Stat label="BB lower" value={price(values.bb_lower)} />
        <Stat
          label="Volume ratio"
          value={fixed(values.volume_ratio, 2)}
          hint="volume / 20-bar average"
        />
      </dl>
    </div>
  );
}
