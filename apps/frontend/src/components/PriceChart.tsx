import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  LineStyle,
  createChart,
  type IChartApi,
  type ISeriesApi,
} from "lightweight-charts";
import { useEffect, useRef, useState } from "react";

import type { Bar, IndicatorPoint, Indicators, LiveBar } from "../api/types";
import { type ChartPoint, mergeSeries } from "../lib/chartData";
import { chartColors, type Theme } from "../lib/theme";

type Overlay = "sma_20" | "ema_12" | "ema_26" | "bollinger";

const OVERLAYS: { key: Overlay; label: string }[] = [
  { key: "sma_20", label: "SMA 20" },
  { key: "ema_12", label: "EMA 12" },
  { key: "ema_26", label: "EMA 26" },
  { key: "bollinger", label: "Bollinger 20" },
];

interface SeriesSet {
  candles: ISeriesApi<"Candlestick">;
  volume: ISeriesApi<"Histogram">;
  lines: Record<"sma_20" | "ema_12" | "ema_26" | "bb_upper" | "bb_lower", ISeriesApi<"Line">>;
}

function lineData(points: ChartPoint[], field: keyof Indicators) {
  return points.flatMap((p) => {
    const value = p.indicators?.[field];
    return value === null || value === undefined ? [] : [{ time: p.time, value }];
  });
}

export interface PriceChartProps {
  symbol: string;
  bars: Bar[];
  indicators: IndicatorPoint[];
  liveBar: LiveBar | undefined;
  theme: Theme;
}

export function PriceChart({ symbol, bars, indicators, liveBar, theme }: PriceChartProps) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const series = useRef<SeriesSet | null>(null);
  const [overlays, setOverlays] = useState<Set<Overlay>>(new Set(["sma_20", "bollinger"]));

  // Create the chart once per mount.
  useEffect(() => {
    const el = container.current;
    if (!el) return;
    const c = createChart(el, {
      autoSize: true,
      layout: { background: { type: ColorType.Solid, color: "transparent" }, fontSize: 11 },
      rightPriceScale: { scaleMargins: { top: 0.08, bottom: 0.25 } },
      timeScale: { timeVisible: true, secondsVisible: true, rightOffset: 4 },
      crosshair: { mode: CrosshairMode.Normal },
      // Explicit locale: the browser default can be an invalid BCP 47 tag (e.g. "en-US@posix").
      localization: { locale: "en-US" },
    });
    const candles = c.addSeries(CandlestickSeries, { priceLineVisible: true });
    const volume = c.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceFormat: { type: "volume" },
      lastValueVisible: false,
      priceLineVisible: false,
    });
    c.priceScale("volume").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    const line = () =>
      c.addSeries(LineSeries, {
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
    series.current = {
      candles,
      volume,
      lines: { sma_20: line(), ema_12: line(), ema_26: line(), bb_upper: line(), bb_lower: line() },
    };
    chart.current = c;
    return () => {
      c.remove();
      chart.current = null;
      series.current = null;
    };
  }, []);

  // Colours follow the theme (canvas cannot read CSS variables by itself).
  useEffect(() => {
    const c = chart.current;
    const s = series.current;
    if (!c || !s) return;
    const colors = chartColors();
    c.applyOptions({
      layout: { textColor: colors.text },
      grid: { vertLines: { color: colors.grid }, horzLines: { color: colors.grid } },
    });
    s.candles.applyOptions({
      upColor: colors.up,
      downColor: colors.down,
      wickUpColor: colors.up,
      wickDownColor: colors.down,
      borderVisible: false,
    });
    s.lines.sma_20.applyOptions({ color: colors.sma });
    s.lines.ema_12.applyOptions({ color: colors.ema });
    s.lines.ema_26.applyOptions({ color: colors.ema, lineStyle: LineStyle.Dashed });
    s.lines.bb_upper.applyOptions({ color: colors.band, lineStyle: LineStyle.Dotted });
    s.lines.bb_lower.applyOptions({ color: colors.band, lineStyle: LineStyle.Dotted });
  }, [theme]);

  // Data: full reload when history changes, incremental update for the live bar.
  const points = mergeSeries(bars, indicators, liveBar);
  const loadedKey = useRef<string>("");
  useEffect(() => {
    const s = series.current;
    if (!s || points.length === 0) return;
    const colors = chartColors();
    const key = `${symbol}:${String(bars.length)}:${String(indicators.length)}:${String(points[0]?.time)}`;
    const last = points[points.length - 1];
    if (key !== loadedKey.current || !last) {
      loadedKey.current = key;
      s.candles.setData(points.map((p) => ({ time: p.time, ...ohlc(p.bar) })));
      s.volume.setData(points.map((p) => volumePoint(p, colors)));
      for (const field of ["sma_20", "ema_12", "ema_26", "bb_upper", "bb_lower"] as const) {
        s.lines[field].setData(lineData(points, field));
      }
      chart.current?.timeScale().scrollToRealTime();
      return;
    }
    s.candles.update({ time: last.time, ...ohlc(last.bar) });
    s.volume.update(volumePoint(last, colors));
    for (const field of ["sma_20", "ema_12", "ema_26", "bb_upper", "bb_lower"] as const) {
      const value = last.indicators?.[field];
      if (value !== null && value !== undefined) s.lines[field].update({ time: last.time, value });
    }
  });

  useEffect(() => {
    const s = series.current;
    if (!s) return;
    s.lines.sma_20.applyOptions({ visible: overlays.has("sma_20") });
    s.lines.ema_12.applyOptions({ visible: overlays.has("ema_12") });
    s.lines.ema_26.applyOptions({ visible: overlays.has("ema_26") });
    s.lines.bb_upper.applyOptions({ visible: overlays.has("bollinger") });
    s.lines.bb_lower.applyOptions({ visible: overlays.has("bollinger") });
  }, [overlays]);

  const toggle = (key: Overlay) => {
    setOverlays((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <div className="flex h-full flex-col gap-2">
      <div className="flex flex-wrap items-center gap-1" role="group" aria-label="Chart overlays">
        {OVERLAYS.map(({ key, label }) => (
          <button
            key={key}
            aria-pressed={overlays.has(key)}
            onClick={() => {
              toggle(key);
            }}
            className={`rounded border px-2 py-0.5 text-xs ${
              overlays.has(key) ? "border-ink/40 bg-line font-medium" : "border-line text-muted"
            }`}
          >
            {label}
          </button>
        ))}
        <span className="ml-auto text-xs text-muted">
          {points.length > 0 ? `${String(points.length)} bars · UTC` : "No bars yet"}
        </span>
      </div>
      <div
        ref={container}
        className="min-h-[320px] flex-1"
        role="img"
        aria-label={`Candlestick chart of ${symbol} with volume`}
      />
    </div>
  );
}

function ohlc(bar: Bar) {
  return { open: bar.open, high: bar.high, low: bar.low, close: bar.close };
}

function volumePoint(p: ChartPoint, colors: Record<string, string>) {
  return {
    time: p.time,
    value: p.bar.volume,
    color: p.bar.close >= p.bar.open ? colors.volumeUp : colors.volumeDown,
  };
}
