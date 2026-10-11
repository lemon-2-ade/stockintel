import { useMemo, useState } from "react";

import { useLiveMarket } from "./api/live";
import {
  useAnomalies,
  useHistory,
  useIndicatorHistory,
  useModel,
  useMonitoring,
  usePipelineStatus,
  usePredictions,
  useStock,
  useStocks,
  useToggleWatchlist,
  useWatchlist,
} from "./api/queries";
import type { Anomaly } from "./api/types";
import { AnomalyFeed } from "./components/AnomalyFeed";
import { Header } from "./components/Header";
import { MlPanel, PredictionHistory } from "./components/MlPanel";
import { PipelineStatusPanel } from "./components/PipelineStatus";
import { PriceChart } from "./components/PriceChart";
import { IndicatorGrid, QuoteHeader } from "./components/QuotePanel";
import { SymbolList } from "./components/SymbolList";
import { Empty, ErrorNote, Panel } from "./components/ui";
import { latestEstimate } from "./lib/predictions";
import { useSelectedSymbol } from "./lib/selection";
import { useTheme } from "./lib/theme";
import { useNow } from "./lib/useNow";

function mergeAnomalies(live: Anomaly[], rest: Anomaly[] | undefined): Anomaly[] {
  const seen = new Set<string>();
  return [...live, ...(rest ?? [])]
    .filter((a) => (seen.has(a.anomaly_id) ? false : (seen.add(a.anomaly_id), true)))
    .sort((a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp))
    .slice(0, 100);
}

export default function App() {
  const [theme, toggleTheme] = useTheme();
  const now = useNow();
  const [chosen, select] = useSelectedSymbol();
  const [onlySelected, setOnlySelected] = useState(false);

  const stocks = useStocks();
  const symbols = useMemo(
    () => (stocks.data ?? []).map((s) => s.symbol).slice(0, 50),
    [stocks.data],
  );
  const selected = chosen ?? symbols[0] ?? null;
  const live = useLiveMarket(symbols);

  const detail = useStock(selected);
  const interval = detail.data?.observation.interval ?? null;
  const history = useHistory(selected, interval);
  const indicatorHistory = useIndicatorHistory(selected, interval);
  const predictions = usePredictions(selected);
  const anomalies = useAnomalies();
  const model = useModel();
  const monitoring = useMonitoring();
  const status = usePipelineStatus();
  const watchlist = useWatchlist();
  const toggleWatch = useToggleWatchlist();
  const watched = useMemo(
    () => new Set((watchlist.data ?? []).map((w) => w.symbol)),
    [watchlist.data],
  );

  const liveBar = selected ? live.bars[selected] : undefined;
  const liveIndicators =
    liveBar &&
    detail.data &&
    Date.parse(liveBar.timestamp) >= Date.parse(detail.data.observation.timestamp)
      ? liveBar.indicators
      : detail.data?.analytics;
  const estimate = latestEstimate(
    detail.data?.prediction,
    selected ? live.predictions[selected] : undefined,
  );

  return (
    <div className="flex min-h-full flex-col">
      <Header connection={live.status} theme={theme} onToggleTheme={toggleTheme} />
      <main className="grid flex-1 gap-3 p-3 lg:grid-cols-[240px_minmax(0,1fr)_340px]">
        <div className="lg:row-span-2">
          <SymbolList
            stocks={stocks.data}
            live={live.bars}
            watchlist={watched}
            selected={selected}
            loading={stocks.isPending}
            onSelect={select}
            onToggleWatch={(symbol, isWatched) => {
              toggleWatch.mutate({ symbol, watched: isWatched });
            }}
          />
        </div>

        <div className="flex min-w-0 flex-col gap-3">
          {stocks.isError && !stocks.data && <ErrorNote error={stocks.error} />}
          {!selected && !stocks.isPending && (
            <Panel title="Market">
              <Empty>No symbols are streaming yet. Start the stack with `make dev`.</Empty>
            </Panel>
          )}
          {selected && (
            <>
              <section className="rounded-lg border border-line bg-panel p-3" aria-label="Quote">
                {detail.isError && !detail.data ? (
                  <ErrorNote error={detail.error} />
                ) : (
                  <QuoteHeader
                    detail={detail.data}
                    liveBar={liveBar}
                    loading={detail.isPending}
                    now={now}
                    connected={live.status === "open"}
                  />
                )}
                {detail.isError && detail.data && (
                  <p role="status" className="mt-2 text-xs text-warn">
                    Showing the last data received; the API is not responding.
                  </p>
                )}
              </section>
              <Panel title="Price" id="chart" className="min-h-[420px]">
                <PriceChart
                  symbol={selected}
                  bars={history.data?.bars ?? []}
                  indicators={indicatorHistory.data?.items ?? []}
                  liveBar={liveBar}
                  theme={theme}
                />
              </Panel>
              <div className="grid gap-3 xl:grid-cols-2">
                <Panel title="Analytics (deterministic)" id="indicators">
                  <IndicatorGrid values={liveIndicators} />
                </Panel>
                <Panel title="Recent predictions" id="history">
                  <PredictionHistory items={predictions.data?.items ?? []} />
                </Panel>
              </div>
            </>
          )}
        </div>

        <div className="flex min-w-0 flex-col gap-3">
          <MlPanel
            estimate={estimate}
            history={predictions.data?.items ?? []}
            model={model.data}
            monitoring={monitoring.data}
            now={now}
          />
          <AnomalyFeed
            anomalies={mergeAnomalies(live.anomalies, anomalies.data?.items)}
            selected={selected}
            onlySelected={onlySelected}
            onToggleOnlySelected={() => {
              setOnlySelected((v) => !v);
            }}
            onSelect={select}
            now={now}
          />
          <PipelineStatusPanel
            status={status.data}
            error={status.isError}
            connection={live.status}
          />
        </div>
      </main>
      <footer className="px-4 pb-3 text-[11px] text-muted">
        Prices are simulated unless the producer replays historical data. Analytics are computed
        from observed bars; model estimates are experimental and not investment advice. Charts by
        TradingView Lightweight Charts.
      </footer>
    </div>
  );
}
