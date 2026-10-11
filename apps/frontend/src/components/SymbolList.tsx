import { useState } from "react";

import type { LiveBar, StockSummary } from "../api/types";
import { pctPoints, price, trend } from "../lib/format";
import { Empty, Panel, Skeleton } from "./ui";

type Tab = "watchlist" | "all";

export interface SymbolListProps {
  stocks: StockSummary[] | undefined;
  live: Record<string, LiveBar>;
  watchlist: Set<string>;
  selected: string | null;
  loading: boolean;
  onSelect: (symbol: string) => void;
  onToggleWatch: (symbol: string, watched: boolean) => void;
}

export function SymbolList({
  stocks,
  live,
  watchlist,
  selected,
  loading,
  onSelect,
  onToggleWatch,
}: SymbolListProps) {
  const [tab, setTab] = useState<Tab>("all");
  const rows = (stocks ?? []).filter((s) => tab === "all" || watchlist.has(s.symbol));

  const tabs = (
    <div role="tablist" aria-label="Symbol lists" className="flex gap-1 text-xs">
      {(["all", "watchlist"] as const).map((t) => (
        <button
          key={t}
          role="tab"
          aria-selected={tab === t}
          onClick={() => {
            setTab(t);
          }}
          className={`rounded px-2 py-0.5 capitalize ${tab === t ? "bg-line font-semibold" : "text-muted hover:text-ink"}`}
        >
          {t === "all" ? "All" : `Watchlist (${String(watchlist.size)})`}
        </button>
      ))}
    </div>
  );

  return (
    <Panel title="Symbols" actions={tabs} id="symbols" className="h-full">
      {loading && (
        <div className="space-y-2">
          {Array.from({ length: 8 }, (_, i) => (
            <Skeleton key={i} className="h-8 w-full" />
          ))}
        </div>
      )}
      {!loading && rows.length === 0 && (
        <Empty>{tab === "watchlist" ? "Star a symbol to add it here." : "No symbols yet."}</Empty>
      )}
      <ul className="-mx-1 max-h-72 space-y-0.5 overflow-auto lg:max-h-none">
        {rows.map((stock) => {
          const bar = live[stock.symbol];
          const last = bar?.close ?? stock.last_price;
          const liveReturn = bar?.indicators?.return_1;
          // Change vs the previous bar (same definition as the API's change_pct).
          const changePct = liveReturn != null ? liveReturn * 100 : stock.change_pct;
          const direction = trend(changePct);
          const watched = watchlist.has(stock.symbol);
          return (
            <li key={stock.symbol} className="flex items-center gap-1">
              <button
                onClick={() => {
                  onToggleWatch(stock.symbol, watched);
                }}
                aria-label={
                  watched
                    ? `Remove ${stock.symbol} from watchlist`
                    : `Add ${stock.symbol} to watchlist`
                }
                aria-pressed={watched}
                className={`px-1 text-base leading-none ${watched ? "text-warn" : "text-muted hover:text-ink"}`}
              >
                {watched ? "★" : "☆"}
              </button>
              <button
                onClick={() => {
                  onSelect(stock.symbol);
                }}
                aria-current={selected === stock.symbol ? "true" : undefined}
                className={`grid flex-1 grid-cols-[1fr_auto] items-baseline gap-x-2 rounded px-2 py-1.5 text-left ${
                  selected === stock.symbol ? "bg-line" : "hover:bg-line/60"
                }`}
              >
                <span className="truncate font-semibold">
                  {stock.symbol}
                  {stock.stale && (
                    <span className="ml-1 text-[10px] font-normal text-warn" title="No recent bars">
                      stale
                    </span>
                  )}
                </span>
                <span className="tabular text-right text-sm">{price(last)}</span>
                <span className="truncate text-xs text-muted">{stock.name ?? ""}</span>
                <span
                  className={`tabular text-right text-xs ${
                    direction === "up"
                      ? "text-up"
                      : direction === "down"
                        ? "text-down"
                        : "text-muted"
                  }`}
                >
                  {direction === "up" ? "▲ " : direction === "down" ? "▼ " : ""}
                  {pctPoints(changePct)}
                </span>
              </button>
            </li>
          );
        })}
      </ul>
    </Panel>
  );
}
