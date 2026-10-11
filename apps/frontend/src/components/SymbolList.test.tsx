import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import type { LiveBar } from "../api/types";
import { stock } from "../test/fixtures";
import { SymbolList } from "./SymbolList";

const stocks = [
  stock("AAPL", { change_pct: 1.2 }),
  stock("MSFT", { change_pct: -0.4, stale: true }),
];

describe("SymbolList", () => {
  it("shows live prices, direction with text not only colour, and staleness", () => {
    const live: Record<string, LiveBar> = {
      AAPL: {
        symbol: "AAPL",
        timestamp: "2026-10-11T12:00:00Z",
        interval: "1s",
        open: 1,
        high: 1,
        low: 1,
        close: 123.45,
        volume: 1,
        indicators: { return_1: 0.002 },
      },
    };
    render(
      <SymbolList
        stocks={stocks}
        live={live}
        watchlist={new Set()}
        selected="AAPL"
        loading={false}
        onSelect={vi.fn()}
        onToggleWatch={vi.fn()}
      />,
    );
    expect(screen.getByText("123.45")).toBeInTheDocument();
    expect(screen.getByText(/▲/)).toHaveTextContent("▲ +0.20%");
    expect(screen.getByText(/▼/)).toHaveTextContent("−0.40%");
    expect(screen.getByText("stale")).toBeInTheDocument();
  });

  it("toggles the watchlist and filters by it", async () => {
    const onToggle = vi.fn();
    render(
      <SymbolList
        stocks={stocks}
        live={{}}
        watchlist={new Set(["MSFT"])}
        selected={null}
        loading={false}
        onSelect={vi.fn()}
        onToggleWatch={onToggle}
      />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Add AAPL to watchlist" }));
    expect(onToggle).toHaveBeenCalledWith("AAPL", false);
    await userEvent.click(screen.getByRole("tab", { name: /Watchlist/ }));
    expect(screen.queryByText("AAPL")).not.toBeInTheDocument();
    expect(screen.getByText("MSFT")).toBeInTheDocument();
  });
});
