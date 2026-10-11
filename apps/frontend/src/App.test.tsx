import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";
import { anomaly, detail, model, monitoring, pipeline, stock } from "./test/fixtures";

// Canvas charts cannot render in jsdom: replace the library with inert stubs.
vi.mock("lightweight-charts", () => {
  const series = { setData: vi.fn(), update: vi.fn(), applyOptions: vi.fn() };
  const chart = {
    addSeries: vi.fn(() => series),
    applyOptions: vi.fn(),
    priceScale: vi.fn(() => ({ applyOptions: vi.fn() })),
    timeScale: vi.fn(() => ({ scrollToRealTime: vi.fn() })),
    remove: vi.fn(),
  };
  return {
    createChart: vi.fn(() => chart),
    CandlestickSeries: {},
    HistogramSeries: {},
    LineSeries: {},
    ColorType: { Solid: "solid" },
    CrosshairMode: { Normal: 0 },
    LineStyle: { Solid: 0, Dotted: 1, Dashed: 2 },
  };
});

class FakeSocket {
  static instances: FakeSocket[] = [];
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  sent: string[] = [];
  constructor(readonly url: string) {
    FakeSocket.instances.push(this);
  }
  send(data: string) {
    this.sent.push(data);
  }
  close() {
    this.readyState = 3;
  }
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
  receive(message: unknown) {
    this.onmessage?.({ data: JSON.stringify(message) });
  }
}

const responses: Record<string, unknown> = {
  "/api/v1/stocks": [stock("AAPL"), stock("MSFT")],
  "/api/v1/stocks/AAPL": detail("AAPL"),
  "/api/v1/stocks/AAPL/history": {
    symbol: "AAPL",
    interval: "1s",
    bars: [detail().observation],
    next_before: null,
  },
  "/api/v1/stocks/AAPL/indicators": { items: [], next_before: null },
  "/api/v1/stocks/AAPL/predictions": { items: [], next_before: null },
  "/api/v1/anomalies": { items: [anomaly({ symbol: "MSFT" })], next_before: null },
  "/api/v1/watchlist": [],
  "/api/v1/model": model,
  "/api/v1/monitoring": monitoring,
  "/api/v1/status": pipeline,
};

beforeEach(() => {
  FakeSocket.instances = [];
  vi.stubGlobal("WebSocket", FakeSocket);
  vi.stubGlobal(
    "fetch",
    vi.fn((input: string) => {
      const path = new URL(input, "http://localhost").pathname;
      if (!(path in responses)) return Promise.resolve(new Response("{}", { status: 404 }));
      return Promise.resolve(Response.json(responses[path]));
    }),
  );
  vi.stubGlobal("matchMedia", () => ({ matches: false }));
  window.location.hash = "";
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderApp() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <App />
    </QueryClientProvider>,
  );
}

describe("App", () => {
  it("loads the first symbol and keeps observation, analytics and model apart", async () => {
    renderApp();
    expect(await screen.findByRole("heading", { name: "AAPL" })).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: /Analytics \(deterministic\)/ }),
    ).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Model estimate/ })).toBeInTheDocument();
    expect(await screen.findByText(/no demonstrated edge/i)).toBeInTheDocument();
    expect(await screen.findByText("Stale: MSFT")).toBeInTheDocument();
  });

  it("applies live updates from the WebSocket", async () => {
    renderApp();
    await screen.findByRole("heading", { name: "AAPL" });
    const socket = FakeSocket.instances[0];
    expect(socket).toBeDefined();
    act(() => {
      socket?.open();
      socket?.receive({
        type: "updates",
        server_time: "2026-10-11T12:00:01Z",
        items: [
          {
            type: "bar",
            symbol: "AAPL",
            data: {
              symbol: "AAPL",
              timestamp: "2026-10-11T12:00:00Z",
              interval: "1s",
              open: 100,
              high: 111,
              low: 99,
              close: 110.25,
              volume: 5,
              indicators: { return_1: 0.05 },
            },
          },
          {
            type: "anomaly",
            symbol: "AAPL",
            data: anomaly({ symbol: "AAPL", anomaly_type: "price_drop" }),
          },
        ],
      });
    });
    const quote = screen.getByRole("region", { name: "Quote" });
    expect(within(quote).getByText("110.25")).toBeInTheDocument();
    expect(within(quote).getByText("live")).toBeInTheDocument();
    expect(screen.getByText("Price drop")).toBeInTheDocument();
    expect(screen.getByText("Live")).toBeInTheDocument();
  });
});

describe("App degradation", () => {
  it("keeps the last quote when the API stops answering", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    renderWith(client);
    await screen.findByRole("heading", { name: "AAPL" });
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(new Response("bad gateway", { status: 502 }))),
    );
    await act(async () => {
      await client.refetchQueries();
    });
    const quote = screen.getByRole("region", { name: "Quote" });
    expect(within(quote).getByText("100.50")).toBeInTheDocument();
    expect(within(quote).getByRole("status")).toHaveTextContent(/last data received/);
  });
});

function renderWith(client: QueryClient) {
  return render(
    <QueryClientProvider client={client}>
      <App />
    </QueryClientProvider>,
  );
}
