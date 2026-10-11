import type {
  Anomaly,
  History,
  IndicatorPoint,
  ModelStatus,
  MonitoringSummary,
  Page,
  PipelineStatus,
  Prediction,
  StockDetail,
  StockSummary,
  WatchlistItem,
} from "./types";

/** Base URL of the API. Empty = same origin (the Vite dev proxy or a reverse proxy). */
export const API_BASE: string = import.meta.env.VITE_API_URL ?? "";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

interface ErrorBody {
  error?: { code?: string; message?: string };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set("Accept", "application/json");
  const response = await fetch(`${API_BASE}${path}`, { ...init, headers });
  if (!response.ok) {
    let code = "http_error";
    let message = `${String(response.status)} ${response.statusText}`;
    try {
      const body = (await response.json()) as ErrorBody;
      code = body.error?.code ?? code;
      message = body.error?.message ?? message;
    } catch {
      // non-JSON error body: keep the status line
    }
    throw new ApiError(response.status, code, message);
  }
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

const q = (params: Record<string, string | number | undefined>): string => {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) search.set(key, String(value));
  }
  const text = search.toString();
  return text ? `?${text}` : "";
};

export const api = {
  stocks: () => request<StockSummary[]>("/api/v1/stocks"),
  stock: (symbol: string) => request<StockDetail>(`/api/v1/stocks/${symbol}`),
  history: (symbol: string, interval: string, limit = 500) =>
    request<History>(`/api/v1/stocks/${symbol}/history${q({ interval, limit })}`),
  indicators: (symbol: string, interval: string, limit = 500) =>
    request<Page<IndicatorPoint>>(`/api/v1/stocks/${symbol}/indicators${q({ interval, limit })}`),
  predictions: (symbol: string, limit = 50) =>
    request<Page<Prediction>>(`/api/v1/stocks/${symbol}/predictions${q({ limit })}`),
  anomalies: (limit = 50) => request<Page<Anomaly>>(`/api/v1/anomalies${q({ limit })}`),
  watchlist: () => request<WatchlistItem[]>("/api/v1/watchlist"),
  addToWatchlist: (symbol: string) =>
    request<WatchlistItem>("/api/v1/watchlist", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbol }),
    }),
  removeFromWatchlist: (symbol: string) =>
    request<undefined>(`/api/v1/watchlist/${symbol}`, { method: "DELETE" }),
  model: () => request<ModelStatus | null>("/api/v1/model"),
  monitoring: () => request<MonitoringSummary | null>("/api/v1/monitoring"),
  status: () => request<PipelineStatus>("/api/v1/status"),
};

/** WebSocket URL for live updates, derived from the API base or the page origin. */
export function liveUrl(symbols: string[]): string {
  const base = API_BASE || window.location.origin;
  const url = new URL("/ws/market", base);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("symbols", symbols.join(","));
  return url.toString();
}
