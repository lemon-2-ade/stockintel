import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "./client";

export const keys = {
  stocks: ["stocks"] as const,
  stock: (s: string) => ["stock", s] as const,
  history: (s: string, i: string) => ["history", s, i] as const,
  indicators: (s: string, i: string) => ["indicators", s, i] as const,
  predictions: (s: string) => ["predictions", s] as const,
  anomalies: ["anomalies"] as const,
  watchlist: ["watchlist"] as const,
  model: ["model"] as const,
  monitoring: ["monitoring"] as const,
  status: ["status"] as const,
};

export const useStocks = () =>
  useQuery({ queryKey: keys.stocks, queryFn: api.stocks, refetchInterval: 15_000 });

export const useStock = (symbol: string | null) =>
  useQuery({
    queryKey: keys.stock(symbol ?? ""),
    queryFn: () => api.stock(symbol ?? ""),
    enabled: symbol !== null,
    refetchInterval: 10_000,
  });

// History and indicator series load once per symbol/interval; live bars are appended.
export const useHistory = (symbol: string | null, interval: string | null) =>
  useQuery({
    queryKey: keys.history(symbol ?? "", interval ?? ""),
    queryFn: () => api.history(symbol ?? "", interval ?? ""),
    enabled: symbol !== null && interval !== null,
    staleTime: Infinity,
  });

export const useIndicatorHistory = (symbol: string | null, interval: string | null) =>
  useQuery({
    queryKey: keys.indicators(symbol ?? "", interval ?? ""),
    queryFn: () => api.indicators(symbol ?? "", interval ?? ""),
    enabled: symbol !== null && interval !== null,
    staleTime: Infinity,
  });

export const usePredictions = (symbol: string | null) =>
  useQuery({
    queryKey: keys.predictions(symbol ?? ""),
    queryFn: () => api.predictions(symbol ?? ""),
    enabled: symbol !== null,
    refetchInterval: 30_000,
  });

export const useAnomalies = () =>
  useQuery({ queryKey: keys.anomalies, queryFn: () => api.anomalies(50), refetchInterval: 60_000 });

export const useModel = () =>
  useQuery({ queryKey: keys.model, queryFn: api.model, refetchInterval: 60_000 });

export const useMonitoring = () =>
  useQuery({ queryKey: keys.monitoring, queryFn: api.monitoring, refetchInterval: 60_000 });

export const usePipelineStatus = () =>
  useQuery({ queryKey: keys.status, queryFn: api.status, refetchInterval: 10_000 });

export const useWatchlist = () => useQuery({ queryKey: keys.watchlist, queryFn: api.watchlist });

export function useToggleWatchlist() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ symbol, watched }: { symbol: string; watched: boolean }) =>
      watched ? api.removeFromWatchlist(symbol) : api.addToWatchlist(symbol),
    onSettled: () => client.invalidateQueries({ queryKey: keys.watchlist }),
  });
}
