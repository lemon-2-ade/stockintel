import { useEffect, useReducer, useRef } from "react";

import { liveUrl } from "./client";
import type { Anomaly, LiveBar, LiveItem, LivePrediction, ServerMessage } from "./types";

export type ConnectionStatus = "connecting" | "open" | "reconnecting" | "offline";

export interface LiveState {
  status: ConnectionStatus;
  bars: Record<string, LiveBar>;
  predictions: Record<string, LivePrediction>;
  anomalies: Anomaly[];
  lastMessageAt: number | null;
}

export const MAX_ANOMALIES = 100;

export const initialLiveState: LiveState = {
  status: "connecting",
  bars: {},
  predictions: {},
  anomalies: [],
  lastMessageAt: null,
};

export type LiveAction =
  | { kind: "status"; status: ConnectionStatus }
  | { kind: "items"; items: LiveItem[]; at: number }
  | { kind: "tick"; at: number };

const newer = (a: { timestamp: string } | undefined, b: { timestamp: string }): boolean =>
  a === undefined || Date.parse(b.timestamp) >= Date.parse(a.timestamp);

/**
 * Pure state transition for live messages.
 *
 * Bars and predictions keep only the newest per symbol (an out-of-order
 * message never replaces a newer one); anomalies are a de-duplicated,
 * newest-first feed capped at MAX_ANOMALIES.
 */
export function liveReducer(state: LiveState, action: LiveAction): LiveState {
  switch (action.kind) {
    case "status":
      return state.status === action.status ? state : { ...state, status: action.status };
    case "tick":
      return { ...state, lastMessageAt: action.at };
    case "items": {
      let bars = state.bars;
      let predictions = state.predictions;
      let anomalies = state.anomalies;
      for (const item of action.items) {
        if (item.type === "bar") {
          if (newer(bars[item.symbol], item.data)) bars = { ...bars, [item.symbol]: item.data };
        } else if (item.type === "prediction") {
          if (newer(predictions[item.symbol], item.data))
            predictions = { ...predictions, [item.symbol]: item.data };
        } else if (!anomalies.some((a) => a.anomaly_id === item.data.anomaly_id)) {
          anomalies = [item.data, ...anomalies];
        }
      }
      if (anomalies !== state.anomalies) {
        anomalies = [...anomalies]
          .sort((a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp))
          .slice(0, MAX_ANOMALIES);
      }
      return { ...state, bars, predictions, anomalies, lastMessageAt: action.at };
    }
  }
}

/** Reconnect delay: exponential from 1 s, capped at 30 s, with +-20% jitter. */
export function backoffMs(attempt: number, random: () => number = Math.random): number {
  const base = Math.min(30_000, 1_000 * 2 ** attempt);
  return Math.round(base * (0.8 + 0.4 * random()));
}

const STALE_CONNECTION_MS = 45_000; // server heartbeats every 15 s

/**
 * One WebSocket for the whole dashboard. Reconnects with backoff, re-subscribes
 * when the symbol set changes, and treats 45 s of silence as a dead connection.
 */
export function useLiveMarket(symbols: string[]): LiveState {
  const [state, dispatch] = useReducer(liveReducer, initialLiveState);
  const socketRef = useRef<WebSocket | null>(null);
  const symbolsRef = useRef<string[]>(symbols);
  const key = [...symbols].sort().join(",");

  useEffect(() => {
    symbolsRef.current = key ? key.split(",") : [];
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ action: "subscribe", symbols: symbolsRef.current }));
    }
  }, [key]);

  useEffect(() => {
    let attempt = 0;
    let closed = false;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let lastSeen = Date.now();

    const watchdog = setInterval(() => {
      if (
        socketRef.current?.readyState === WebSocket.OPEN &&
        Date.now() - lastSeen > STALE_CONNECTION_MS
      ) {
        socketRef.current.close();
      }
    }, 5_000);

    const connect = (): void => {
      if (closed) return;
      const socket = new WebSocket(liveUrl(symbolsRef.current));
      socketRef.current = socket;
      socket.onopen = () => {
        attempt = 0;
        lastSeen = Date.now();
        dispatch({ kind: "status", status: "open" });
      };
      socket.onmessage = (event: MessageEvent<string>) => {
        lastSeen = Date.now();
        let message: ServerMessage;
        try {
          message = JSON.parse(event.data) as ServerMessage;
        } catch {
          return;
        }
        if (message.type === "snapshot" || message.type === "updates") {
          dispatch({ kind: "items", items: message.items, at: lastSeen });
        } else {
          dispatch({ kind: "tick", at: lastSeen });
        }
      };
      socket.onclose = () => {
        if (closed) return;
        dispatch({ kind: "status", status: attempt === 0 ? "reconnecting" : "offline" });
        retry = setTimeout(connect, backoffMs(attempt));
        attempt += 1;
      };
      socket.onerror = () => {
        socket.close();
      };
    };

    connect();
    return () => {
      closed = true;
      clearInterval(watchdog);
      if (retry !== undefined) clearTimeout(retry);
      socketRef.current?.close();
      socketRef.current = null;
    };
  }, []);

  return state;
}
