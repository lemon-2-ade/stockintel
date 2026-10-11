import type { Anomaly } from "../api/types";
import { signedPct } from "./format";

/** What the detector saw, in its own units (see services/stream-processor detectors). */
export function describeObserved(a: Anomaly): string {
  switch (a.anomaly_type) {
    case "price_spike":
    case "price_drop":
      return `${signedPct(a.observed_value)} in one bar`;
    case "return_zscore":
      return `${signedPct(a.observed_value)} log return, |z| = ${a.score.toFixed(1)}`;
    case "volume_spike":
      return `volume ${a.score.toFixed(1)}× its rolling median`;
    default:
      return `score ${a.score.toFixed(2)}`;
  }
}
