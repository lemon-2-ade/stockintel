import type { Anomaly, Severity } from "../api/types";
import { describeObserved } from "../lib/anomalies";
import { ago, humanize } from "../lib/format";
import { Badge, Empty, Panel, type Tone } from "./ui";

const severityTone: Record<Severity, Tone> = {
  low: "muted",
  medium: "warn",
  high: "alert",
  critical: "alert",
};

export interface AnomalyFeedProps {
  anomalies: Anomaly[];
  selected: string | null;
  onlySelected: boolean;
  onToggleOnlySelected: () => void;
  onSelect: (symbol: string) => void;
  now: number;
}

export function AnomalyFeed({
  anomalies,
  selected,
  onlySelected,
  onToggleOnlySelected,
  onSelect,
  now,
}: AnomalyFeedProps) {
  const rows =
    onlySelected && selected ? anomalies.filter((a) => a.symbol === selected) : anomalies;
  const filter = (
    <label className="flex items-center gap-1 text-xs text-muted">
      <input type="checkbox" checked={onlySelected} onChange={onToggleOnlySelected} />
      {selected ?? "selected"} only
    </label>
  );
  return (
    <Panel title="Anomalies (rule-based)" id="anomalies" actions={filter}>
      {rows.length === 0 ? (
        <Empty>No anomalies detected{onlySelected && selected ? ` for ${selected}` : ""}.</Empty>
      ) : (
        <ul
          className="max-h-80 space-y-1 overflow-auto"
          aria-live="polite"
          aria-relevant="additions"
        >
          {rows.map((a) => (
            <li key={a.anomaly_id} className="rounded border border-line px-2 py-1.5 text-xs">
              <div className="flex items-center gap-2">
                <Badge tone={severityTone[a.severity]}>{a.severity}</Badge>
                <button
                  className="font-semibold hover:underline"
                  onClick={() => {
                    onSelect(a.symbol);
                  }}
                >
                  {a.symbol}
                </button>
                <span>{humanize(a.anomaly_type)}</span>
                <span className="ml-auto text-muted">{ago(a.timestamp, now)}</span>
              </div>
              <div className="mt-0.5 text-muted">
                {describeObserved(a)} · {a.detector} v{a.detector_version}
              </div>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
