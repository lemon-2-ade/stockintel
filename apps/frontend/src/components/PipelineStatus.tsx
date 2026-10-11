import type { ConnectionStatus } from "../api/live";
import type { PipelineStatus as Status, StageStatus } from "../api/types";
import { duration } from "../lib/format";
import { Panel, Skeleton, StatusDot, type Tone } from "./ui";

const stageTone = (s: StageStatus): Tone =>
  s.status === "ok" ? "ok" : s.status === "stale" ? "warn" : "muted";

const connectionTone: Record<ConnectionStatus, Tone> = {
  open: "ok",
  connecting: "muted",
  reconnecting: "warn",
  offline: "alert",
};

function Row({
  label,
  tone,
  value,
  detail,
}: {
  label: string;
  tone: Tone;
  value: string;
  detail?: string;
}) {
  return (
    <li className="flex items-center justify-between gap-2 py-1">
      <span className="text-sm">{label}</span>
      <span className="flex items-center gap-2 text-right">
        {detail && <span className="text-xs text-muted">{detail}</span>}
        <StatusDot tone={tone} label={value} />
      </span>
    </li>
  );
}

function stageRow(label: string, stage: StageStatus) {
  return (
    <Row
      label={label}
      tone={stageTone(stage)}
      value={stage.status}
      detail={
        stage.age_seconds !== null
          ? `${duration(stage.age_seconds)} old`
          : (stage.detail ?? undefined)
      }
    />
  );
}

export interface PipelineStatusProps {
  status: Status | undefined;
  error: boolean;
  connection: ConnectionStatus;
}

export function PipelineStatusPanel({ status, error, connection }: PipelineStatusProps) {
  return (
    <Panel title="Pipeline status" id="pipeline">
      {error && (
        <p role="alert" className="mb-2 text-sm text-alert">
          API unreachable.{status ? " Rows below are the last known state." : ""}
        </p>
      )}
      <ul className={`divide-y divide-line ${error ? "opacity-60" : ""}`}>
        <Row
          label="Live updates (WebSocket)"
          tone={connectionTone[connection]}
          value={connection}
        />
        {!status && !error && (
          <li className="py-2">
            <Skeleton className="h-16 w-full" />
          </li>
        )}
        {status && (
          <>
            {Object.entries(status.stores).map(([store, state]) => (
              <Row
                key={store}
                label={store === "postgres" ? "PostgreSQL" : store === "redis" ? "Redis" : store}
                tone={state === "ok" ? "ok" : "alert"}
                value={state === "ok" ? "ok" : "down"}
              />
            ))}
            {stageRow("Market data", status.market_data)}
            {stageRow("Predictions", status.predictions)}
            {stageRow("Model monitor", status.monitoring)}
            <Row
              label="Served model"
              tone={status.model ? "ok" : "muted"}
              value={status.model ? `v${status.model.version}` : "none"}
              detail={status.model?.name}
            />
          </>
        )}
      </ul>
      {status && status.stale_symbols.length > 0 && (
        <p className="mt-2 text-xs text-warn">Stale: {status.stale_symbols.join(", ")}</p>
      )}
    </Panel>
  );
}
