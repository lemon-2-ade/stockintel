import type { ModelStatus, MonitoringSummary, Prediction } from "../api/types";
import { type Estimate, trackRecord } from "../lib/predictions";
import { ago, humanize, percent, signed, signedPct, time } from "../lib/format";
import { Badge, Empty, Panel, StatusDot } from "./ui";

function ProbabilityBar({ pUp, baseRate }: { pUp: number; baseRate: number | null }) {
  return (
    <div>
      <div
        className="relative h-3 overflow-hidden rounded bg-line"
        role="meter"
        aria-label="Estimated probability of an up move"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={Math.round(pUp * 100)}
      >
        <div
          className="absolute inset-y-0 left-0 bg-up/70"
          style={{ width: `${String(pUp * 100)}%` }}
        />
        <div className="absolute inset-y-0 left-1/2 w-px bg-ink/50" aria-hidden />
        {baseRate !== null && (
          <div
            className="absolute -inset-y-0.5 w-0.5 bg-warn"
            style={{ left: `${String(baseRate * 100)}%` }}
            title="Base rate"
            aria-hidden
          />
        )}
      </div>
      <div className="mt-1 flex justify-between text-[11px] text-muted">
        <span>0%</span>
        <span>50% (coin flip)</span>
        <span>100%</span>
      </div>
    </div>
  );
}

export interface MlPanelProps {
  estimate: Estimate | null;
  history: Prediction[];
  model: ModelStatus | null | undefined;
  monitoring: MonitoringSummary | null | undefined;
  now: number;
}

export function MlPanel({ estimate, history, model, monitoring, now }: MlPanelProps) {
  const baseRate = model?.evaluation.test_base_rate ?? null;
  const record = trackRecord(history);
  const drifted = (monitoring?.items ?? [])
    .filter((i) => i.report_type === "data_drift" && i.status === "alert")
    .sort((a, b) => b.value - a.value);
  const perf = monitoring?.items.find((i) => i.metric === "log_loss_minus_null");

  return (
    <Panel title="Model estimate" id="ml" actions={<Badge tone="warn">experimental</Badge>}>
      <div className="space-y-4">
        {model && model.overridden_gates.length > 0 && (
          <p className="rounded border border-warn/50 bg-warn/10 p-2 text-xs">
            Deployed with an overridden quality gate (
            {model.overridden_gates.map((g) => `“${humanize(g).toLowerCase()}”`).join(", ")}): the
            model has <strong>no demonstrated edge</strong> over always predicting the most common
            outcome. It is shown to exercise the platform, not to inform decisions.
          </p>
        )}

        {!estimate && (
          <Empty>
            No fresh estimate. The model may not be served yet, or this symbol is still in its
            61-bar feature warm-up.
          </Empty>
        )}

        {estimate && estimate.pUp !== null && (
          <div className="space-y-2">
            <div className="flex items-baseline justify-between gap-2">
              <span className="text-sm">P(up over next {estimate.horizonBars} bars)</span>
              <span className="tabular text-2xl font-semibold">{percent(estimate.pUp)}</span>
            </div>
            <ProbabilityBar pUp={estimate.pUp} baseRate={baseRate} />
            <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
              <dt className="text-muted">Leaning</dt>
              <dd className={estimate.direction === "up" ? "text-up" : "text-down"}>
                {estimate.direction === "up" ? "▲ up" : "▼ down"}
              </dd>
              {baseRate !== null && (
                <>
                  <dt className="text-muted">Historical base rate</dt>
                  <dd>
                    {percent(baseRate)} up <span className="text-muted">(orange mark)</span>
                  </dd>
                </>
              )}
              <dt className="text-muted">As of / target</dt>
              <dd className="tabular">
                {time(estimate.timestamp)} → {time(estimate.target)}
              </dd>
              <dt className="text-muted">Model</dt>
              <dd>
                {estimate.modelName} v{estimate.modelVersion}
              </dd>
            </dl>
          </div>
        )}

        <div>
          <h3 className="mb-1 text-xs font-semibold text-muted uppercase">Live track record</h3>
          {record.resolved === 0 ? (
            <p className="text-xs text-muted">
              No resolved predictions yet ({record.pending} awaiting their target time).
            </p>
          ) : (
            <p className="text-sm">
              <span className="tabular font-semibold">{percent(record.hitRate)}</span> correct of{" "}
              {record.resolved} resolved; always picking the most common outcome would score{" "}
              <span className="tabular font-semibold">{percent(record.majorityRate)}</span>.
            </p>
          )}
        </div>

        <div>
          <h3 className="mb-1 text-xs font-semibold text-muted uppercase">Monitoring</h3>
          {!monitoring ? (
            <p className="text-xs text-muted">The model monitor has not reported yet.</p>
          ) : (
            <div className="space-y-1.5 text-xs">
              <div className="flex flex-wrap gap-3">
                <StatusDot tone="ok" label={`${String(monitoring.status_counts.ok)} ok`} />
                <StatusDot
                  tone="warn"
                  label={`${String(monitoring.status_counts.warning)} warning`}
                />
                <StatusDot tone="alert" label={`${String(monitoring.status_counts.alert)} alert`} />
                <span className="text-muted">checked {ago(monitoring.computed_at, now)}</span>
              </div>
              {drifted.length >= 10 && (
                <p className="text-muted">
                  Broad drift is expected when the stream is intraday or simulated: the model was
                  trained on real daily bars.
                </p>
              )}
              {drifted.length > 0 && (
                <p>
                  Inputs drifted from training:{" "}
                  {drifted
                    .slice(0, 5)
                    .map((d) => d.feature ?? "")
                    .join(", ")}
                  {drifted.length > 5 ? ` and ${String(drifted.length - 5)} more` : ""}.
                </p>
              )}
              {perf && (
                <p>
                  Live log loss minus the base-rate forecast&apos;s:{" "}
                  <span className="tabular font-medium">{signed(perf.value, 4)}</span>{" "}
                  <span className="text-muted">(above 0 = worse than the base rate)</span>
                </p>
              )}
              {monitoring.retrain_recommended && (
                <p className="text-warn">
                  Retraining recommended. A retrained model still needs an explicit, audited
                  promotion before it serves.
                </p>
              )}
            </div>
          )}
        </div>

        <p className="text-[11px] leading-snug text-muted">
          {model?.disclaimer ??
            "Statistical model estimate for research and demonstration only. Not investment advice."}
        </p>
      </div>
    </Panel>
  );
}

export function PredictionHistory({ items }: { items: Prediction[] }) {
  if (items.length === 0) return <Empty>No predictions recorded for this symbol yet.</Empty>;
  return (
    <div className="max-h-64 overflow-auto">
      <table className="w-full text-left text-xs">
        <caption className="sr-only">Recent predictions and their outcomes</caption>
        <thead className="sticky top-0 bg-panel text-muted">
          <tr>
            <th className="px-1.5 py-1 font-medium">As of (UTC)</th>
            <th className="px-1.5 py-1 text-right font-medium">P(up)</th>
            <th className="px-1.5 py-1 font-medium">Leaning</th>
            <th className="px-1.5 py-1 font-medium">Actual</th>
            <th className="px-1.5 py-1 text-right font-medium">Return</th>
            <th className="px-1.5 py-1 text-center font-medium">Hit</th>
          </tr>
        </thead>
        <tbody className="tabular">
          {items.map((p) => (
            <tr key={p.prediction_id} className="border-t border-line">
              <td className="px-1.5 py-1">{time(p.timestamp)}</td>
              <td className="px-1.5 py-1 text-right">
                {percent(p.class_probabilities?.up ?? null)}
              </td>
              <td className="px-1.5 py-1">{p.predicted_direction ?? "–"}</td>
              <td className="px-1.5 py-1">
                {p.outcome?.actual_direction ?? <span className="text-muted">pending</span>}
              </td>
              <td className="px-1.5 py-1 text-right">
                {p.outcome ? signedPct(p.outcome.actual_return) : "–"}
              </td>
              <td className="px-1.5 py-1 text-center">
                {p.outcome?.is_correct === true && (
                  <span className="text-up" aria-label="correct">
                    ✓
                  </span>
                )}
                {p.outcome?.is_correct === false && (
                  <span className="text-down" aria-label="wrong">
                    ✗
                  </span>
                )}
                {(p.outcome?.is_correct ?? null) === null && <span className="text-muted">–</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
