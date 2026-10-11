import type { ReactNode } from "react";

export function Panel({
  title,
  actions,
  children,
  className = "",
  id,
}: {
  title: string;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  id?: string;
}) {
  const headingId = id ? `${id}-title` : undefined;
  return (
    <section
      aria-labelledby={headingId}
      className={`flex min-w-0 flex-col rounded-lg border border-line bg-panel ${className}`}
    >
      <header className="flex items-center justify-between gap-2 border-b border-line px-3 py-2">
        <h2 id={headingId} className="text-xs font-semibold tracking-wide text-muted uppercase">
          {title}
        </h2>
        {actions}
      </header>
      <div className="min-h-0 flex-1 p-3">{children}</div>
    </section>
  );
}

export type Tone = "ok" | "warn" | "alert" | "muted";

const toneClass: Record<Tone, string> = {
  ok: "bg-ok",
  warn: "bg-warn",
  alert: "bg-alert",
  muted: "bg-muted",
};

const toneText: Record<Tone, string> = {
  ok: "text-ok",
  warn: "text-warn",
  alert: "text-alert",
  muted: "text-muted",
};

export function StatusDot({ tone, label }: { tone: Tone; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span aria-hidden className={`inline-block size-2 rounded-full ${toneClass[tone]}`} />
      <span className={`text-xs font-medium ${toneText[tone]}`}>{label}</span>
    </span>
  );
}

export function Badge({ tone = "muted", children }: { tone?: Tone; children: ReactNode }) {
  return (
    <span
      className={`inline-flex items-center rounded border border-current px-1.5 py-0.5 text-[11px] leading-none font-semibold uppercase ${toneText[tone]}`}
    >
      {children}
    </span>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="py-4 text-center text-sm text-muted">{children}</p>;
}

export function ErrorNote({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : "request failed";
  return (
    <p role="alert" className="text-sm text-alert">
      Could not load: {message}
    </p>
  );
}

export function Skeleton({ className = "h-4 w-full" }: { className?: string }) {
  return <div aria-hidden className={`animate-pulse rounded bg-line ${className}`} />;
}

export function Stat({ label, value, hint }: { label: string; value: ReactNode; hint?: string }) {
  return (
    <div className="min-w-0">
      <dt className="truncate text-[11px] text-muted uppercase" title={hint}>
        {label}
      </dt>
      <dd className="tabular truncate text-sm font-medium">{value}</dd>
    </div>
  );
}
