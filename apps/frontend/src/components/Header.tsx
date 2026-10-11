import type { ConnectionStatus } from "../api/live";
import type { Theme } from "../lib/theme";
import { StatusDot, type Tone } from "./ui";

const label: Record<ConnectionStatus, [Tone, string]> = {
  open: ["ok", "Live"],
  connecting: ["muted", "Connecting"],
  reconnecting: ["warn", "Reconnecting"],
  offline: ["alert", "Offline"],
};

export function Header({
  connection,
  theme,
  onToggleTheme,
}: {
  connection: ConnectionStatus;
  theme: Theme;
  onToggleTheme: () => void;
}) {
  const [tone, text] = label[connection];
  return (
    <header className="flex items-center justify-between gap-4 border-b border-line bg-panel px-4 py-2">
      <div className="flex items-baseline gap-3">
        <span className="text-base font-bold tracking-tight">Stock Intelligence</span>
        <span className="hidden text-xs text-muted sm:inline">
          Simulated market data · deterministic analytics · experimental model estimates
        </span>
      </div>
      <div className="flex items-center gap-4">
        <span aria-live="polite">
          <StatusDot tone={tone} label={text} />
        </span>
        <button
          onClick={onToggleTheme}
          className="rounded border border-line px-2 py-1 text-xs hover:bg-line"
          aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} theme`}
        >
          {theme === "dark" ? "☀ Light" : "☾ Dark"}
        </button>
      </div>
    </header>
  );
}
