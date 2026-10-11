import { useCallback, useEffect, useState } from "react";

export type Theme = "light" | "dark";
const KEY = "stockintel.theme";

export function initialTheme(): Theme {
  try {
    const stored = localStorage.getItem(KEY);
    if (stored === "light" || stored === "dark") return stored;
  } catch {
    // storage unavailable (private mode): fall back to the system preference
  }
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

/** Apply a theme to the document. Called before the first render too, so canvas
 * charts created during mount already read the right colours. */
export function applyTheme(theme: Theme): void {
  document.documentElement.classList.toggle("dark", theme === "dark");
  document.documentElement.style.colorScheme = theme;
}

export function useTheme(): [Theme, () => void] {
  const [theme, setTheme] = useState<Theme>(initialTheme);
  useEffect(() => {
    applyTheme(theme);
    try {
      localStorage.setItem(KEY, theme);
    } catch {
      // ignore
    }
  }, [theme]);
  const toggle = useCallback(() => {
    setTheme((t) => (t === "dark" ? "light" : "dark"));
  }, []);
  return [theme, toggle];
}

/** Resolved colours for canvas-based charts, read from the CSS custom properties. */
export function chartColors(): Record<string, string> {
  const style = getComputedStyle(document.documentElement);
  const read = (name: string, fallback: string) => style.getPropertyValue(name).trim() || fallback;
  return {
    text: read("--sx-muted", "#64748b"),
    grid: read("--sx-grid", "#e2e8f0"),
    up: read("--sx-up", "#059669"),
    down: read("--sx-down", "#dc2626"),
    sma: read("--sx-series-1", "#2563eb"),
    ema: read("--sx-series-2", "#d97706"),
    band: read("--sx-series-3", "#7c3aed"),
    volumeUp: read("--sx-up-soft", "rgba(5,150,105,0.35)"),
    volumeDown: read("--sx-down-soft", "rgba(220,38,38,0.35)"),
  };
}
