import { useCallback, useEffect, useState } from "react";

const SYMBOL = /^[A-Z][A-Z0-9.-]{0,14}$/;

function fromHash(): string | null {
  const value = decodeURIComponent(window.location.hash.replace(/^#\/?/, "")).toUpperCase();
  return SYMBOL.test(value) ? value : null;
}

/** Selected symbol, mirrored in the URL hash (#/AAPL) so views can be bookmarked and shared. */
export function useSelectedSymbol(): [string | null, (symbol: string) => void] {
  const [symbol, setSymbol] = useState<string | null>(fromHash);
  useEffect(() => {
    const onHash = () => {
      setSymbol(fromHash());
    };
    window.addEventListener("hashchange", onHash);
    return () => {
      window.removeEventListener("hashchange", onHash);
    };
  }, []);
  const select = useCallback((next: string) => {
    window.location.hash = `/${next}`;
    setSymbol(next);
  }, []);
  return [symbol, select];
}
