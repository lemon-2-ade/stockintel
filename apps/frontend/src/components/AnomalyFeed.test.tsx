import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { describeObserved } from "../lib/anomalies";
import { NOW, anomaly } from "../test/fixtures";
import { AnomalyFeed } from "./AnomalyFeed";

describe("AnomalyFeed", () => {
  const items = [
    anomaly({ symbol: "AAPL" }),
    anomaly({
      symbol: "MSFT",
      anomaly_type: "volume_spike",
      severity: "medium",
      score: 8.2,
      observed_value: 9_000,
    }),
  ];

  it("lists anomalies and selects a symbol on click", async () => {
    const onSelect = vi.fn();
    render(
      <AnomalyFeed
        anomalies={items}
        selected="AAPL"
        onlySelected={false}
        onToggleOnlySelected={vi.fn()}
        onSelect={onSelect}
        now={NOW}
      />,
    );
    expect(screen.getByText("Price spike")).toBeInTheDocument();
    expect(screen.getByText(/8\.2× its rolling median/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "MSFT" }));
    expect(onSelect).toHaveBeenCalledWith("MSFT");
  });

  it("filters to the selected symbol", () => {
    render(
      <AnomalyFeed
        anomalies={items}
        selected="AAPL"
        onlySelected
        onToggleOnlySelected={vi.fn()}
        onSelect={vi.fn()}
        now={NOW}
      />,
    );
    expect(screen.queryByRole("button", { name: "MSFT" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "AAPL" })).toBeInTheDocument();
  });

  it("describes each detector in its own units", () => {
    expect(describeObserved(anomaly())).toBe("+2.10% in one bar");
    expect(
      describeObserved(
        anomaly({ anomaly_type: "return_zscore", observed_value: -0.03, score: 6.1 }),
      ),
    ).toBe("−3.00% log return, |z| = 6.1");
  });
});
