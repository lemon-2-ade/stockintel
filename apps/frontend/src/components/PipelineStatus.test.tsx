import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { pipeline } from "../test/fixtures";
import { PipelineStatusPanel } from "./PipelineStatus";

describe("PipelineStatusPanel", () => {
  it("shows each stage with its freshness", () => {
    render(<PipelineStatusPanel status={pipeline} error={false} connection="open" />);
    const row = (label: string) => screen.getByText(label).closest("li") as HTMLElement;
    expect(within(row("Market data")).getByText("ok")).toBeInTheDocument();
    expect(within(row("Predictions")).getByText("stale")).toBeInTheDocument();
    expect(within(row("Predictions")).getByText("10m old")).toBeInTheDocument();
    expect(within(row("Model monitor")).getByText("missing")).toBeInTheDocument();
    expect(within(row("Served model")).getByText("v3")).toBeInTheDocument();
    expect(screen.getByText("Stale: MSFT")).toBeInTheDocument();
  });

  it("degrades visibly when the API is unreachable", () => {
    render(<PipelineStatusPanel status={undefined} error connection="reconnecting" />);
    expect(screen.getByRole("alert")).toHaveTextContent(/API unreachable/);
    expect(screen.getByRole("alert")).not.toHaveTextContent(/last known/);
    expect(screen.getByText("reconnecting")).toBeInTheDocument();
  });

  it("marks the previous status as last known when a refresh fails", () => {
    render(<PipelineStatusPanel status={pipeline} error connection="offline" />);
    expect(screen.getByRole("alert")).toHaveTextContent(/last known state/);
  });
});
