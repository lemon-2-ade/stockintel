import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { latestEstimate } from "../lib/predictions";
import { NOW, model, monitoring, prediction } from "../test/fixtures";
import { MlPanel, PredictionHistory } from "./MlPanel";

describe("MlPanel", () => {
  it("labels the estimate as experimental and shows the base rate next to it", () => {
    render(
      <MlPanel
        estimate={latestEstimate(prediction(), undefined)}
        history={[]}
        model={model}
        monitoring={monitoring}
        now={NOW}
      />,
    );
    expect(screen.getByText("experimental")).toBeInTheDocument();
    expect(screen.getByText("58.0%")).toBeInTheDocument();
    expect(screen.getByText(/56\.4% up/)).toBeInTheDocument();
    expect(screen.getByRole("meter", { name: /probability of an up move/i })).toHaveAttribute(
      "aria-valuenow",
      "58",
    );
    expect(screen.getByText(/no demonstrated edge/i)).toBeInTheDocument();
    expect(screen.getByText(/“beats base rate”/)).toBeInTheDocument();
    expect(screen.getByText(/Not investment advice/)).toBeInTheDocument();
  });

  it("summarises monitoring and the retraining signal", () => {
    render(
      <MlPanel estimate={null} history={[]} model={model} monitoring={monitoring} now={NOW} />,
    );
    expect(screen.getByText("2 alert")).toBeInTheDocument();
    expect(screen.getByText(/vol_10, ret_5/)).toBeInTheDocument();
    expect(screen.getByText(/Retraining recommended/)).toBeInTheDocument();
    expect(screen.getByText("+0.0032")).toBeInTheDocument();
  });

  it("explains a missing estimate instead of showing a number", () => {
    render(<MlPanel estimate={null} history={[]} model={null} monitoring={null} now={NOW} />);
    expect(screen.getByText(/No fresh estimate/)).toBeInTheDocument();
    expect(screen.queryByRole("meter")).not.toBeInTheDocument();
    expect(screen.getByText(/has not reported yet/)).toBeInTheDocument();
  });

  it("compares the live hit rate with the majority baseline", () => {
    const outcome = (direction: "up" | "down", correct: boolean) =>
      prediction({
        outcome: {
          actual_direction: direction,
          actual_return: 0.01,
          is_correct: correct,
          resolved_at: "2026-10-11T12:00:00Z",
        },
      });
    render(
      <MlPanel
        estimate={null}
        history={[
          outcome("up", true),
          outcome("up", false),
          outcome("up", true),
          outcome("down", false),
        ]}
        model={model}
        monitoring={null}
        now={NOW}
      />,
    );
    expect(screen.getByText(/correct of/)).toHaveTextContent("50.0% correct of 4 resolved");
    expect(screen.getByText("75.0%")).toBeInTheDocument();
  });
});

describe("PredictionHistory", () => {
  it("marks hits, misses and pending outcomes", () => {
    render(
      <PredictionHistory
        items={[
          prediction({
            outcome: {
              actual_direction: "up",
              actual_return: 0.012,
              is_correct: true,
              resolved_at: "x",
            },
          }),
          prediction({
            outcome: {
              actual_direction: "down",
              actual_return: -0.004,
              is_correct: false,
              resolved_at: "x",
            },
          }),
          prediction(),
        ]}
      />,
    );
    expect(screen.getByLabelText("correct")).toBeInTheDocument();
    expect(screen.getByLabelText("wrong")).toBeInTheDocument();
    expect(screen.getByText("pending")).toBeInTheDocument();
    expect(screen.getByText("+1.20%")).toBeInTheDocument();
  });
});
