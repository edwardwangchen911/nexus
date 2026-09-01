import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { DecisionPath } from "../../../app/components/decision-path";

describe("decision evidence path", () => {
  it.each([
    ["research", "研究证据"],
    ["diagnose", "组合诊断"],
    ["what-if", "情景试算"],
    ["paper", "内部模拟"],
    ["review", "离线审核"],
  ] as const)("marks %s as the current real workflow stage", (stage, label) => {
    render(<DecisionPath current={stage} />);

    const path = screen.getByRole("region", { name: "投资决策证据链，可横向滚动" });
    expect(within(path).getAllByRole("listitem")).toHaveLength(5);
    expect(within(path).getByText(label).closest("li")).toHaveAttribute("aria-current", "step");
    expect(path).toHaveAttribute("tabindex", "0");
  });

  it("brings a late current stage into the narrow-screen viewport", () => {
    const scrollIntoView = vi.fn();
    const original = HTMLElement.prototype.scrollIntoView;
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", { configurable: true, value: scrollIntoView });

    render(<DecisionPath current="review" />);

    expect(scrollIntoView).toHaveBeenCalledWith({ block: "nearest", inline: "center" });
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", { configurable: true, value: original });
  });
});
