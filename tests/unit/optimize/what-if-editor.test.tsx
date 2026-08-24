import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { WhatIfEditor } from "../../../app/components/what-if-editor";
import { proposalGroup, proposalIds } from "./fixtures";

describe("what-if analysis", () => {
  it("submits Decimal strings for only the selected proposal universe", async () => {
    const group = proposalGroup();
    const onEvaluate = vi.fn().mockResolvedValue({
      policy_version: "optimizer_v1",
      instrument_count: 2,
      result: {
        diagnostics: {
          volatility: { status: "unknown", value: null, unit: "basis_points", reason: "VOLATILITY_REQUIRES_PRICE_HISTORY" },
        },
        constraints: { violation_count: { status: "known", value: 0, unit: "count", reason: null } },
      },
    });
    render(<WhatIfEditor group={group} proposalId={proposalIds.minimum_change} onEvaluate={onEvaluate} />);

    const weightInputs = screen.getAllByRole("textbox", { name: /目标权重/ });
    expect(weightInputs).toHaveLength(2);
    fireEvent.change(weightInputs[0], { target: { value: "0.1400" } });
    fireEvent.click(screen.getByRole("button", { name: "评估情景" }));

    await waitFor(() => expect(onEvaluate).toHaveBeenCalledTimes(1));
    expect(onEvaluate.mock.calls[0][0]).toEqual({
      proposal_group_id: group.proposal_group_id,
      proposal_id: proposalIds.minimum_change,
      policy_version: "optimizer_v1",
      positions: [
        { instrument_id: group.proposals[1].positions[0].instrument_id, weight: "0.1400" },
        { instrument_id: group.proposals[1].positions[1].instrument_id, weight: "0.10" },
      ],
    });
    expect(screen.getByText(/仅分析，不持久化/)).toBeVisible();
    expect(screen.getByText(/VOLATILITY_REQUIRES_PRICE_HISTORY/)).toBeVisible();
    expect(screen.queryByText(/下单|交易/)).not.toBeInTheDocument();
  });

  it("rejects malformed decimals locally without calculating risk", () => {
    const onEvaluate = vi.fn();
    render(<WhatIfEditor group={proposalGroup()} proposalId={proposalIds.minimum_change} onEvaluate={onEvaluate} />);

    fireEvent.change(screen.getAllByRole("textbox", { name: /目标权重/ })[0], { target: { value: "NaN" } });
    fireEvent.click(screen.getByRole("button", { name: "评估情景" }));

    expect(screen.getByRole("alert")).toHaveTextContent("请输入 0 到 1 之间的十进制权重");
    expect(onEvaluate).not.toHaveBeenCalled();
  });
});
