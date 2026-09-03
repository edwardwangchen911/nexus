import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { WhatIfEditor } from "../../../app/components/what-if-editor";
import { proposalGroup, proposalIds } from "./fixtures";

describe("what-if analysis", () => {
  it("uses the trusted display symbol and retains a missing-name fallback", () => {
    const group = proposalGroup();
    Object.assign(group.proposals[1].positions[0], { display_symbol: "AAPL" });
    Object.assign(group.proposals[1].positions[1], { display_symbol: null });
    render(<WhatIfEditor group={group} proposalId={proposalIds.minimum_change} onEvaluate={vi.fn()} />);
    expect(screen.getByRole("textbox", { name: "AAPL 目标权重（0–1）" })).toBeVisible();
    expect(screen.getByRole("textbox", { name: "标的 2 目标权重（0–1）" })).toBeVisible();
    expect(screen.getByText("证券名称未提供，请先核对方案中的标的。")).toBeVisible();
  });
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
    expect(screen.getByText(/试算不会保存修改或生成订单/)).toBeVisible();
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

  it("uses the same eighteen-decimal limit as the BFF", () => {
    const onEvaluate = vi.fn();
    render(<WhatIfEditor group={proposalGroup()} proposalId={proposalIds.minimum_change} actionsEnabled onEvaluate={onEvaluate} />);

    fireEvent.change(screen.getAllByRole("textbox", { name: /目标权重/ })[0], { target: { value: "0.1234567890123456789" } });
    fireEvent.click(screen.getByRole("button", { name: "评估情景" }));

    expect(screen.getByRole("alert")).toHaveTextContent("最多 18 位小数");
    expect(onEvaluate).not.toHaveBeenCalled();
  });

  it("keeps a proposal with more than thirty positions readable but disables what-if", () => {
    const group = proposalGroup();
    const minimumChange = group.proposals[1];
    minimumChange.positions = Array.from({ length: 31 }, (_, index) => ({
      ...minimumChange.positions[0],
      instrument_id: `0198d500-1000-7000-8000-${String(index + 1).padStart(12, "0")}`,
    }));

    render(<WhatIfEditor group={group} proposalId={proposalIds.minimum_change} actionsEnabled onEvaluate={vi.fn()} />);

    expect(screen.getByText(/试算最多支持 30 个标的/)).toBeVisible();
    expect(screen.queryByRole("textbox", { name: /目标权重/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "评估情景" })).not.toBeInTheDocument();
  });
});
