import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ProposalComparison } from "../../../app/components/proposal-comparison";
import { eligibleProposalGroup, proposalGroup } from "./fixtures";

describe("three-way proposal comparison", () => {
  it("uses percent weights and coefficient HHI without promising blocked group eligibility", () => {
    const { container } = render(<ProposalComparison group={proposalGroup()} onStartShadow={vi.fn()} onReject={vi.fn()} />);
    const row = (label: string) => screen.getByRole("rowheader", { name: label }).closest("tr");
    expect(row("最大单项权重")).toHaveTextContent("20%");
    expect(row("持仓集中度指数（HHI）")).toHaveTextContent("0.52");
    expect(row("持仓集中度指数（HHI）")).not.toHaveTextContent("52%");
    expect(container).not.toHaveTextContent("可进入影子验证");
  });

  it("does not advertise shadow eligibility when cached data disables the action", () => {
    render(<ProposalComparison group={eligibleProposalGroup()} startShadowEnabled={false} onStartShadow={vi.fn()} onReject={vi.fn()} />);
    expect(screen.queryByText("可进入影子验证")).not.toBeInTheDocument();
  });

  it("renders exactly three ordered semantic columns and no trading control", () => {
    render(<ProposalComparison group={proposalGroup()} onStartShadow={vi.fn()} onReject={vi.fn()} />);

    const headers = screen.getAllByRole("columnheader", { name: /保持现状|最小调整|策略目标/ });
    expect(headers).toHaveLength(3);
    expect(headers.map((header) => header.textContent)).toEqual(["保持现状", "最小调整", "策略目标"]);
    expect(screen.getAllByText("仅分析").length).toBeGreaterThan(0);
    expect(screen.getAllByText(/COST_NOT_PROVEN/).length).toBeGreaterThan(0);
    expect(screen.getAllByTitle("TAX_DATA_UNAVAILABLE").length).toBe(3);
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
  });

  it("requires a separate explicit confirmation before starting shadow validation", async () => {
    const onStartShadow = vi.fn().mockResolvedValue(undefined);
    const eligible = eligibleProposalGroup();
    render(<ProposalComparison group={eligible} onStartShadow={onStartShadow} onReject={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "启动影子验证" }));
    expect(onStartShadow).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog", { name: "确认启动影子验证" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "确认启动影子验证" }));
    await waitFor(() => expect(onStartShadow).toHaveBeenCalledWith(eligible.proposal_group_id, eligible.version));
  });

  it.each(["expired", "rejected", "validated"] as const)(
    "prevents new lifecycle actions for a %s group",
    (status) => {
      render(<ProposalComparison group={proposalGroup({ status })} onStartShadow={vi.fn()} onReject={vi.fn()} />);

      expect(screen.queryByRole("button", { name: "启动影子验证" })).not.toBeInTheDocument();
      expect(screen.getAllByText(({ expired: "已过期", rejected: "已拒绝", validated: "已验证" } as const)[status]).length).toBeGreaterThan(0);
    },
  );

  it("does not re-offer shadow validation but still allows rejection while shadowing", async () => {
    const onReject = vi.fn().mockResolvedValue(undefined);
    const group = proposalGroup({ status: "shadowing", shadow_eligibility: { eligible: true, reasons: [] } });
    render(<ProposalComparison group={group} startShadowEnabled rejectEnabled onStartShadow={vi.fn()} onReject={onReject} />);

    expect(screen.queryByRole("button", { name: "启动影子验证" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "拒绝方案组" }));
    await waitFor(() => expect(onReject).toHaveBeenCalledWith(group.proposal_group_id, group.version));
    expect(screen.getByText(/当前状态 观察中/)).toBeVisible();
  });

  it("closes an outstanding confirmation when the data becomes stale", () => {
    const onStartShadow = vi.fn();
    const eligible = eligibleProposalGroup();
    const { rerender } = render(<ProposalComparison group={eligible} startShadowEnabled rejectEnabled onStartShadow={onStartShadow} onReject={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "启动影子验证" }));
    expect(screen.getByRole("dialog", { name: "确认启动影子验证" })).toBeVisible();
    rerender(<ProposalComparison group={eligible} startShadowEnabled={false} rejectEnabled={false} onStartShadow={onStartShadow} onReject={vi.fn()} />);

    expect(screen.queryByRole("dialog", { name: "确认启动影子验证" })).not.toBeInTheDocument();
    expect(onStartShadow).not.toHaveBeenCalled();
  });

  it("shows version-conflict recovery without discarding the comparison", async () => {
    const onStartShadow = vi.fn().mockRejectedValue(new Error("VERSION_CONFLICT"));
    const group = eligibleProposalGroup();
    render(<ProposalComparison group={group} onStartShadow={onStartShadow} onReject={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "启动影子验证" }));
    fireEvent.click(screen.getByRole("button", { name: "确认启动影子验证" }));
    expect(await screen.findByText(/版本已变化/)).toBeVisible();
    expect(screen.getAllByRole("columnheader", { name: /保持现状|最小调整|策略目标/ })).toHaveLength(3);
    expect(screen.getByRole("button", { name: "重新加载方案" })).toBeVisible();
  });
});
