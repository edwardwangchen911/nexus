import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ProposalComparison } from "../../../app/components/proposal-comparison";
import { eligibleProposalGroup, proposalGroup } from "./fixtures";

describe("three-way proposal comparison", () => {
  it("explains the current price-purpose restriction without offering a doomed start", () => {
    const onStartShadow = vi.fn();
    render(<ProposalComparison group={eligibleProposalGroup({ shadow_eligibility: {
      eligible: false, reasons: ["SHADOW_PRICE_AUTHORITY_UNAVAILABLE"],
    } })} onStartShadow={onStartShadow} onReject={vi.fn()} />);
    expect(screen.getByText(/当前价格证据尚不能用于新的前向观察/)).toBeVisible();
    expect(screen.queryByRole("button", { name: "启动影子验证" })).not.toBeInTheDocument();
    expect(screen.queryByText("可进入影子验证")).not.toBeInTheDocument();
    expect(onStartShadow).not.toHaveBeenCalled();
  });
  it.each([undefined, null])("reports legacy economic output %s as unreported, never hold", (economic) => {
    const onStartShadow = vi.fn();
    const onReject = vi.fn();
    render(<ProposalComparison group={proposalGroup({ economic_comparison: economic })} onStartShadow={onStartShadow} onReject={onReject} />);
    const region = screen.getByRole("region", { name: "经济比较结论" });
    expect(region).toHaveTextContent("经济比较结论未报告");
    expect(region).toHaveTextContent("不据此判断哪种方案更优");
    expect(region).not.toHaveTextContent("比较结果：保持现状");
    expect(onStartShadow).not.toHaveBeenCalled();
    expect(onReject).not.toHaveBeenCalled();
  });

  it.each([
    ["incomparable", "COST_EVIDENCE_INCOMPLETE", "暂不可比", "费用证据不完整"],
    ["incomparable", "COMPARISON_EVIDENCE_INCOMPLETE", "暂不可比", "方案比较证据不完整"],
    ["policy_required", "ECONOMIC_COMPARISON_POLICY_MISSING", "待明确比较判据", "风险改善与新增成本如何权衡"],
  ] as const)("explains %s / %s without inferring a hold recommendation", (status, reason, label, explanation) => {
    const group = proposalGroup({ economic_comparison: { status, recommendation: null, reasons: [reason] } });
    render(<ProposalComparison group={group} onStartShadow={vi.fn()} onReject={vi.fn()} />);
    const region = screen.getByRole("region", { name: "经济比较结论" });
    expect(region).toHaveTextContent(label);
    expect(region).toHaveTextContent(explanation);
    expect(region).toHaveTextContent("尚未形成经济比较结论");
    expect(region).not.toHaveTextContent("比较结果：保持现状");
    expect(within(region).getByText("查看原因代码")).toBeVisible();
    expect(within(region).getByText(reason).closest("details")).not.toHaveAttribute("open");
    expect(screen.queryByRole("button", { name: "启动影子验证" })).not.toBeInTheDocument();
  });

  it.each(["hold_current", "minimum_change", "strategy_target"] as const)("shows the reported %s result without changing lifecycle eligibility", (recommendation) => {
    const labels = { hold_current: "保持现状", minimum_change: "最小调整", strategy_target: "策略目标" };
    const onStartShadow = vi.fn();
    render(<ProposalComparison group={proposalGroup({ economic_comparison: { status: "compared", recommendation, reasons: [] } })} onStartShadow={onStartShadow} onReject={vi.fn()} />);
    const region = screen.getByRole("region", { name: "经济比较结论" });
    expect(region).toHaveTextContent(`比较结果：${labels[recommendation]}`);
    expect(region).toHaveTextContent("不代表已作出人工选择，也不授予执行权限");
    expect(screen.queryByRole("button", { name: "启动影子验证" })).not.toBeInTheDocument();
    expect(onStartShadow).not.toHaveBeenCalled();
  });

  it("keeps economic explanations independent from existing shadow permission and confirmation", () => {
    const onStartShadow = vi.fn();
    render(<ProposalComparison group={eligibleProposalGroup({ economic_comparison: {
      status: "policy_required", recommendation: null, reasons: ["ECONOMIC_COMPARISON_POLICY_MISSING"],
    } })} onStartShadow={onStartShadow} onReject={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "启动影子验证" }));
    expect(screen.getByRole("dialog", { name: "确认启动影子验证" })).toBeVisible();
    expect(onStartShadow).not.toHaveBeenCalled();
  });

  it.each(['<img src=x onerror="alert(1)">', "__proto__"])("preserves zero and unknown costs and renders unfamiliar reason %s only as inert details", (reason) => {
    const group = proposalGroup({ economic_comparison: { status: "incomparable", recommendation: null, reasons: [reason] } });
    group.proposals[0].comparison.metrics.dimensions.cost = { status: "known", value: "0", reason: null };
    const { container } = render(<ProposalComparison group={group} onStartShadow={vi.fn()} onReject={vi.fn()} />);
    const row = screen.getByRole("rowheader", { name: "成本占比" }).closest("tr")!;
    expect(within(row).getAllByRole("cell")[0]).toHaveTextContent(/^0%$/);
    expect(within(row).getAllByRole("cell")[2]).toHaveTextContent("未知");
    expect(screen.getByRole("region", { name: "经济比较结论" })).toHaveTextContent("存在未分类的比较限制，请查看原因代码");
    expect(container.querySelector("img")).toBeNull();
  });

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
