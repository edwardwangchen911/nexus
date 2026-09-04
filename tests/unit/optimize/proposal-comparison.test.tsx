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
    render(<ProposalComparison group={eligibleProposalGroup({
      economic_comparison: { status: "compared", recommendation, reasons: [] },
      shadow_eligibility: { eligible: false, reasons: ["SHADOW_PRICE_AUTHORITY_UNAVAILABLE"] },
    })} onStartShadow={onStartShadow} onReject={vi.fn()} />);
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

  it("shows exact saved position targets, unknown symbols, action reasons, and tiny deltas", () => {
    const group = proposalGroup();
    group.proposals[1].positions[0].display_symbol = "AAPL";
    group.proposals[1].positions[0].target_weight = "0.1999999";
    group.proposals[1].positions[0].reason_code = "MINIMUM_CHANGE_HEURISTIC";
    group.proposals[1].positions[1].display_symbol = null;
    group.proposals[1].comparison.cost_complete = false;
    group.proposals[1].comparison.non_executable = true;
    group.proposals[1].comparison.metrics.evidence_complete = false;
    group.proposals[1].comparison.metrics.ranking_eligible = false;
    group.proposals[1].comparison.metrics.dimensions.cost = {
      status: "unknown", value: null, reason: "COST_NOT_PROVEN",
    };
    render(<ProposalComparison group={group} onStartShadow={vi.fn()} onReject={vi.fn()} />);

    const region = screen.getByRole("region", { name: "最小调整仓位目标与完整性" });
    expect(within(region).getByRole("rowheader", { name: "AAPL" }).closest("tr")).toHaveTextContent(
      "20%20%(-0.001, 0) 个百分点减持MINIMUM_CHANGE_HEURISTIC",
    );
    expect(within(region).getByRole("rowheader", { name: "标的名称未知" })).toBeVisible();
    expect(within(region).getByText("成本依据不完整")).toBeVisible();
    expect(within(region).getByText("COST_NOT_PROVEN")).toBeVisible();
    expect(region).toHaveTextContent("非可执行");
    expect(region).toHaveTextContent("比较证据不完整");
  });

  it("marks matching strategy targets as not formed even when actions and reasons differ", () => {
    const group = proposalGroup();
    group.proposals[2].positions = group.proposals[0].positions.map((position, index) => ({
      ...position,
      action: index === 0 ? "reduce" : "increase",
      reason_code: index === 0 ? "STRATEGY_RESEARCH_PENDING" : "STRATEGY_SCOPE_PENDING",
    }));
    group.proposals[2].strategy_evidence_complete = false;
    render(<ProposalComparison group={group} onStartShadow={vi.fn()} onReject={vi.fn()} />);

    const region = screen.getByRole("region", { name: "策略目标仓位目标与完整性" });
    expect(region).toHaveTextContent("策略目标尚未形成");
    expect(region).toHaveTextContent("保存内容与保持现状相同");
    expect(region).not.toHaveTextContent("策略目标已形成");
  });

  it("does not mark matching strategy targets as pending when strategy evidence is complete", () => {
    const group = eligibleProposalGroup();
    group.proposals[2].positions = group.proposals[0].positions.map((position) => ({ ...position }));
    render(<ProposalComparison group={group} onStartShadow={vi.fn()} onReject={vi.fn()} />);

    const region = screen.getByRole("region", { name: "策略目标仓位目标与完整性" });
    expect(region).not.toHaveTextContent("策略目标尚未形成");
  });

  const proposalGates: ReadonlyArray<[
    string,
    (group: ReturnType<typeof eligibleProposalGroup>) => void,
    0 | 1 | 2,
  ]> = [
    ["策略证据", (group) => { group.proposals[2].strategy_evidence_complete = false; }, 2],
    ["约束核验", (group) => { group.proposals[1].comparison.metrics.constraint_complete = false; }, 1],
    ["适用性核验", (group) => { group.proposals[1].comparison.metrics.eligibility_complete = false; }, 1],
    ["比较证据", (group) => { group.proposals[1].comparison.metrics.evidence_complete = false; }, 1],
    ["成本依据", (group) => { group.proposals[1].comparison.cost_complete = false; }, 1],
    ["数据过期", (group) => { group.proposals[1].comparison.metrics.stale = true; }, 1],
    ["需要外汇换算", (group) => { group.proposals[1].comparison.requires_fx = true; }, 1],
    ["未知指标", (group) => { group.proposals[1].comparison.metrics.unknown_metrics = ["cost"]; }, 1],
    ["排名资格", (group) => { group.proposals[1].comparison.metrics.ranking_eligible = false; }, 1],
    ["非可执行", (group) => { group.proposals[1].comparison.non_executable = true; }, 1],
  ];

  it.each(proposalGates)("hides recommendations and shadow actions when %s alone is incomplete", (_name, breakGate, proposalIndex) => {
    const group = eligibleProposalGroup({
      economic_comparison: { status: "compared", recommendation: "minimum_change", reasons: [] },
      shadow_eligibility: { eligible: true, reasons: [] },
    });
    breakGate(group);
    render(<ProposalComparison group={group} onStartShadow={vi.fn()} onReject={vi.fn()} />);

    const summary = screen.getByRole("region", { name: "经济比较结论" });
    expect(summary).toHaveTextContent("至少一个方案不具备排名或执行适用性");
    expect(summary).not.toHaveTextContent("比较结果：最小调整");
    expect(screen.queryByRole("button", { name: "启动影子验证" })).not.toBeInTheDocument();
    expect(screen.queryByText(/启动影子验证/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "拒绝方案组" })).toBeVisible();
    const variant = ["保持现状", "最小调整", "策略目标"][proposalIndex];
    expect(screen.getByRole("region", { name: `${variant}仓位目标与完整性` })).toHaveTextContent(/暂不可比较|非可执行/);
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
