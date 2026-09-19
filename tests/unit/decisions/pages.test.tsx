import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useParams: () => ({ cycleId: "0198f7a0-1111-7777-8111-111111111120" }),
}));

import DecisionDetailPage, { DecisionDetailPanel } from "../../../app/(protected)/decisions/[cycleId]/page";
import DecisionsPage, { DecisionsPanel } from "../../../app/(protected)/decisions/page";
import type { DataStateKind } from "../../../app/components/data-state";
import type { components } from "../../../app/lib/generated/schema";
import { agentPaperCycle, cancellableOrder, meta } from "../paper/fixtures";
import { managementDetail, managementId } from "../paper/management-fixtures";

type Cycle = components["schemas"]["AgentPaperCycleResponse"];
type CyclePage = components["schemas"]["AgentPaperCyclePageResponse"];
type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function cycle(suffix: string, changes: Partial<Cycle>): Cycle {
  return { ...structuredClone(agentPaperCycle), cycle_id: `0198f7a0-1111-7777-8111-1111111111${suffix}`, ...changes };
}

function detail(cycle: Cycle, orders = [cancellableOrder]): Detail {
  return {
    cycle,
    orders,
    candidate_batch: null,
    security_research: [],
    synthesis: null,
    proposal: null,
    active_target: null,
    activation_result: null,
    blocker: null,
    blocking_plan: null,
    plan_summary: null,
  };
}

const cycles = (): Cycle[] => [
  cycle("20", { status: "completed", result_code: "NO_ORDER_REQUIRED", order_ids: [], candidates: [{ ...agentPaperCycle.candidates[0], stance: "hold", sizing_action: "maintain" }] }),
  cycle("21", { status: "completed", result_code: "NO_ORDER_REQUIRED", order_ids: [], candidates: [{ ...agentPaperCycle.candidates[0], stance: "watch", sizing_action: "none" }] }),
  cycle("22", { status: "unavailable", result_code: "RISK_INPUT_REQUIRED", decision: null, decision_market: null, order_ids: [], candidates: [] }),
  cycle("23", { status: "failed", result_code: "AGENT_DECISION_CONFLICT", decision: null, order_ids: [], candidates: [] }),
  cycle("24", { status: "risk_evaluated", result_code: "RISK_REJECTED", order_ids: [], decision: { ...agentPaperCycle.decision!, risk_outcomes: ["rejected"] } }),
  cycle("25", { status: "partially_filled", result_code: "PAPER_PARTIALLY_FILLED", order_ids: [cancellableOrder.order_id] }),
  cycle("26", { status: "partially_filled", result_code: "PAPER_RESULT_INCOMPLETE", order_ids: [] }),
];

const summary: CyclePage["summary"] = {
  decision_session: "2026-08-26", status: "completed", cycle_count: 7, completed_count: 2,
  last_completed_decision_at: agentPaperCycle.decision_time, next_eligible_at: null,
  next_eligible_reason: null, latest_result_code: "PAPER_RESULT_INCOMPLETE", execution_locked: true,
};

describe("decision records", () => {
  it("keeps a legacy cycle's research and execution ledger when the account has a current target", () => {
    const legacy = detail(agentPaperCycle);
    legacy.active_target = managementDetail.active_target;
    render(<DecisionDetailPanel state="ready" detail={legacy} onRefresh={vi.fn()} />);
    expect(screen.getByRole("region", { name: "市场与研究证据" })).toBeVisible();
    expect(screen.getByRole("region", { name: "订单结论" })).toBeVisible();
    expect(screen.getByRole("region", { name: "执行结果" })).toBeVisible();
    expect(screen.queryByRole("heading", { name: "本轮组合取舍" })).not.toBeInTheDocument();
  });
  it("uses the producer's blocked activation shape and links only the blocking plan", () => {
    expect(managementDetail.activation_result?.target_version_id).toBeNull();
    expect(managementDetail.activation_result?.batch_id).toBeNull();
    expect(managementDetail.blocking_plan?.batch_id).toBe(managementId(10));
  });
  it("does not call a resource-blocked proposal completed in the list", () => {
    render(<DecisionsPanel state="ready" page={{ items: [managementDetail.cycle], summary, version_observation: null, version_history: [], version_evaluation: null }} onRefresh={vi.fn()} />);
    expect(screen.getByRole("list", { name: "决策记录" })).toHaveTextContent("提案未生效");
  });
  it("labels the producer's inherited-target reason without renewing research", () => {
    const value = structuredClone(managementDetail);
    value.proposal!.targets[0].reason_code = "INHERITED_TARGET";
    render(<DecisionDetailPanel state="ready" detail={value} onRefresh={vi.fn()} />);
    expect(screen.getByRole("table", { name: "确定性目标对照" })).toHaveTextContent("不更新研究有效期");
  });
  it("separates blocked proposal, Agent judgment and deterministic target authority", () => {
    render(<DecisionDetailPanel state="ready" detail={managementDetail} onRefresh={vi.fn()} />);
    const conclusion = screen.getByRole("note", { name: "目标生效结论" });
    expect(conclusion).toHaveTextContent("未生效");
    expect(conclusion).toHaveTextContent("目标 V7 继续生效");
    expect(conclusion).toHaveTextContent("提案 …0008");
    expect(within(conclusion).getByTitle(managementId(8))).toHaveTextContent("…0008");
    expect(screen.getByRole("table", { name: "确定性目标对照" })).toHaveTextContent("标的 …0009");
    expect(screen.getAllByTitle(managementId(9)).length).toBeGreaterThan(0);
    const headings = screen.getAllByRole("heading", { level: 2 }).map((h) => h.textContent);
    expect(headings.indexOf("本轮组合取舍")).toBeLessThan(headings.indexOf("当前目标与提案对照"));
    expect(screen.getByRole("table", { name: "确定性目标对照" })).toHaveTextContent("不更新研究有效期");
    expect(screen.getByRole("link", { name: "查看调整计划" })).toHaveAttribute("href", `/paper?plan=${managementId(10)}`);
    expect(screen.queryByRole("button", { name: /批准|立即生效/ })).not.toBeInTheDocument();
  });
  it.each(["initial", "unchanged", "policy"])("keeps %s target authority explicit", (variant) => {
    const value = structuredClone(managementDetail); value.blocker = null; value.blocking_plan = null;
    if (variant === "initial") { value.active_target = null; value.activation_result = null; }
    if (variant === "unchanged") value.activation_result = { ...value.activation_result!, result_code: "NO_ORDER_REQUIRED" };
    if (variant === "policy") { value.proposal = null; value.activation_result = null; value.blocker = { category: "policy_unconfirmed", code: "POLICY_UNCONFIRMED", source_stage: "policy", proposal_rejection_code: null, as_of: meta().as_of }; }
    render(<DecisionDetailPanel state="ready" detail={value} onRefresh={vi.fn()} />);
    expect(screen.getByRole("note", { name: "目标生效结论" })).toHaveTextContent(variant === "initial" ? "尚未建立目标" : variant === "policy" ? "政策未确认" : "目标 V7 继续生效");
  });
  it("renders one semantic row per cycle and keeps distinct product outcomes", () => {
    render(<DecisionsPanel state="ready" page={{ items: cycles(), summary, version_observation: null, version_history: [], version_evaluation: null }} onRefresh={vi.fn()} />);

    const records = screen.getByRole("list", { name: "决策记录" });
    expect(within(records).getAllByRole("listitem")).toHaveLength(7);
    for (const label of ["持有", "观察", "数据不足", "模型失败", "风控拒绝", "部分成交", "结果未知"]) {
      expect(within(records).getAllByText(label).length).toBeGreaterThan(0);
    }
    expect(screen.queryByRole("button", { name: /运行|停止|批准|下单/ })).not.toBeInTheDocument();
  });

  it("filters only the bounded loaded page without changing cycle identity", () => {
    render(<DecisionsPanel state="ready" page={{ items: cycles(), summary, version_observation: null, version_history: [], version_evaluation: null }} onRefresh={vi.fn()} />);
    fireEvent.change(screen.getByLabelText("决策类型"), { target: { value: "hold" } });

    const rows = screen.getByRole("list", { name: "决策记录" });
    expect(within(rows).getAllByRole("listitem")).toHaveLength(1);
    expect(within(rows).getByRole("link", { name: /查看决策/ })).toHaveAttribute("href", "/decisions/0198f7a0-1111-7777-8111-111111111120");
    expect(screen.getByText((_, node) => node?.classList.contains("decision-records__count") === true && node.textContent === "当前筛选显示 1 / 7 条已加载记录")).toBeVisible();
  });

  it.each<[DataStateKind, string]>([
    ["loading", "正在读取决策记录"], ["empty", "暂无决策记录"], ["partial", "决策记录不完整"],
    ["stale", "显示上次决策记录"], ["error", "决策记录加载失败"], ["forbidden", "无权读取决策记录"],
  ])("renders the list %s state without inferred facts", (state, title) => {
    render(<DecisionsPanel state={state} page={null} onRefresh={vi.fn()} />);
    expect(screen.getByRole("heading", { name: title })).toBeVisible();
    expect(screen.queryByRole("list", { name: "决策记录" })).not.toBeInTheDocument();
  });

  it("renders every evidence-to-outcome stage in order", () => {
    render(<DecisionDetailPanel state="ready" detail={detail(agentPaperCycle)} onRefresh={vi.fn()} />);

    const headings = screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent);
    expect(headings).toEqual([
      "运行窗口", "市场与研究证据", "Agent 决策", "确定性仓位计算",
      "确定性风控", "订单结论", "执行结果", "后续表现",
    ]);
    expect(screen.getByRole("heading", { name: "买入 · AAPL" })).toBeVisible();
    expect(screen.getAllByText("部分成交").length).toBeGreaterThan(0);
    expect(screen.getByText(cancellableOrder.order_id)).toBeInTheDocument();
    expect(screen.getByText(/接口尚未提供该周期之后的统一表现归因/)).toBeVisible();
  });

  it("keeps missing stages visible as unknown with one safe action", () => {
    const unknown = cycle("27", {
      status: "partially_filled", result_code: "PAPER_RESULT_INCOMPLETE", decision: null,
      decision_market: null, order_ids: [], candidates: [],
    });
    render(<DecisionDetailPanel state="ready" detail={detail(unknown, [])} onRefresh={vi.fn()} />);

    expect(screen.getAllByText("未知").length).toBeGreaterThanOrEqual(4);
    expect(screen.getByRole("region", { name: "订单结论" })).toHaveTextContent("没有关联订单事实，无法证明订单结果");
    expect(screen.getByRole("link", { name: "核对原操作结果" })).toHaveAttribute("href", "/paper");
    expect(screen.queryByRole("button", { name: /重试|提交|批准/ })).not.toBeInTheDocument();
  });

  it("presents a completed no-order execution as a known terminal result", () => {
    const noOrder = cycle("28", {
      status: "completed", result_code: "NO_ORDER_REQUIRED", order_ids: [],
      candidates: [{ ...agentPaperCycle.candidates[0], stance: "watch", sizing_action: "none" }],
    });
    render(<DecisionDetailPanel state="ready" detail={detail(noOrder, [])} onRefresh={vi.fn()} />);

    const execution = screen.getByRole("region", { name: "执行结果" });
    expect(execution).toHaveTextContent("无需下单");
    expect(execution).not.toHaveTextContent("处理中");
  });

  it("loads list and exact detail using GET only", async () => {
    const calls: Array<{ path: string; method: string }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      calls.push({ path, method: init?.method ?? "GET" });
      if (path === "/api/v1/paper/cycles?limit=20") return Response.json({ data: { items: cycles(), summary }, meta: { ...meta(), schema_version: "1" } });
      if (path.endsWith(agentPaperCycle.cycle_id)) return Response.json({ data: { cycle: agentPaperCycle, orders: [cancellableOrder] }, meta: { ...meta(), schema_version: "1" } });
      throw new Error(`unexpected path ${path}`);
    }));

    const list = render(<DecisionsPage />);
    expect(await screen.findByRole("list", { name: "决策记录" })).toBeVisible();
    list.unmount();
    render(<DecisionDetailPage />);
    await waitFor(() => expect(screen.getByRole("region", { name: "决策详情页面" })).toHaveAttribute("data-state", "ready"));
    expect(calls).toEqual([
      { path: "/api/v1/paper/cycles?limit=20", method: "GET" },
      { path: `/api/v1/paper/management/cycles/${agentPaperCycle.cycle_id}`, method: "GET" },
    ]);
  });

  it("rejects malformed detail responses", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data: { cycle: agentPaperCycle, orders: [{}] }, meta: meta() })));
    render(<DecisionDetailPage />);

    await waitFor(() => expect(screen.getByRole("region", { name: "决策详情页面" })).toHaveAttribute("data-state", "error"));
    expect(screen.queryByText(agentPaperCycle.cycle_id)).not.toBeInTheDocument();
  });
});
