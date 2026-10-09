import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { managementPlan, managementSummary, cancelRead } from "./management-fixtures";
import { CancelResultCheck } from "../../../app/components/paper/cancel-result-check";
import { AdjustmentPlan, AdjustmentPlanLoader } from "../../../app/components/paper/adjustment-plan";

import PaperPage, { PaperPanel } from "../../../app/(protected)/paper/page";
import { AgentPaperCycle, AgentPaperSessionSummary, AgentPaperVersionHistory } from "../../../app/components/agent-paper-cycle";
import { AgentVersionEvaluation } from "../../../app/components/agent-version-evaluation";
import type { DataStateKind } from "../../../app/components/data-state";
import { agentPaperCycle, cancellableOrder, filledOrder, meta, olderAgentPaperCycle, paperPortfolio } from "./fixtures";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"], ["empty", "暂无数据"], ["ready", "数据已就绪"],
  ["partial", "数据不完整"], ["stale", "显示上次数据"], ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

function evaluationSlots(paired = 0): Array<Record<string, unknown>> {
  return Array.from({ length: 24 }, (_, index) => ({
    phase: index < 4 ? "train" : "oos",
    decision_slot_at: new Date(Date.UTC(2026, 8, 18 + index, 14, 5)).toISOString(),
    status: index < paired ? "paired" : "scheduled",
    incumbent: { status: index < paired ? "completed" : "pending", result_code: index < paired ? "NO_ORDER_REQUIRED" : null },
    challenger: { status: index < paired ? "completed" : "pending", result_code: index < paired ? "NO_ORDER_REQUIRED" : null },
    reason_code: index < paired ? "PAIR_RECORDED" : "REGISTERED_SLOT_SCHEDULED",
  }));
}

describe("internal paper page", () => {
  it("reads a selected historical cycle without changing the current account or orders", async () => {
    const historical = { ...agentPaperCycle, decision_session: "2026-08-25" };
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (path === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [], next_cursor: null }, meta: meta() });
      if (path === "/api/v1/paper/cycles?limit=20") return Response.json({ data: {
        items: [], summary: { decision_session: "2026-08-26", status: "not_started", cycle_count: 0,
          completed_count: 0, latest_result_code: null, last_completed_decision_at: null,
          next_eligible_at: null, next_eligible_reason: null, execution_locked: true },
        version_observation: null, version_history: [], version_evaluation: null,
      }, meta: meta() });
      if (path === "/api/v1/paper/cycles?decision_session=2026-08-25&limit=20") return Response.json({ data: {
        items: [historical], summary: { decision_session: "2026-08-25", status: "completed", cycle_count: 1,
          completed_count: 1, latest_result_code: historical.result_code,
          last_completed_decision_at: historical.decision_time, next_eligible_at: null,
          next_eligible_reason: null, execution_locked: true },
        version_observation: null, version_history: [], version_evaluation: null,
      }, meta: meta() });
      throw new Error(`unexpected path ${path}`);
    });
    vi.stubGlobal("fetch", fetcher);
    render(<PaperPage />);
    expect(await screen.findByText("尚无 Agent 模拟周期。")).toBeVisible();
    fireEvent.change(screen.getByLabelText("决策会话日期"), { target: { value: "2026-08-25" } });
    expect(await screen.findByText(`记录代码：${historical.result_code}`)).toBeVisible();
    expect(screen.getByRole("heading", { name: "所选会话 Agent 模拟周期" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "所选会话运行状态" })).toBeVisible();
    expect(fetcher.mock.calls.map(([path]) => String(path))).toContain("/api/v1/paper/cycles?decision_session=2026-08-25&limit=20");
    expect(fetcher.mock.calls.every(([path]) => !String(path).includes("/cancel"))).toBe(true);
  });

  it("shows a filled current position even before any close performance is published", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/paper/actual") return Response.json({ data: {
        data_state: "READY", as_of: "2026-09-22T20:00:00Z", available_cash: "8000",
        reserved_cash: "0", nav: "10000", positions: [{ instrument_id: paperPortfolio.positions[0].instrument_id, symbol: "AAPL", quantity: "10", reserved_quantity: "0" }],
      }, meta: meta() });
      if (url === "/api/v1/paper/management/portfolio") return Response.json({ data: { ...managementSummary, occupying_plan: null }, meta: meta() });
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: { ...paperPortfolio, data_state: "EMPTY", performance: null, comparison: null, positions: [], attributions: [] }, meta: meta() });
      if (url.includes("/paper/orders?")) return Response.json({ data: { items: [], next_cursor: null }, meta: meta() });
      return new Response(null, { status: 404 });
    });
    vi.stubGlobal("fetch", fetcher);
    render(<PaperPage />);
    const current = await screen.findByRole("heading", { name: "当前实际持仓与现金" });
    const section = current.closest("section")!;
    await waitFor(() => expect(section).toHaveTextContent("AAPL"));
    expect(section).toHaveTextContent("USD 8,000.00");
    expect(section).toHaveTextContent("10");
    expect(screen.queryByText("当前模拟账本持仓为 0。")).not.toBeInTheDocument();
    expect(fetcher).not.toHaveBeenCalledWith(expect.stringContaining("/cancel"), expect.anything());
  });
  it("keeps cycles and saved fills visible before close performance without enabling cancellation", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(null, { status: 404 })));
    render(<PaperPanel state="empty" portfolio={{ ...paperPortfolio, data_state: "EMPTY", performance: null, comparison: null, positions: [], attributions: [] }}
      orders={[cancellableOrder, filledOrder]} nextCursor={null} statusFilter="" loadingMore={false} notice={null}
      cycles={[agentPaperCycle]} currentCycleSession={agentPaperCycle.decision_session}
      onRefresh={vi.fn()} onStatusFilter={vi.fn()} onLoadMore={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByLabelText("决策会话日期")).toHaveValue(agentPaperCycle.decision_session);
    expect(screen.getByRole("heading", { name: "订单与模拟成交" })).toBeVisible();
    expect(screen.getByText(new RegExp(filledOrder.order_id))).toBeVisible();
    expect(screen.getByText("尚无已发布的内部模拟绩效快照。")).toBeVisible();
    expect(screen.queryByRole("button", { name: /取消.*模拟订单/ })).not.toBeInTheDocument();
    await screen.findByText("组合经济证据暂不可读；样本数量与评价状态未知。");
  });
  it("keeps current target details reachable without inventing an occupying plan", async () => {
    const fetcher = vi.fn(async () => Response.json({ data: { ...managementSummary, occupying_plan: null }, meta: meta() }));
    vi.stubGlobal("fetch", fetcher);
    render(<AdjustmentPlanLoader />);
    const target = await screen.findByRole("region", { name: "当前目标详情" });
    expect(target).toHaveTextContent("目标 V7");
    expect(target).toHaveTextContent("20%");
    expect(target).toHaveTextContent("80%");
    expect(target).toHaveTextContent("当前无资源占用计划");
    expect(within(target).getByRole("link", { name: "查看独立实际账户事实" })).toHaveAttribute("href", "#paper-actual-facts");
    expect(screen.queryByRole("region", { name: "当前调整计划" })).not.toBeInTheDocument();
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
  it("sorts interleaved multi-order fills by fact time with unambiguous order identity", () => {
    const value = structuredClone(managementPlan);
    const first = value.plan.items[0];
    first.confirmed_fills = [{ ...first.confirmed_fills[0], fill_id: "late", as_of: "2026-09-18T14:00:00Z" }];
    value.plan.items.push({ ...structuredClone(first), order_id: filledOrder.order_id, confirmed_fills: [{ ...first.confirmed_fills[0], fill_id: "early", as_of: "2026-09-18T12:00:00Z" }, { ...first.confirmed_fills[0], fill_id: "middle", as_of: "2026-09-18T13:00:00Z" }] });
    render(<AdjustmentPlan plan={value} />);
    const rows = screen.getAllByRole("heading", { name: "已确认成交" }).map((heading) => heading.closest("li")!);
    expect(rows.map((row) => row.querySelector("time")?.dateTime)).toEqual(["2026-09-18T12:00:00Z", "2026-09-18T13:00:00Z", "2026-09-18T14:00:00Z"]);
    expect(within(rows[0]).getByTitle(filledOrder.order_id)).toHaveTextContent("订单");
    expect(within(rows[2]).getByTitle(cancellableOrder.order_id)).toHaveTextContent("订单");
  });
  it("places original-operation checks before the timeline in DOM reading order", () => {
    render(<AdjustmentPlan plan={managementPlan} />);
    const check = screen.getByRole("heading", { name: "核对原操作结果" });
    const timeline = screen.getByRole("heading", { name: "执行时间线" });
    expect(check.compareDocumentPosition(timeline) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
  it("isolates a late original-operation read after changing the selected order", async () => {
    let resolveFirst!: (value: Response) => void;
    vi.stubGlobal("fetch", vi.fn(() => new Promise<Response>((resolve) => { resolveFirst = resolve; })));
    const attempt = { orderId: cancellableOrder.order_id, key: "original-attempt", requestedAt: meta().as_of };
    const view = render(<CancelResultCheck attempt={attempt} />);
    fireEvent.click(screen.getByRole("button", { name: "核对原操作结果" }));
    view.rerender(<CancelResultCheck attempt={{ ...attempt, orderId: filledOrder.order_id, key: "other-original-attempt" }} />);
    await act(async () => { resolveFirst(Response.json({ data: { ...cancelRead, status: "COMMITTED" }, meta: meta() })); });
    expect(screen.queryByText("原取消已确认")).not.toBeInTheDocument();
    expect(screen.getByText(filledOrder.order_id)).toBeVisible();
  });
  it("reads an exact plan and independently displays risk, fills, coverage, gap and cash", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/paper/management/portfolio") return Response.json({ data: managementSummary, meta: meta() });
      if (url.includes("/paper/plans/")) return Response.json({ data: managementPlan, meta: meta() });
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url.includes("/paper/orders?")) return Response.json({ data: { items: [cancellableOrder], next_cursor: null }, meta: meta() });
      return new Response(null, { status: 404 });
    }));
    render(<PaperPage />);
    const plan = await screen.findByRole("region", { name: "当前调整计划" });
    expect(plan).toHaveTextContent(managementPlan.batch_id);
    expect(within(plan).getByRole("heading", { name: "调整计划 …0010" })).toHaveTextContent("…0010");
    expect(within(plan).getByTitle(managementPlan.batch_id)).toHaveTextContent("…0010");
    for (const label of ["风控缩减", "已确认成交", "未结订单覆盖", "剩余目标差额", "资源仍占用", "可用现金", "预留现金", "USD 3,500.00"]) expect(plan).toHaveTextContent(label);
    expect(within(plan).getByRole("button", { name: "核对原操作结果" })).toBeDisabled();
  });
  it("renders a read-only paired Agent version evaluation with all write paths closed", () => {
    render(<AgentVersionEvaluation evaluation={{
      evaluation_id: "71000000-0000-0000-0000-000000000099",
      status: "observing",
      incumbent_prompt_hash: "a".repeat(64), incumbent_model_hash: "b".repeat(64),
      challenger_prompt_hash: "c".repeat(64), challenger_model_hash: "d".repeat(64),
      train_completed: 2, oos_completed: 0, oos_missing: 20, oos_incomparable: 0,
      slots: Array.from({ length: 24 }, (_, index) => ({
        phase: index < 4 ? "train" as const : "oos" as const,
        decision_slot_at: new Date(Date.UTC(2026, 8, 18 + index, 14, 5)).toISOString(),
        status: index < 2 ? "paired" as const : "scheduled" as const,
        incumbent: { status: index < 2 ? "completed" as const : "pending" as const, result_code: index < 2 ? "NO_ORDER_REQUIRED" : null },
        challenger: { status: index < 2 ? "completed" as const : "pending" as const, result_code: index < 2 ? "NO_ORDER_REQUIRED" : null },
        reason_code: index < 2 ? "PAIR_RECORDED" : "REGISTERED_SLOT_SCHEDULED",
      })),
      incumbent_metrics: null, challenger_metrics: null,
      comparisons: [
        { key: "hold_current", data_state: "READY", reason_code: "CASH_HOLD_CURRENT" },
        { key: "baseline_v1", data_state: "UNKNOWN", reason_code: "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE" },
        { key: "market_benchmark", data_state: "UNKNOWN", reason_code: "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE" },
      ],
      gates: [], reason_code: "EVALUATION_OBSERVING",
      execution_locked: true, broker_write: false, live_write: false,
    }} />);

    const region = screen.getByRole("region", { name: "Agent 版本并行评价" });
    expect(region).toHaveTextContent("训练槽2 / 4 已配对");
    expect(region).toHaveTextContent("评价运行时间线");
    expect(region).toHaveTextContent("训练 1");
    expect(region).toHaveTextContent("已配对");
    expect(region).toHaveTextContent("等待登记时间");
    expect(region).toHaveTextContent("执行锁定 · 券商写入关闭 · 实盘写入关闭");
    expect(region).toHaveTextContent("尚未终结，不展示推测性门禁结果");
    expect(within(region).queryByRole("button")).not.toBeInTheDocument();
  });

  it("stops claiming the challenger is unavailable after registration is proven", () => {
    render(<AgentPaperVersionHistory history={[{
      prompt_version: "local-openai-v1", prompt_hash: "a".repeat(64),
      model_id: "gpt-5.6-terra", model_version: "codex-sdk-chatgpt-v4-actionable",
      model_hash: "b".repeat(64), first_decision_at: "2026-09-14T15:32:03Z",
      last_decision_at: "2026-09-16T18:12:07Z", observed_sessions: 3,
      cycle_count: 48, completed_count: 39, unavailable_count: 9,
      order_count: 0, fill_count: 0, challenger_available: true,
      lifecycle_state: "incumbent_frozen", verdict: "continue_observing",
      reason_codes: ["VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE", "VERSION_ORDER_PATH_UNOBSERVED"],
    }]} />);

    const region = screen.getByRole("region", { name: "Agent 版本历史" });
    expect(region).toHaveTextContent("已注册的并行 challenger");
    expect(region).not.toHaveTextContent("尚无并行 challenger 版本");
  });

  it("shows persisted continuous-run status without implying a live scheduler", () => {
    render(<AgentPaperSessionSummary summary={{
      decision_session: "2026-08-26",
      status: "completed",
      cycle_count: 2,
      completed_count: 2,
      latest_result_code: "NO_ORDER_REQUIRED",
      last_completed_decision_at: "2026-08-26T19:30:00Z",
      next_eligible_at: "2026-08-26T19:35:00Z",
      next_eligible_reason: "NEXT_SLOT",
      execution_locked: true,
    } as never} />);

    const region = screen.getByRole("region", { name: "今日连续运行状态" });
    expect(region).toHaveTextContent("已完成 2 / 2 个周期");
    expect(region).toHaveTextContent("最近完成周期的决策时间");
    expect(region).toHaveTextContent("Agent 已完成判断，确定性仓位计算结果为不调整");
    expect(region).toHaveTextContent("下一五分钟槽");
    expect(region).toHaveTextContent("只反映已保存周期，不代表前台循环进程在线");
    expect(within(region).queryByRole("button")).not.toBeInTheDocument();
  });

  it("shows sequential Agent version history without inventing challenger or promotion evidence", async () => {
    const currentVersion = {
      prompt_version: "local-openai-v1", prompt_hash: "a".repeat(64), model_id: "gpt-5.6-terra",
      model_version: "codex-sdk-chatgpt-v4-actionable",
      model_hash: "b".repeat(64),
      first_decision_at: "2026-09-14T15:32:03Z", last_decision_at: "2026-09-16T18:12:07Z",
      observed_sessions: 3, cycle_count: 48, completed_count: 39, unavailable_count: 9,
      order_count: 0, fill_count: 0, challenger_available: false as const,
      lifecycle_state: "incumbent_frozen" as const,
      verdict: "continue_observing" as const,
      reason_codes: [
        "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE",
        "VERSION_ORDER_PATH_UNOBSERVED",
        "VERSION_CHALLENGER_UNAVAILABLE",
      ] as const,
    };
    const historicalVersion = {
      ...currentVersion,
      prompt_hash: "c".repeat(64),
      first_decision_at: "2026-09-13T15:32:03Z",
      last_decision_at: "2026-09-13T18:12:07Z",
      observed_sessions: 1,
      cycle_count: 4,
      completed_count: 4,
      unavailable_count: 0,
      lifecycle_state: "historical_frozen" as const,
      verdict: "retained_as_frozen_history" as const,
      reason_codes: [
        "VERSION_FROZEN_AUDIT_HISTORY",
        "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE",
        "VERSION_ORDER_PATH_UNOBSERVED",
        "VERSION_SEQUENTIAL_HISTORY_NOT_COMPARABLE",
      ] as const,
    };
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [], next_cursor: null }, meta: meta() });
      if (url === "/api/v1/paper/cycles?limit=20") return Response.json({ data: {
        items: [agentPaperCycle],
        summary: {
          decision_session: "2026-08-26", status: "completed", cycle_count: 1, completed_count: 1,
          latest_result_code: "NO_ORDER_REQUIRED", last_completed_decision_at: "2026-08-26T19:30:00Z",
          next_eligible_at: "2026-08-26T19:35:00Z", next_eligible_reason: "NEXT_SLOT", execution_locked: true,
        },
        version_observation: currentVersion,
        version_history: [currentVersion, historicalVersion],
      }, meta: meta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);

    const region = await screen.findByRole("region", { name: "Agent 版本历史" });
    expect(region).toHaveTextContent("local-openai-v1");
    expect(region).toHaveTextContent("gpt-5.6-terra");
    expect(region).toHaveTextContent("prompt aaaaaaaaaaaa… · model bbbbbbbbbbbb…");
    expect(region).toHaveTextContent("prompt cccccccccccc… · model bbbbbbbbbbbb…");
    expect(region).toHaveTextContent("39 / 48 个周期完成");
    expect(region).toHaveTextContent("3 个交易日");
    expect(region).toHaveTextContent("继续观察");
    expect(region).toHaveTextContent("尚无 Agent 版本级绩效归因");
    expect(region).toHaveTextContent("尚无模拟订单与成交的持久化路径事实");
    expect(region).toHaveTextContent("尚无并行 challenger 版本");
    expect(region).toHaveTextContent("当前冻结 incumbent");
    expect(region).toHaveTextContent("不代表当前进程在线");
    expect(region).toHaveTextContent("历史冻结证据");
    expect(region).toHaveTextContent("保留为冻结审计证据");
    expect(region).toHaveTextContent("该版本只是顺序历史，不能与 incumbent 做并行比较");
    expect(region).toHaveTextContent("顺序历史不是并行 challenger");
    expect(region).toHaveTextContent("没有正式晋升或淘汰结论");
    expect(within(region).queryByRole("button")).not.toBeInTheDocument();
  });

  it("fails closed when a self-consistent version history has no incumbent", async () => {
    const historicalVersion = {
      prompt_version: "local-openai-v1", prompt_hash: "c".repeat(64), model_id: "gpt-5.6-terra",
      model_version: "codex-sdk-chatgpt-v4-actionable", model_hash: "b".repeat(64),
      first_decision_at: "2026-09-13T15:32:03Z", last_decision_at: "2026-09-13T18:12:07Z",
      observed_sessions: 1, cycle_count: 4, completed_count: 4, unavailable_count: 0,
      order_count: 0, fill_count: 0, challenger_available: false as const,
      lifecycle_state: "historical_frozen" as const, verdict: "retained_as_frozen_history" as const,
      reason_codes: [
        "VERSION_FROZEN_AUDIT_HISTORY", "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE",
        "VERSION_ORDER_PATH_UNOBSERVED", "VERSION_SEQUENTIAL_HISTORY_NOT_COMPARABLE",
      ] as const,
    };
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [], next_cursor: null }, meta: meta() });
      if (url === "/api/v1/paper/cycles?limit=20") return Response.json({ data: {
        items: [agentPaperCycle],
        summary: {
          decision_session: "2026-08-26", status: "completed", cycle_count: 1, completed_count: 1,
          latest_result_code: "NO_ORDER_REQUIRED", last_completed_decision_at: "2026-08-26T19:30:00Z",
          next_eligible_at: "2026-08-26T19:35:00Z", next_eligible_reason: "NEXT_SLOT", execution_locked: true,
        },
        version_observation: historicalVersion, version_history: [historicalVersion],
      }, meta: meta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);

    expect(await screen.findByText("模拟盘事实读取失败；未触发任何订单动作。")).toBeVisible();
    expect(screen.queryByRole("region", { name: "Agent 版本历史" })).not.toBeInTheDocument();
  });

  it("fails closed on inconsistent Agent version evaluation lifecycle counts", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [], next_cursor: null }, meta: meta() });
      if (url === "/api/v1/paper/cycles?limit=20") return Response.json({ data: {
        items: [], summary: {
          decision_session: "2026-08-26", status: "not_started", cycle_count: 0, completed_count: 0,
          latest_result_code: null, last_completed_decision_at: null, next_eligible_at: null,
          next_eligible_reason: null, execution_locked: true,
        }, version_observation: null, version_history: [], version_evaluation: {
          evaluation_id: "71000000-0000-0000-0000-000000000099", status: "observing",
          incumbent_prompt_hash: "a".repeat(64), incumbent_model_hash: "b".repeat(64),
          challenger_prompt_hash: "c".repeat(64), challenger_model_hash: "d".repeat(64),
          train_completed: 0, oos_completed: 0, oos_missing: 19, oos_incomparable: 0,
          slots: evaluationSlots(),
          incumbent_metrics: null, challenger_metrics: null,
          comparisons: [
            { key: "hold_current", data_state: "READY", reason_code: "READY" },
            { key: "baseline_v1", data_state: "UNKNOWN", reason_code: "UNKNOWN" },
            { key: "market_benchmark", data_state: "UNKNOWN", reason_code: "UNKNOWN" },
          ], gates: [], reason_code: "EVALUATION_OBSERVING",
          execution_locked: true, broker_write: false, live_write: false,
        },
      }, meta: meta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    expect(await screen.findByText("模拟盘事实读取失败；未触发任何订单动作。")).toBeVisible();
    expect(screen.queryByRole("region", { name: "Agent 版本并行评价" })).not.toBeInTheDocument();
  });

  it("separates realtime Schwab decision quotes from unknown-delivery execution bars", () => {
    render(<AgentPaperCycle cycles={[agentPaperCycle]} />);

    const region = screen.getByRole("region", { name: "当日 Agent 模拟周期" });
    expect(region).toHaveTextContent("决策报价 · Schwab Level One · 实时");
    expect(region).toHaveTextContent("执行分钟线 · Schwab pricehistory · 实时性未由响应声明");
    expect(region).not.toHaveTextContent("下一交易日");
    expect(region).not.toHaveTextContent("嘉信实时成交");
    expect(within(region).queryByRole("button")).not.toBeInTheDocument();
  });

  it("distinguishes each candidate's research job from decision admission after a stopped cycle", () => {
    const cycle = structuredClone(agentPaperCycle);
    cycle.status = "unavailable";
    cycle.result_code = "RESEARCH_OUTPUT_NOT_ACTIONABLE";
    cycle.decision = null;
    cycle.candidates = [
      { ...cycle.candidates[0], research_status: "pending", research_job_status: "succeeded", stance: null, sizing_action: null, output_id: null, intent_hash: null },
      { ...cycle.candidates[0], instrument_id: "0198f7a0-1111-7777-8111-111111111124", symbol: "AVGO", research_status: "pending", research_job_status: "dead_letter", stance: null, sizing_action: null, output_id: null, intent_hash: null },
      { ...cycle.candidates[0], instrument_id: "0198f7a0-1111-7777-8111-111111111125", symbol: "NVDA", research_status: "pending", research_job_status: "queued", stance: null, sizing_action: null, output_id: null, intent_hash: null },
    ];
    cycle.candidate_count = 3;

    render(<AgentPaperCycle cycles={[cycle]} />);

    expect(screen.getByRole("region", { name: "当日 Agent 模拟周期" })).toHaveTextContent("已停止");
    expect(screen.getByRole("note", { name: "Agent 运行结论" })).toHaveTextContent("研究任务运行失败，未形成可核验的研究结果");
    expect(screen.getByRole("note", { name: "Agent 运行结论" })).not.toHaveTextContent("研究结果未通过可执行准入");
    const table = screen.getByRole("region", { name: "Agent 模拟候选，可横向滚动" });
    expect(within(table).getByRole("row", { name: /AAPL.*任务成功.*未纳入决策/ })).toBeVisible();
    expect(within(table).getByRole("row", { name: /AVGO.*任务失败.*未纳入决策/ })).toBeVisible();
    expect(within(table).getByRole("row", { name: /NVDA.*未开始.*未纳入决策/ })).toBeVisible();
  });

  it("switches between bounded same-session Agent cycles without triggering an action", () => {
    render(<AgentPaperCycle cycles={[agentPaperCycle, olderAgentPaperCycle]} />);

    const region = screen.getByRole("region", { name: "当日 Agent 模拟周期" });
    const selector = screen.getByRole("combobox", { name: "当日 Agent 模拟周期" });
    expect(region).toHaveTextContent("PAPER_ORDERS_AWAITING_MARKET");
    fireEvent.change(selector, { target: { value: olderAgentPaperCycle.cycle_id } });
    expect(region).toHaveTextContent("RISK_INPUT_REQUIRED");
    expect(region).toHaveTextContent("实盘锁定");
    expect(within(region).queryByRole("button")).not.toBeInTheDocument();
  });

  it.each([
    [
      "NO_ORDER_REQUIRED",
      "completed",
      "Agent 已完成判断，确定性仓位计算结果为不调整，因此没有创建模拟订单。",
    ],
    [
      "RESEARCH_OUTPUT_NOT_ACTIONABLE",
      "unavailable",
      "研究结果未通过可执行准入；本周期未创建模拟订单。",
    ],
    [
      "RISK_INPUT_REQUIRED",
      "unavailable",
      "风险输入不足，系统已停止本周期，没有创建模拟订单。",
    ],
    [
      "PAPER_ORDERS_AWAITING_MARKET",
      "awaiting_market",
      "Agent 已形成可执行方向并通过风控；模拟订单正在等待执行行情窗口。",
    ],
  ] as const)("explains the selected Agent cycle result %s in plain language", (resultCode, status, explanation) => {
    const cycle = structuredClone(agentPaperCycle);
    cycle.result_code = resultCode;
    cycle.status = status;
    if (resultCode !== "PAPER_ORDERS_AWAITING_MARKET") cycle.order_ids = [];

    render(<AgentPaperCycle cycles={[cycle]} />);

    const conclusion = screen.getByRole("note", { name: "Agent 运行结论" });
    expect(conclusion).toHaveTextContent(explanation);
    expect(conclusion).toHaveTextContent(`记录代码：${resultCode}`);
  });

  it("keeps an unknown Agent result code without inventing an explanation", () => {
    const cycle = structuredClone(agentPaperCycle);
    cycle.result_code = "constructor";
    cycle.status = "failed";

    render(<AgentPaperCycle cycles={[cycle]} />);

    const conclusion = screen.getByRole("note", { name: "Agent 运行结论" });
    expect(conclusion).toHaveTextContent("系统已保存本周期结果；请结合周期状态和记录代码核对详细事实。");
    expect(conclusion).toHaveTextContent("记录代码：constructor");
  });

  it("keeps portfolio and orders when the cycle read fails", async () => {
    const methods: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      methods.push(init?.method ?? "GET");
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [cancellableOrder], next_cursor: null }, meta: meta() });
      if (url === "/api/v1/paper/cycles?limit=20") return Response.json({ error: { code: "UPSTREAM_ERROR" } }, { status: 502 });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    expect(await screen.findByText("账户绩效")).toBeVisible();
    expect(screen.getByText(/Agent 周期事实暂不可用/)).toBeVisible();
    expect(screen.getByRole("button", { name: "取消 AAPL 模拟订单" })).toBeVisible();
    expect(methods.every((method) => method === "GET")).toBe(true);
  });
  it.each(["1", "0.1"])("renders integer and short-decimal drawdown ratios correctly: %s", (ratio) => {
    const portfolio = structuredClone(paperPortfolio);
    portfolio.performance!.drawdown_ratio = ratio;
    render(<PaperPanel state="ready" portfolio={portfolio} orders={[]} nextCursor={null} statusFilter="" loadingMore={false} notice={null} onRefresh={vi.fn()} onStatusFilter={vi.fn()} onLoadMore={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByLabelText("模拟账户绩效事实")).toHaveTextContent(ratio === "1" ? "100%" : "10%");
  });

  it.each(states)("renders the visible %s state without inventing facts", (state, label) => {
    const hasFacts = ["ready", "partial", "stale"].includes(state);
    render(<PaperPanel
      state={state}
      portfolio={hasFacts ? paperPortfolio : null}
      orders={hasFacts ? [cancellableOrder] : []}
      nextCursor={null}
      statusFilter=""
      loadingMore={false}
      notice={null}
      onRefresh={vi.fn()}
      onStatusFilter={vi.fn()}
      onLoadMore={vi.fn()}
      onCancel={vi.fn()}
    />);

    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "内部模拟盘控制台" })).toHaveAttribute("data-state", state);
    if (!hasFacts) expect(screen.queryByText("USD 10,000.00")).not.toBeInTheDocument();
    if (state === "ready") expect(screen.getByRole("button", { name: "取消 AAPL 模拟订单" })).toBeVisible();
    if (state === "partial" || state === "stale") {
      expect(screen.queryByRole("button", { name: "取消 AAPL 模拟订单" })).not.toBeInTheDocument();
    }
  });

  it("renders authoritative cash, PnL, position and attribution facts", () => {
    const portfolio = structuredClone(paperPortfolio);
    portfolio.attributions[1].completeness = "PARTIAL";
    render(<PaperPanel state="ready" portfolio={portfolio} orders={[cancellableOrder, filledOrder]}
      nextCursor="next" statusFilter="" loadingMore={false} notice={null}
      onRefresh={vi.fn()} onStatusFilter={vi.fn()} onLoadMore={vi.fn()} onCancel={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "内部模拟盘" })).toBeVisible();
    expect(screen.getByText("内部模拟").closest("li")).toHaveAttribute("aria-current", "step");
    expect(screen.getByText("冻结绩效事实")).toBeVisible();
    expect(screen.getAllByText(/不是实盘/).length).toBeGreaterThanOrEqual(2);
    expect(within(screen.getByRole("region", { name: "模拟账户边界" })).getByText("USD 10,000.00")).toBeVisible();
    const facts = screen.getByLabelText("模拟账户绩效事实");
    expect(facts).toHaveTextContent("USD 8,500.00");
    expect(facts).toHaveTextContent("USD 500.00");
    expect(facts).toHaveTextContent("USD 25.00");
    expect(facts).toHaveTextContent("0.99%");

    const positions = screen.getByRole("table", { name: "内部模拟持仓" });
    expect(positions).toHaveTextContent("AAPL");
    expect(positions).toHaveTextContent("alpaca / iex");
    expect(positions).toHaveTextContent("延迟");
    expect(positions).toHaveTextContent("2026-08-27 04:00:00 北京时间");
    expect(screen.getByRole("table", { name: "内部模拟绩效归因" })).toHaveTextContent("information_technology");
    expect(screen.getByRole("table", { name: "内部模拟绩效归因" })).toHaveTextContent("资料不完整");
    expect(screen.getByRole("table", { name: "内部模拟绩效归因" })).not.toHaveTextContent("PARTIAL");
    expect(screen.getByRole("table", { name: "内部模拟绩效归因" })).not.toHaveTextContent("未识别状态");
    expect(document.body).not.toHaveTextContent(/account_id|client_order_id|provider_object/);
  });

  it("compares Agent and cash hold facts without inventing missing forward baselines", () => {
    render(<PaperPanel state="ready" portfolio={paperPortfolio} orders={[]} nextCursor={null} statusFilter="" loadingMore={false} notice={null} onRefresh={vi.fn()} onStatusFilter={vi.fn()} onLoadMore={vi.fn()} onCancel={vi.fn()} />);

    const table = screen.getByRole("table", { name: "连续模拟四方对照" });
    expect(table).toHaveTextContent("Agent");
    expect(table).toHaveTextContent("保持不动（现金）");
    expect(table).toHaveTextContent("baseline_v1");
    expect(table).toHaveTextContent("市场基准（SPY）");
    expect(table).toHaveTextContent("USD 2.92");
    expect(within(table).getAllByText("缺少同窗前向事实")).toHaveLength(2);
    expect(table).not.toHaveTextContent("历史回测");
  });

  it("shows current drawdown declining after recovery, not the historical maximum", () => {
    const props = { state: "ready" as const, orders: [], nextCursor: null, statusFilter: "" as const, loadingMore: false, notice: null, onRefresh: vi.fn(), onStatusFilter: vi.fn(), onLoadMore: vi.fn(), onCancel: vi.fn() };
    const portfolio = structuredClone(paperPortfolio);
    Object.assign(portfolio.performance!, { nav: "8000", peak_nav: "10000", drawdown_ratio: "0.2" });
    const view = render(<PaperPanel {...props} portfolio={portfolio} />);
    expect(screen.getByText("当前回撤").nextElementSibling).toHaveTextContent("20%");
    expect(screen.getByText("当前回撤")).toHaveAttribute("title", "当前净资产相对历史净资产峰值的回撤，不是历史最大回撤");
    expect(screen.getByText("账户净资产（NAV）")).toBeVisible();
    const recovered = structuredClone(portfolio);
    Object.assign(recovered.performance!, { nav: "9500", drawdown_ratio: "0.05" });
    view.rerender(<PaperPanel {...props} portfolio={recovered} />);
    expect(screen.getByText("当前回撤").nextElementSibling).toHaveTextContent("5%");
    expect(within(screen.getByRole("region", { name: "账户绩效" })).queryByText("最大回撤")).not.toBeInTheDocument();
    expect(recovered.performance!.drawdown_ratio).toBe("0.05");
  });

  it("loads facts, applies a server status filter, and appends a bounded cursor page", async () => {
    const calls: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input); calls.push(url);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [cancellableOrder], next_cursor: "signed-next" }, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20&status=filled") return Response.json({ data: { items: [filledOrder], next_cursor: "filled-next" }, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20&status=filled&cursor=filled-next") return Response.json({ data: { items: [{ ...filledOrder, order_id: "0198f7a0-1111-7777-8111-111111111129", symbol: "MSFT" }], next_cursor: null }, meta: meta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    expect(await screen.findByRole("heading", { name: /AAPL/ })).toBeVisible();
    fireEvent.change(screen.getByRole("combobox", { name: "订单状态筛选" }), { target: { value: "filled" } });
    await waitFor(() => expect(calls).toContain("/api/v1/paper/orders?limit=20&status=filled"));
    const loadMore = await screen.findByRole("button", { name: "加载更多订单" });
    await waitFor(() => expect(loadMore).toBeEnabled());
    fireEvent.click(loadMore);
    expect(await screen.findByRole("heading", { name: /MSFT/ })).toBeVisible();
    expect(calls).toContain("/api/v1/paper/orders?limit=20&status=filled&cursor=filled-next");
  });

  it("fails closed when an order filter cannot refresh its authority", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [cancellableOrder], next_cursor: null }, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20&status=filled") return Response.json({ error: { code: "UPSTREAM_ERROR" } }, { status: 502 });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    expect(await screen.findByRole("button", { name: "取消 AAPL 模拟订单" })).toBeVisible();
    fireEvent.change(screen.getByRole("combobox", { name: "订单状态筛选" }), { target: { value: "filled" } });

    expect(await screen.findByText("无法应用订单筛选；当前列表不执行取消操作。")).toBeVisible();
    expect(screen.getByRole("region", { name: "内部模拟盘控制台" })).toHaveAttribute("data-state", "partial");
    expect(screen.queryByRole("button", { name: "取消 AAPL 模拟订单" })).not.toBeInTheDocument();
  });

  it("clears visible paper facts when the BFF denies access", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") return Response.json({ error: { code: "FORBIDDEN" } }, { status: 403 });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [cancellableOrder], next_cursor: null }, meta: meta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    expect((await screen.findAllByText("无权访问")).length).toBeGreaterThanOrEqual(2);
    expect(screen.queryByRole("region", { name: "模拟账户边界" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "取消 AAPL 模拟订单" })).not.toBeInTheDocument();
  });

  it("confirms cancellation, sends the current event version, and rereads server facts", async () => {
    const postKeys: Array<string | null> = [];
    const postBodies: string[] = [];
    let portfolioReads = 0;
    vi.spyOn(crypto, "randomUUID").mockReturnValue("0198f7a0-1111-7777-8111-111111111130");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") { portfolioReads += 1; return Response.json({ data: paperPortfolio, meta: meta() }); }
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [cancellableOrder], next_cursor: null }, meta: meta() });
      if (url.endsWith("/cancel")) {
        postKeys.push(new Headers(init?.headers).get("idempotency-key"));
        postBodies.push(String(init?.body));
        return Response.json({ data: { order_id: cancellableOrder.order_id, status: "cancelled", filled_quantity: "2", remaining_quantity: "3", result_event_seq: 5, replayed: false }, meta: meta() });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    fireEvent.click(await screen.findByRole("button", { name: "取消 AAPL 模拟订单" }));
    const dialog = screen.getByRole("dialog", { name: "确认取消 AAPL 模拟订单" });
    expect(dialog).toHaveTextContent("事件版本 4");
    expect(within(dialog).getByRole("button", { name: "返回订单" })).toHaveFocus();
    fireEvent.click(within(dialog).getByRole("button", { name: "确认取消模拟订单" }));

    expect(await screen.findByText("模拟订单已取消；已重新读取服务端事实。")).toBeVisible();
    expect(postKeys).toEqual(["paper-cancel-0198f7a0-1111-7777-8111-111111111130"]);
    expect(postBodies).toEqual(["{\"expected_event_seq\":4}"]);
    expect(portfolioReads).toBeGreaterThanOrEqual(2);
  });

  it("keeps an unknown cancellation locked to one idempotency key for recovery", async () => {
    const keys: Array<string | null> = [];
    const operations: Array<[string, string]> = [];
    let cancels = 0;
    let reads = 0;
    vi.spyOn(crypto, "randomUUID").mockReturnValue("0198f7a0-1111-7777-8111-111111111131");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      operations.push([url, init?.method ?? "GET"]);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [cancellableOrder], next_cursor: null }, meta: meta() });
      if (url.endsWith("/cancel")) {
        cancels += 1; keys.push(new Headers(init?.headers).get("idempotency-key"));
        throw new TypeError("network result unknown");
      }
      if (url.endsWith("/cancel-result")) {
        reads += 1; keys.push(new Headers(init?.headers).get("idempotency-key"));
        expect(init?.method ?? "GET").toBe("GET");
        return Response.json({ data: cancelRead, meta: meta() });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    fireEvent.click(await screen.findByRole("button", { name: "取消 AAPL 模拟订单" }));
    fireEvent.click(screen.getByRole("button", { name: "确认取消模拟订单" }));
    expect((await screen.findAllByText(/取消结果未知/)).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: "核对原操作结果" }));
    expect(await screen.findByText("服务端未记录原取消结果")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "核对原操作结果" }));
    await waitFor(() => expect(reads).toBe(2));
    expect(cancels).toBe(1);
    expect(operations.filter(([, method]) => method !== "GET")).toEqual([[`/api/v1/paper/orders/${cancellableOrder.order_id}/cancel`, "POST"]]);
    expect(new Set(keys).size).toBe(1);
    fireEvent.click(screen.getByRole("button", { name: "返回订单" }));
    fireEvent.click(screen.getByRole("button", { name: "取消 AAPL 模拟订单" }));
    expect(screen.queryByRole("button", { name: "确认取消模拟订单" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "核对原操作结果" })).toBeEnabled();
  });

  it("dismisses the confirmation with Escape and returns focus to the order", async () => {
    vi.spyOn(crypto, "randomUUID").mockReturnValue("0198f7a0-1111-7777-8111-111111111132");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [cancellableOrder], next_cursor: null }, meta: meta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    const cancel = await screen.findByRole("button", { name: "取消 AAPL 模拟订单" });
    fireEvent.click(cancel);
    fireEvent.keyDown(screen.getByRole("dialog", { name: "确认取消 AAPL 模拟订单" }), { key: "Escape" });

    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    await waitFor(() => expect(cancel).toHaveFocus());
  });
});
