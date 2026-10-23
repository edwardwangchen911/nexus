import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import WorkspacePage, {
  WorkbenchPanel,
  type WorkbenchPageState,
  type WorkbenchPayload,
} from "../../../app/(protected)/workspace/page";
import { agentPaperCycle, cancellableOrder, meta, paperPortfolio } from "../paper/fixtures";
import { managementSummary } from "../paper/management-fixtures";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const readyPayload = (): WorkbenchPayload => ({
  data: {
    mode: "internal_paper",
    execution_locked: true,
    loop: {
      status: "online",
      reason_code: null,
      started_at: "2026-08-26T19:20:00Z",
      last_seen_at: "2026-08-26T20:00:00Z",
      stale_after_seconds: 30,
    },
    session: {
      decision_session: "2026-08-26",
      status: "completed",
      cycle_count: 1,
      completed_count: 1,
      last_completed_decision_at: agentPaperCycle.decision_time,
      next_eligible_at: "2026-08-26T20:05:00Z",
      next_eligible_reason: "NEXT_SLOT",
      latest_result_code: "NO_ORDER_REQUIRED",
      execution_locked: true,
    },
    latest_cycle: {
      ...agentPaperCycle,
      status: "completed",
      result_code: "NO_ORDER_REQUIRED",
      order_ids: [],
      candidates: [{ ...agentPaperCycle.candidates[0], stance: "hold", sizing_action: "maintain" }],
    },
    active_orders: [],
    account: {
      portfolio_id: paperPortfolio.portfolio_id,
      currency: "USD",
      initial_capital: paperPortfolio.initial_capital,
      data_state: "READY",
      as_of: paperPortfolio.performance!.as_of,
      nav: paperPortfolio.performance!.nav,
      available_cash: paperPortfolio.performance!.available_cash,
      reserved_cash: paperPortfolio.performance!.reserved_cash,
      market_value: paperPortfolio.performance!.market_value,
      realized_pnl: paperPortfolio.performance!.realized_pnl,
      unrealized_pnl: paperPortfolio.performance!.unrealized_pnl,
    },
    attention: [],
  },
  meta: { ...meta(), schema_version: "1" as const },
});

const states: Array<[WorkbenchPageState, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无工作台事实"],
  ["ready", "工作台已更新"],
  ["partial", "部分事实不可用"],
  ["stale", "显示上次数据"],
  ["error", "工作台加载失败"],
  ["forbidden", "无权访问工作台"],
];

describe("decision-first trading workbench", () => {
  it("keeps target and plan in a compact read-only summary", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => Response.json(String(input).includes("management/portfolio") ? { data: managementSummary, meta: meta() } : readyPayload())));
    render(<WorkspacePage />);
    const summary = await screen.findByRole("region", { name: "内部模拟目标与计划摘要" });
    await waitFor(() => expect(summary).toHaveTextContent("目标 V7"));
    expect(within(summary).getByRole("link", { name: "查看调整计划" })).toHaveAttribute("href", `/paper?plan=${managementSummary.occupying_plan!.batch_id}`);
    expect(within(summary).queryByRole("table")).not.toBeInTheDocument();
    expect(within(summary).queryByRole("button")).not.toBeInTheDocument();
  });
  it.each(states)("renders the %s state honestly", (state, label) => {
    const hasFacts = ["ready", "partial", "stale"].includes(state);
    render(<WorkbenchPanel state={state} payload={hasFacts ? readyPayload() : null} refreshing={false} onRefresh={vi.fn()} />);

    expect(screen.getByRole("region", { name: "交易工作台页面" })).toHaveAttribute("data-state", state);
    expect(screen.getByRole("status", { name: "工作台状态" })).toHaveTextContent(label);
    expect(screen.getAllByRole("status")).toHaveLength(1);
    if (!hasFacts) expect(screen.queryByText("AAPL")).not.toBeInTheDocument();
  });

  it("shows a completed cycle honestly after terminal orders leave the active list", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data: managementSummary, meta: meta() })));
    const payload = readyPayload();
    payload.data.latest_cycle!.result_code = "PAPER_CYCLE_COMPLETED";
    payload.data.latest_cycle!.order_ids = [cancellableOrder.order_id];
    render(<WorkbenchPanel state="ready" payload={payload} refreshing={false} onRefresh={vi.fn()} />);
    expect(within(screen.getByRole("region", { name: "最新决策" })).getByText("本周期订单已终态")).toBeVisible();
    const orders = screen.getByRole("region", { name: "活动订单" });
    expect(within(orders).getByText("本周期订单已终态")).toBeVisible();
    expect(within(orders).getByRole("link", { name: "查看全部订单" })).toHaveAttribute("href", "/paper");
    expect(screen.queryByText("已成交")).not.toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole("region", { name: "内部模拟目标与计划摘要" })).toHaveTextContent("目标 V7"));
  });

  it("leads with the decision and exposes no write controls", () => {
    render(<WorkbenchPanel state="ready" payload={readyPayload()} refreshing={false} onRefresh={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "交易工作台" })).toBeVisible();
    expect(screen.getByText("内部模拟")).toBeVisible();
    expect(screen.getByText("实盘锁定")).toBeVisible();
    expect(screen.getByRole("region", { name: "最新决策" })).toBeVisible();
    expect(screen.getByRole("region", { name: "需要处理" })).toBeVisible();
    expect(screen.getByRole("link", { name: "查看完整决策" })).toHaveAttribute(
      "href",
      `/decisions/${readyPayload().data.latest_cycle?.cycle_id}`,
    );
    expect(screen.queryByRole("button", { name: /运行|停止|批准/ })).not.toBeInTheDocument();
  });

  it("distinguishes offline, unknown, no-order, and result-unknown states", () => {
    const offline = readyPayload();
    offline.data.loop.status = "offline";
    offline.data.loop.reason_code = "HEARTBEAT_STALE";
    offline.data.attention = [{
      kind: "service", severity: "warning", title_code: "AGENT_PAPER_LOOP_OFFLINE",
      reason_code: "HEARTBEAT_STALE", safe_action: "wait", href: "/settings",
    }];
    const { rerender } = render(<WorkbenchPanel state="ready" payload={offline} refreshing={false} onRefresh={vi.fn()} />);
    expect(screen.getAllByText("循环离线").length).toBeGreaterThan(0);
    expect(screen.getAllByText("无需下单").length).toBeGreaterThan(0);

    const unknown = readyPayload();
    unknown.data.loop.status = "unknown";
    unknown.data.loop.reason_code = "HEARTBEAT_MISSING";
    unknown.data.latest_cycle = { ...unknown.data.latest_cycle!, status: "partially_filled", order_ids: [] };
    unknown.data.attention = [{
      kind: "execution", severity: "critical", title_code: "PAPER_ORDER_RECONCILING",
      reason_code: "ORDER_RESULT_UNKNOWN", safe_action: "verify_original_result", href: "/paper",
      cycle_id: unknown.data.latest_cycle.cycle_id,
    }];
    rerender(<WorkbenchPanel state="ready" payload={unknown} refreshing={false} onRefresh={vi.fn()} />);
    expect(screen.getByText("循环状态未知")).toBeVisible();
    expect(screen.getAllByText("结果未知").length).toBeGreaterThan(0);
    expect(screen.getByRole("link", { name: "核对原操作结果" })).toHaveAttribute("href", "/paper");
    expect(screen.queryByRole("button", { name: /重试/ })).not.toBeInTheDocument();
  });

  it("loads through GET only", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      void input;
      void init;
      return Response.json(readyPayload());
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<WorkspacePage />);

    expect(await screen.findByText("工作台已更新")).toBeVisible();
    expect(fetchMock).toHaveBeenCalledWith("/api/v1/workbench", expect.objectContaining({ cache: "no-store" }));
    for (const [, init] of fetchMock.mock.calls) {
      expect((init as RequestInit | undefined)?.method ?? "GET").toBe("GET");
      expect((init as RequestInit | undefined)?.signal).toBeInstanceOf(AbortSignal);
    }
  });

  it("retains valid facts when refresh fails and marks them stale", async () => {
    const workbenchRead = vi.fn()
      .mockResolvedValueOnce(Response.json(readyPayload()))
      .mockResolvedValueOnce(Response.json({ error: { code: "UNAVAILABLE" } }, { status: 503 }));
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      if (String(input) === "/api/v1/workbench") return workbenchRead();
      if (String(input) === "/api/v1/paper/management/portfolio") {
        return Response.json({ data: managementSummary, meta: meta() });
      }
      throw new Error("Unexpected test request");
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<WorkspacePage />);
    expect(await screen.findByText("AAPL")).toBeVisible();
    await waitFor(() => expect(screen.getByRole("region", { name: "内部模拟目标与计划摘要" })).toHaveTextContent("目标 V7"));

    fireEvent.click(screen.getByRole("button", { name: "刷新工作台" }));
    await waitFor(() => expect(screen.getByRole("region", { name: "交易工作台页面" })).toHaveAttribute("data-state", "stale"));
    expect(workbenchRead).toHaveBeenCalledTimes(2);
    expect(screen.getByText("AAPL")).toBeVisible();
    expect(screen.getByRole("status", { name: "工作台状态" })).toHaveTextContent("显示上次数据");
  });

  it("clears facts after a forbidden response", async () => {
    const workbenchRead = vi.fn()
      .mockResolvedValueOnce(Response.json(readyPayload()))
      .mockResolvedValueOnce(Response.json({ error: { code: "FORBIDDEN" } }, { status: 403 }));
    const fetchMock = vi.fn((input: RequestInfo | URL) => String(input) === "/api/v1/workbench" ? workbenchRead() : Promise.resolve(new Response(null, { status: 404 })));
    vi.stubGlobal("fetch", fetchMock);
    render(<WorkspacePage />);
    expect(await screen.findByText("AAPL")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "刷新工作台" }));
    expect(await screen.findByText("无权访问工作台")).toBeVisible();
    expect(screen.queryByText("AAPL")).not.toBeInTheDocument();
  });

  it("rejects malformed responses instead of guessing partial data", async () => {
    const malformed = readyPayload() as unknown as { data: Record<string, unknown>; meta: unknown };
    delete (malformed.data.loop as Record<string, unknown>).reason_code;
    vi.stubGlobal("fetch", vi.fn(async () => Response.json(malformed)));
    render(<WorkspacePage />);

    expect(await screen.findByText("工作台加载失败")).toBeVisible();
    expect(screen.queryByText("AAPL")).not.toBeInTheDocument();
  });

  it("shows active orders and account facts without hiding units", () => {
    const payload = readyPayload();
    payload.data.latest_cycle = agentPaperCycle;
    payload.data.active_orders = [cancellableOrder];
    render(<WorkbenchPanel state="ready" payload={payload} refreshing={false} onRefresh={vi.fn()} />);

    const orders = screen.getByRole("region", { name: "活动订单" });
    expect(within(orders).getByText("AAPL")).toBeVisible();
    expect(orders).toHaveTextContent("2 / 5");
    const account = screen.getByRole("region", { name: "账户概况（内部模拟）" });
    expect(account).toHaveTextContent("USD 10,000");
    expect(screen.getByRole("link", { name: "查看账户与持仓" })).toHaveAttribute("href", "/portfolio");
  });
});

it("explains readiness, compatibility, and adjacent-cycle changes without new actions", () => {
  const payload = readyPayload();
  payload.data.readiness = {
    stage: "agent", blocker: "research_not_actionable", reason_code: "RESEARCH_OUTPUT_NOT_ACTIONABLE",
    previous_cycle_id: agentPaperCycle.cycle_id, changes: ["blocker", "result"],
  };
  payload.data.compatibility = {
    status: "incompatible", expected_process_version: "agent-paper-loop-v1",
    release_status: "mismatching", expected_release_revision: "a".repeat(40),
    expected_migration_head: "0064_prompt_release_channel", reported_process_version: "old-loop",
    reported_release_revision: "b".repeat(40),
    reported_migration_head: "0061_agent_paper_job_isolation", database_migration_heads: ["0064_prompt_release_channel"],
  };
  render(<WorkbenchPanel state="ready" payload={payload} refreshing={false} onRefresh={vi.fn()} />);
  const readiness = screen.getByRole("region", { name: "运行就绪与阻断" });
  expect(readiness).toHaveTextContent("研究未通过交易准入");
  expect(readiness).toHaveTextContent("服务运行的代码提交与当前 API 不一致");
  expect(readiness).toHaveTextContent("aaaaaaaaaaaa");
  expect(readiness).toHaveTextContent("bbbbbbbbbbbb");
  expect(readiness).toHaveTextContent("old-loop");
  expect(readiness).toHaveTextContent("阻断分类、结果代码");
  expect(readiness).toHaveTextContent("下一可运行时段");
  expect(within(readiness).queryByRole("button")).not.toBeInTheDocument();
});

it("preserves unknown readiness for an older server and unknown next time", () => {
  const payload = readyPayload();
  payload.data.session.next_eligible_at = null;
  payload.data.session.next_eligible_reason = "CURRENT_CYCLE_IN_PROGRESS";
  render(<WorkbenchPanel state="ready" payload={payload} refreshing={false} onRefresh={vi.fn()} />);
  const readiness = screen.getByRole("region", { name: "运行就绪与阻断" });
  expect(readiness).toHaveTextContent("当前服务尚未提供运行解释");
  expect(readiness).toHaveTextContent("当前周期结束后再判断");
  expect(readiness).toHaveTextContent("兼容状态未知");
});

it("rejects malformed readiness instead of rendering an invented phase", async () => {
  const payload = readyPayload();
  Object.assign(payload.data, { readiness: { stage: "auto_trade", blocker: "none", changes: null } });
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json(payload)));
  render(<WorkspacePage />);
  await waitFor(() => expect(screen.getByRole("status", { name: "工作台状态" })).toHaveTextContent("工作台加载失败"));
  expect(screen.queryByRole("region", { name: "运行就绪与阻断" })).not.toBeInTheDocument();
});

const frozenActivity = () => ({ read_at: "2026-10-23T08:00:00Z", read_status: "complete", scopes: [{
  mode: "research_and_paper", today_registered: 1, today_started: 0, registration_date: "2026-10-23", decision_session: "2026-10-23",
  selection: "active", request_id: "00000000-0000-4000-8000-000000000001", cycle_id: null, requested_at: "2026-10-23T07:00:00Z",
  status: "preparing", reason_code: null, trigger: "manual", historical: false, occupancy: "not_indicated",
  portfolio_status: "awaiting_cycle", portfolio_reason_code: null, href: null, candidates: [],
}] });

it("recognizes a preparing request without a cycle as saved facts and retains the same round on timeout", async () => {
  const payload = readyPayload();
  payload.data.latest_cycle = null;
  payload.data.account = null;
  Object.assign(payload.data, { activity: frozenActivity() });
  const workbenchRead = vi.fn().mockResolvedValueOnce(Response.json(payload)).mockRejectedValueOnce(new DOMException("Timeout", "TimeoutError"));
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => String(input) === "/api/v1/workbench" ? workbenchRead() : Promise.resolve(new Response(null, { status: 404 }))));
  const timeout = vi.spyOn(AbortSignal, "timeout");
  render(<WorkspacePage />);
  expect(await screen.findByText(/正在准备；尚未创建周期/)).toBeVisible();
  expect(screen.getByRole("region", { name: "交易工作台页面" })).toHaveAttribute("data-state", "ready");
  const before = screen.getByRole("region", { name: "本轮运行与研究限制" }).textContent;
  fireEvent.click(screen.getByRole("button", { name: "刷新工作台" }));
  await waitFor(() => expect(screen.getByRole("region", { name: "交易工作台页面" })).toHaveAttribute("data-state", "stale"));
  expect(screen.getByRole("region", { name: "本轮运行与研究限制" }).textContent).toBe(before);
  expect(timeout).toHaveBeenCalledWith(10_000);
});

it.each([401, 403])("clears frozen round detail after access is lost (%s)", async (status) => {
  const payload = readyPayload();
  Object.assign(payload.data, { activity: frozenActivity() });
  const workbenchRead = vi.fn().mockResolvedValueOnce(Response.json(payload)).mockResolvedValueOnce(new Response(null, { status }));
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => String(input) === "/api/v1/workbench" ? workbenchRead() : Promise.resolve(new Response(null, { status: 404 }))));
  render(<WorkspacePage />);
  expect(await screen.findByText(/正在准备；尚未创建周期/)).toBeVisible();
  fireEvent.click(screen.getByRole("button", { name: "刷新工作台" }));
  expect(await screen.findByText("无权访问工作台")).toBeVisible();
  expect(screen.queryByRole("region", { name: "本轮运行与研究限制" })).not.toBeInTheDocument();
  expect(screen.queryByText("00000000-0000-4000-8000-000000000001")).not.toBeInTheDocument();
});

it("isolates malformed activity without discarding other valid workbench facts", async () => {
  const payload = readyPayload();
  Object.assign(payload.data, { activity: { ...frozenActivity(), scopes: "malformed" } });
  vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => Promise.resolve(String(input) === "/api/v1/workbench" ? Response.json(payload) : new Response(null, { status: 404 }))));
  render(<WorkspacePage />);
  expect(await screen.findByText("本轮摘要格式不可核验；未展示未核验正文。")).toBeVisible();
  expect(screen.getByRole("region", { name: "最新决策" })).toHaveTextContent("AAPL");
  expect(screen.getByRole("region", { name: "交易工作台页面" })).toHaveAttribute("data-state", "partial");
});

it.each([
  ["CURRENT_SLOT", "当前定时启动区间"], ["NEXT_SLOT", "下一定时启动区间"], ["SESSION_OPEN", "本交易日定时启动时点"],
] as const)("describes %s as the producer's scheduled window", (reason, label) => {
  const payload = readyPayload();
  payload.data.session.next_eligible_reason = reason;
  render(<WorkbenchPanel state="ready" payload={payload} refreshing={false} onRefresh={vi.fn()} />);
  const readiness = screen.getByRole("region", { name: "运行就绪与阻断" });
  expect(readiness).toHaveTextContent(label);
  expect(readiness).not.toHaveTextContent("五分钟槽");
  expect(readiness).not.toHaveTextContent("开盘时间");
});
