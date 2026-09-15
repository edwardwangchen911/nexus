import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import PaperPage, { PaperPanel } from "../../../app/(protected)/paper/page";
import { AgentPaperCycle } from "../../../app/components/agent-paper-cycle";
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

describe("internal paper page", () => {
  it("separates realtime Schwab decision quotes from unknown-delivery execution bars", () => {
    render(<AgentPaperCycle cycles={[agentPaperCycle]} />);

    const region = screen.getByRole("region", { name: "当日 Agent 模拟周期" });
    expect(region).toHaveTextContent("决策报价 · Schwab Level One · 实时");
    expect(region).toHaveTextContent("执行分钟线 · Schwab pricehistory · 实时性未由响应声明");
    expect(region).not.toHaveTextContent("下一交易日");
    expect(region).not.toHaveTextContent("嘉信实时成交");
    expect(within(region).queryByRole("button")).not.toBeInTheDocument();
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
    expect(screen.queryByText("最大回撤")).not.toBeInTheDocument();
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
    let cancels = 0;
    vi.spyOn(crypto, "randomUUID").mockReturnValue("0198f7a0-1111-7777-8111-111111111131");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "/api/v1/paper/portfolio") return Response.json({ data: paperPortfolio, meta: meta() });
      if (url === "/api/v1/paper/orders?limit=20") return Response.json({ data: { items: [cancellableOrder], next_cursor: null }, meta: meta() });
      if (url.endsWith("/cancel")) {
        cancels += 1; keys.push(new Headers(init?.headers).get("idempotency-key"));
        if (cancels === 1) throw new TypeError("network result unknown");
        return Response.json({ data: { order_id: cancellableOrder.order_id, status: "cancelled", filled_quantity: "2", remaining_quantity: "3", result_event_seq: 5, replayed: true }, meta: meta() });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PaperPage />);
    fireEvent.click(await screen.findByRole("button", { name: "取消 AAPL 模拟订单" }));
    fireEvent.click(screen.getByRole("button", { name: "确认取消模拟订单" }));
    expect((await screen.findAllByText(/取消结果未知/)).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: "继续确认原操作结果" }));
    await act(async () => { await Promise.resolve(); });

    expect(cancels).toBe(2);
    expect(new Set(keys).size).toBe(1);
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
