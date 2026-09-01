import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import PortfolioPage, {
  PortfolioPanel,
  type PortfolioItem,
  type PortfolioSnapshot,
} from "../../../app/(protected)/portfolio/page";
import { PortfolioSummary } from "../../../app/components/portfolio-summary";
import type { DataStateKind } from "../../../app/components/data-state";
import { portfolio, portfolioId, responseMeta, snapshot } from "./fixtures";

const stateLabels: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"], ["empty", "暂无数据"], ["ready", "数据已就绪"],
  ["partial", "数据不完整"], ["stale", "数据已过期"], ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("portfolio vertical slice", () => {
  it.each(stateLabels)("renders the visible %s data state", (state, label) => {
    render(<PortfolioPanel
      state={state}
      portfolios={["ready", "partial", "stale"].includes(state) ? [portfolio] : []}
      snapshot={["ready", "partial", "stale"].includes(state) ? snapshot : null}
      selectedPortfolioId={portfolioId}
      notice={null}
      onSelect={vi.fn()}
      onRefresh={vi.fn()}
    />);

    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "组合事实页面" })).toHaveAttribute("data-state", state);
  });

  it("keeps original currencies separate and labels both cost definitions", () => {
    render(<PortfolioSummary portfolio={portfolio} snapshot={snapshot} currency="HKD" />);

    const summary = screen.getByRole("region", { name: "Fosun read-only HKD 组合摘要" });
    expect(summary).toHaveTextContent("HKD");
    expect(summary).toHaveTextContent("8,000.00");
    expect(summary).toHaveTextContent("平均成本");
    expect(summary).toHaveTextContent("6.20");
    expect(summary).toHaveTextContent("摊薄成本");
    expect(summary).toHaveTextContent("5.75");
    expect(summary).not.toHaveTextContent("USD");
    expect(summary).not.toHaveTextContent("9,000");
    expect(screen.queryByText(/折算总资产/)).not.toBeInTheDocument();
  });

  it("shows deterministic original-currency valuation and allocation with source time", () => {
    render(<PortfolioSummary portfolio={portfolio} snapshot={snapshot} currency="HKD" />);

    const valuation = screen.getByRole("region", { name: "HKD 原币种资产估值" });
    expect(valuation).toHaveTextContent("复星（只读）");
    expect(valuation).toHaveTextContent("HKD 27,500.00");
    expect(valuation).toHaveTextContent("HKD 19,500.00");
    expect(valuation).toHaveTextContent("HKD 8,000.00");
    expect(valuation).toHaveTextContent("现金 29.09%");
    expect(valuation).toHaveTextContent("00656.HK 70.91%");
    expect(valuation).toHaveTextContent("2026-08-23 08:00:00 UTC");
    expect(valuation).not.toHaveTextContent("折算总资产");
  });

  it("fails closed instead of presenting a partial total when a holding price is missing", () => {
    const missingPrice: PortfolioSnapshot = {
      ...snapshot,
      positions: snapshot.positions.map((position) => ({ ...position, current_price: null })),
    };
    render(<PortfolioSummary portfolio={portfolio} snapshot={missingPrice} currency="HKD" />);

    const valuation = screen.getByRole("region", { name: "HKD 原币种资产估值" });
    expect(valuation).toHaveTextContent("总资产无法计算");
    expect(valuation).toHaveTextContent("缺失现价：00656.HK");
    expect(valuation).not.toHaveTextContent("HKD 27,500.00");
    expect(valuation).not.toHaveTextContent(/\d+\.\d+%/);
  });

  it("keeps valuation exact beyond the JavaScript safe integer range", () => {
    const exactLargeCash: PortfolioSnapshot = {
      ...snapshot,
      cash: [{
        currency: "HKD",
        cash_balance: "9007199254740993.00",
        available_cash: "9007199254740993.00",
        provider_as_of: "2026-08-23T08:00:00Z",
      }],
      positions: [],
      open_orders: [],
    };
    render(<PortfolioSummary portfolio={portfolio} snapshot={exactLargeCash} currency="HKD" />);

    expect(screen.getByRole("region", { name: "HKD 原币种资产估值" }))
      .toHaveTextContent("HKD 9,007,199,254,740,993.00");
  });

  it("withholds allocation percentages when original-currency total assets are zero", () => {
    const zeroAssets: PortfolioSnapshot = {
      ...snapshot,
      cash: [{
        currency: "HKD",
        cash_balance: "0.00",
        available_cash: "100.00",
        provider_as_of: "2026-08-23T08:00:00Z",
      }],
      positions: [],
      open_orders: [],
    };
    render(<PortfolioSummary portfolio={portfolio} snapshot={zeroAssets} currency="HKD" />);

    const valuation = screen.getByRole("region", { name: "HKD 原币种资产估值" });
    expect(valuation).toHaveTextContent("HKD 0.00");
    expect(valuation).toHaveTextContent("配置结构无法计算：原币种总资产为零");
    expect(valuation).not.toHaveTextContent("%");
  });

  it("links supported US holdings to their persisted market evidence without linking HK", () => {
    const msftId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e709";
    const withUsHolding: PortfolioSnapshot = {
      ...snapshot,
      cash: [
        ...snapshot.cash,
        { currency: "USD", cash_balance: "1000.00", available_cash: "900.00", provider_as_of: "2026-08-23T08:00:00Z" },
      ],
      positions: [
        ...snapshot.positions,
        {
          ...snapshot.positions[0],
          instrument_id: msftId,
          symbol: "MSFT",
          exchange: "XNYS",
          currency: "USD",
          quantity: "2",
          available_quantity: "2",
          frozen_quantity: "0",
          current_price: "500",
        },
      ],
    };

    const { rerender } = render(<PortfolioSummary portfolio={portfolio} snapshot={withUsHolding} currency="USD" />);
    expect(screen.getByRole("link", { name: "查看 MSFT 行情和 K 线" })).toHaveAttribute(
      "href",
      `/market?selected=${msftId}`,
    );

    rerender(<PortfolioSummary portfolio={portfolio} snapshot={withUsHolding} currency="HKD" />);
    expect(screen.queryByRole("link", { name: /查看 00656\.HK 行情和 K 线/ })).not.toBeInTheDocument();
  });

  it("shows 00656.HK and broker orders as read-only without leaking poisoned IDs", () => {
    const poisoned = {
      ...snapshot,
      sub_account_id: "sub-account-must-not-render",
      provider_order_id: "provider-order-must-not-render",
      ciphertext: "ciphertext-must-not-render",
    } as PortfolioSnapshot;
    render(<PortfolioSummary portfolio={portfolio} snapshot={poisoned} currency="HKD" />);

    expect(screen.getAllByText("00656.HK").length).toBeGreaterThan(0);
    expect(screen.getAllByText("只读").length).toBeGreaterThan(0);
    expect(screen.getByText(/香港交易所财报证据尚未准入/)).toBeVisible();
    expect(screen.queryByRole("button", { name: /买入|卖出|下单|修改持仓/ })).not.toBeInTheDocument();
    expect(document.body).not.toHaveTextContent(/sub-account-must|provider-order-must|ciphertext-must/);
  });

  it("labels a stale broker snapshot as last-known and non-actionable", () => {
    render(<PortfolioPanel state="stale" portfolios={[{ ...portfolio, latest_snapshot_status: "STALE" }]}
      snapshot={{ ...snapshot, status: "STALE" }} selectedPortfolioId={portfolioId}
      notice={null} onSelect={vi.fn()} onRefresh={vi.fn()} />);

    expect(screen.getByText("显示最后已知快照，不触发业务动作。", { exact: true })).toBeVisible();
    expect(screen.getByRole("region", { name: "HKD 原币种资产估值" })).toHaveTextContent("STALE");
    expect(screen.queryByRole("link", { name: /优化|方案|下单|交易/ })).not.toBeInTheDocument();
  });

  it("keeps an open order visible even when its instrument has no position", () => {
    const orderOnlySnapshot: PortfolioSnapshot = {
      ...snapshot,
      open_orders: [{
        ...snapshot.open_orders[0],
        broker_order_snapshot_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e706",
        instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e707",
        symbol: "MSFT",
        currency: "USD",
      }],
    };
    render(<PortfolioSummary portfolio={portfolio} snapshot={orderOnlySnapshot} currency="USD" />);

    expect(screen.getByRole("table", { name: /MSFT|\u8131\u654f\u5185\u90e8\u5feb\u7167 ID/ })).toHaveTextContent("MSFT");
    expect(screen.getByRole("region", { name: "Fosun read-only USD 组合摘要" })).not.toHaveTextContent("00656.HK");
  });

  it("creates an original-currency section for a currency present only in open orders", () => {
    const orderOnlyCurrencySnapshot: PortfolioSnapshot = {
      ...snapshot,
      cash: snapshot.cash.filter((item) => item.currency === "HKD"),
      open_orders: [{
        ...snapshot.open_orders[0],
        broker_order_snapshot_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e706",
        instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e707",
        symbol: "MSFT",
        currency: "USD",
      }],
    };
    render(<PortfolioPanel state="ready" portfolios={[{ ...portfolio, original_currencies: ["HKD"] }]}
      snapshot={orderOnlyCurrencySnapshot} selectedPortfolioId={portfolioId}
      notice={null} onSelect={vi.fn()} onRefresh={vi.fn()} />);

    expect(screen.getByRole("region", { name: "Fosun read-only USD 组合摘要" })).toHaveTextContent("MSFT");
  });

  it("keeps the portfolio rail usable when the selected portfolio has no snapshot", () => {
    const replacement = { ...portfolio, portfolio_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799", name: "Replacement portfolio" };
    render(<PortfolioPanel state="error" portfolios={[portfolio, replacement]} snapshot={null}
      selectedPortfolioId={portfolioId} notice={null}
      onSelect={vi.fn()} onRefresh={vi.fn()} />);

    expect(screen.getByRole("navigation", { name: "组合选择" })).toBeVisible();
    expect(screen.getByRole("button", { name: /Replacement portfolio/ })).toBeVisible();
    expect(screen.getAllByText("加载失败").length).toBeGreaterThan(0);
  });

  it("keeps real refresh terminal-only and exposes the persisted run status", () => {
    render(<PortfolioPanel state="ready" portfolios={[portfolio]} snapshot={snapshot}
      selectedPortfolioId={portfolioId} notice={null}
      onSelect={vi.fn()} onRefresh={vi.fn()} />);

    const guidance = screen.getByRole("region", { name: "真实刷新仅限本机终端" });
    expect(guidance).toHaveTextContent("浏览器只重新读取已持久化数据");
    expect(guidance).toHaveTextContent("uv run --package nexus-engine nexus-engine-fosun-local-refresh");
    expect(guidance).toHaveTextContent("最近本地刷新状态READY");
    expect(screen.queryByRole("button", { name: "同步复星只读事实" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "刷新同步状态" })).not.toBeInTheDocument();
  });

  it("shows a historical ACTIVE registration without polling or promising execution", () => {
    render(<PortfolioPanel state="ready"
      portfolios={[{ ...portfolio, last_sync_run_status: "ACTIVE" }]}
      snapshot={snapshot} selectedPortfolioId={portfolioId}
      notice={null} onSelect={vi.fn()} onRefresh={vi.fn()} />);

    expect(screen.getByText(/存在历史 ACTIVE 登记/)).toBeVisible();
    expect(screen.getByText(/当前页面不会执行、轮询或自动重试/)).toBeVisible();
    expect(screen.queryByRole("button", { name: /同步|继续确认/ })).not.toBeInTheDocument();
  });

  it("refreshes persisted portfolio data without creating a broker sync run", async () => {
    const requests: Array<{ url: string; method: string }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      requests.push({ url, method: init?.method ?? "GET" });
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "刷新页面数据" }));
    await waitFor(() => expect(requests.filter((request) => request.url.endsWith("/snapshots/latest"))).toHaveLength(2));

    expect(requests.every((request) => request.method === "GET")).toBe(true);
    expect(requests.some((request) => request.url.includes("broker-sync-runs"))).toBe(false);
  });

  it("loads every bounded portfolio page instead of silently ignoring next_cursor", async () => {
    const replacement = { ...portfolio, portfolio_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799", name: "Replacement portfolio" };
    const cursors: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) {
        cursors.push(url);
        return url.includes("cursor=")
          ? Response.json({ data: { items: [replacement], next_cursor: null }, meta: responseMeta() })
          : Response.json({ data: { items: [portfolio], next_cursor: "opaque+cursor" }, meta: responseMeta() });
      }
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);

    expect(await screen.findByRole("button", { name: /Replacement portfolio/ })).toBeVisible();
    expect(cursors).toEqual([
      "/api/v1/portfolios?kind=real_read_only&limit=50",
      "/api/v1/portfolios?kind=real_read_only&limit=50&cursor=opaque%2Bcursor",
    ]);
  });

  it("discards a slow portfolio-list body after a newer generation wins", async () => {
    const replacement = { ...portfolio, portfolio_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799", name: "Replacement portfolio" };
    const replacementSnapshot = { ...snapshot, portfolio_id: replacement.portfolio_id };
    let listCalls = 0;
    let releaseOldList: ((value: { data: { items: PortfolioItem[]; next_cursor: null } }) => void) | undefined;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) {
        listCalls += 1;
        if (listCalls === 1) {
          const body = new Promise<{ data: { items: PortfolioItem[]; next_cursor: null } }>((resolve) => { releaseOldList = resolve; });
          return { ok: true, status: 200, json: () => body } as Response;
        }
        return Response.json({ data: { items: [replacement], next_cursor: null }, meta: responseMeta() });
      }
      if (url.includes(replacement.portfolio_id)) return Response.json({ data: replacementSnapshot, meta: responseMeta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    await waitFor(() => expect(releaseOldList).toBeTypeOf("function"));
    fireEvent.click(screen.getByRole("button", { name: "刷新页面数据" }));
    expect(await screen.findByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    await act(async () => { releaseOldList?.({ data: { items: [portfolio], next_cursor: null } }); await Promise.resolve(); });

    expect(screen.getByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "Fosun read-only HKD 组合摘要" })).not.toBeInTheDocument();
  });

  it("does not attach an old snapshot to a newly selected portfolio after refresh failure", async () => {
    const replacement = { ...portfolio, portfolio_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799", name: "Replacement portfolio" };
    let listCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) {
        listCalls += 1;
        return Response.json({ data: { items: [listCalls === 1 ? portfolio : replacement], next_cursor: null }, meta: responseMeta() });
      }
      if (url.includes(portfolioId)) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url.includes(replacement.portfolio_id)) return Response.json({ error: { code: "INTERNAL_ERROR" } }, { status: 500 });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    expect(await screen.findByRole("region", { name: "Fosun read-only HKD 组合摘要" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "刷新页面数据" }));

    expect(await screen.findByText("无法读取组合事实。当前状态保持只读。")).toBeVisible();
    expect(screen.queryByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).not.toBeInTheDocument();
    expect(screen.queryByText("HKD 8,000.00")).not.toBeInTheDocument();
  });

  it("discards a slow snapshot body after a newer portfolio generation wins", async () => {
    const replacement = { ...portfolio, portfolio_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e798", name: "Replacement portfolio" };
    const replacementSnapshot = { ...snapshot, portfolio_id: replacement.portfolio_id };
    let listCalls = 0;
    let releaseOldBody: ((value: { data: PortfolioSnapshot }) => void) | undefined;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) {
        listCalls += 1;
        return Response.json({ data: { items: [listCalls === 1 ? portfolio : replacement], next_cursor: null }, meta: responseMeta() });
      }
      if (url.includes(portfolioId)) {
        const body = new Promise<{ data: PortfolioSnapshot }>((resolve) => { releaseOldBody = resolve; });
        return { ok: true, status: 200, json: () => body } as Response;
      }
      if (url.includes(replacement.portfolio_id)) return Response.json({ data: replacementSnapshot, meta: responseMeta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    await waitFor(() => expect(releaseOldBody).toBeTypeOf("function"));
    fireEvent.click(screen.getByRole("button", { name: "刷新页面数据" }));
    expect(await screen.findByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    await act(async () => { releaseOldBody?.({ data: snapshot }); await Promise.resolve(); });

    expect(screen.getByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "Fosun read-only HKD 组合摘要" })).not.toBeInTheDocument();
  });

  it("uses one atomic polite live region for async portfolio outcomes", () => {
    render(<PortfolioPanel state="ready" portfolios={[portfolio]} snapshot={snapshot}
      selectedPortfolioId={portfolioId}
      notice={{ kind: "success", text: "页面数据已刷新。" }} onSelect={vi.fn()}
      onRefresh={vi.fn()} />);
    const live = screen.getByRole("status", { name: "组合操作状态" });
    expect(live).toHaveAttribute("aria-live", "polite");
    expect(live).toHaveAttribute("aria-atomic", "true");
    expect(screen.getAllByRole("status")).toHaveLength(1);
  });
});
