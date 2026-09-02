import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import PortfolioPage, {
  PortfolioPanel,
  type PortfolioItem,
  type PortfolioSnapshot,
} from "../../../app/(protected)/portfolio/page";
import { PortfolioSummary, PortfolioValuationSummary } from "../../../app/components/portfolio-summary";
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
  it("connects a partial frozen Agent report without calling a model or promoting readiness", () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const position = { ...snapshot.positions[0], symbol: "TEST", research: {
      read_at: "2026-09-02T08:00:00Z", status: "completed" as const,
      run_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e710", data_snapshot_id: snapshot.portfolio_snapshot_id,
      decision_time: "2026-09-02T04:00:00Z", quality_status: "partial" as const,
      summary: "冻结财报收入有变化。", uncertainties: ["Price authority is unavailable."],
      newer_inputs_available: true,
    } };
    render(<PortfolioSummary portfolio={portfolio} snapshot={{ ...snapshot, positions: [position] }} currency="HKD" />);
    expect(screen.getByText(/Agent 研究输出已生成 1 \/ 1/)).toHaveTextContent("证据完整 0，证据不完整 1");
    expect(screen.getByText(/Agent 研究输出已生成 1 \/ 1/)).toHaveTextContent("不等于当前组合诊断通过；交易保持锁定");
    expect(screen.getByText("研究输出已生成 · 证据不完整")).toBeVisible();
    expect(screen.getByText("已有更新的冻结资料，当前报告未使用")).toBeVisible();
    expect(screen.getByRole("link", { name: "查看 TEST 研究报告" })).toHaveAttribute("href", `/research/${position.instrument_id}?as_of=2026-09-02T04%3A00%3A00Z`);
    expect(screen.getByText("研究截止 2026-09-02 12:00:00 北京时间")).toBeVisible();
    fireEvent.click(screen.getByText("摘要与证据缺口"));
    expect(screen.getByText("冻结财报收入有变化。")).toBeVisible();
    expect(screen.getByText("缺少合格价格证据，不能生成仓位动作。")).toBeVisible();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it.each([
    ["not_started", null, "研究证据待补齐"],
    ["not_started", snapshot.portfolio_snapshot_id, "研究资料已冻结 · 待研究"],
    ["prepared", snapshot.portfolio_snapshot_id, "研究已登记 · 待执行"],
    ["failed", snapshot.portfolio_snapshot_id, "研究失败 · 保持观望"],
    ["completed", snapshot.portfolio_snapshot_id, "结果待核验"],
  ] as const)("does not turn %s inputs into a completed report", (status, data_snapshot_id, label) => {
    const position = { ...snapshot.positions[0], symbol: "TEST", research: {
      read_at: "2026-09-02T08:00:00Z", status, data_snapshot_id,
      decision_time: data_snapshot_id ? "2026-09-02T04:00:00Z" : null,
      uncertainties: [], newer_inputs_available: false,
    } };
    render(<PortfolioSummary portfolio={portfolio} snapshot={{ ...snapshot, positions: [position] }} currency="HKD" />);
    expect(screen.getByText(label)).toBeVisible();
    expect(screen.getByText(/Agent 研究输出已生成 0 \/ 1/)).toBeVisible();
    if (data_snapshot_id) expect(screen.getByRole("link", { name: "查看 TEST 研究资料" })).toBeVisible();
    else expect(screen.queryByRole("link", { name: /研究资料/ })).not.toBeInTheDocument();
  });

  it("labels observed snapshots without inventing provider update timestamps", () => {
    render(<PortfolioPanel state="ready" portfolios={[portfolio]}
      snapshot={{ ...snapshot, time_basis: "gateway_observed" }}
      selectedPortfolioId={portfolioId} notice={null} onSelect={vi.fn()} onRefresh={vi.fn()}
    />);
    expect(screen.getByText("读取时间")).toBeInTheDocument();
    expect(screen.getByText(/券商未提供逐条更新时间/)).toBeInTheDocument();
    expect(screen.getByText("执行能力").parentElement).toHaveTextContent("已锁定");
  });

  it.each([false, true])("uses the same cash time provenance with valuation shown=%s", (showValuation) => {
    const { rerender } = render(<PortfolioSummary portfolio={portfolio} snapshot={{ ...snapshot,
      time_basis: "gateway_observed", cash: [{ ...snapshot.cash[1], provider_as_of: null }],
    }} currency="USD" showValuation={showValuation} />);
    expect(screen.getByText("本机读取时间").parentElement).toHaveTextContent("2026-08-23 16:00:00 北京时间");
    expect(screen.queryByText("数据更新时间")).not.toBeInTheDocument();
    rerender(<PortfolioSummary portfolio={portfolio} snapshot={{ ...snapshot,
      time_basis: "gateway_observed", cash: [{ ...snapshot.cash[1], provider_as_of: "2026-08-23T07:00:00Z" }],
    }} currency="USD" showValuation={showValuation} />);
    expect(screen.getByText("数据源更新时间").parentElement).toHaveTextContent("2026-08-23 15:00:00 北京时间");
    expect(screen.queryByText("本机读取时间")).not.toBeInTheDocument();
    rerender(<PortfolioSummary portfolio={portfolio} snapshot={{ ...snapshot,
      time_basis: "unknown", cash: [{ ...snapshot.cash[1], provider_as_of: null }],
    }} currency="USD" showValuation={showValuation} />);
    expect(screen.getByText("快照时间（含义未确认）").parentElement).toHaveTextContent("2026-08-23 16:00:00 北京时间");
    rerender(<PortfolioSummary portfolio={portfolio} snapshot={{ ...snapshot, as_of: null,
      time_basis: "unknown", cash: [{ ...snapshot.cash[1], provider_as_of: null }],
    }} currency="USD" showValuation={showValuation} />);
    expect(screen.getByText("快照时间（含义未确认）").parentElement).toHaveTextContent("未知");
  });

  it("separates complete and partial research and excludes read-only HK holdings from coverage", () => {
    const research = { read_at: "2026-09-02T08:00:00Z", status: "completed" as const,
      quality_status: "complete" as const, uncertainties: [], newer_inputs_available: false };
    render(<PortfolioSummary portfolio={portfolio} snapshot={{ ...snapshot, positions: [
      { ...snapshot.positions[0], research },
      { ...snapshot.positions[0], instrument_id: "test-a", symbol: "TESTA", research },
      { ...snapshot.positions[0], instrument_id: "test-b", symbol: "TESTB", research: { ...research, quality_status: "partial" } },
    ] }} currency="HKD" />);
    expect(screen.getByText(/Agent 研究输出已生成 2 \/ 2/)).toHaveTextContent("证据完整 1，证据不完整 1");
    expect(screen.getByText("研究输出已生成 · 证据完整")).toBeVisible();
    expect(screen.getByText("只读展示，不生成基本面判断")).toBeVisible();
  });

  it("shows cash-flow reconciliation and the permanent execution lock", () => {
    render(<PortfolioPanel
      state="ready"
      portfolios={[portfolio]}
      snapshot={snapshot}
      selectedPortfolioId={portfolioId}
      notice={null}
      onSelect={vi.fn()}
      onRefresh={vi.fn()}
    />);

    expect(screen.getByText("现金流对账").parentElement).toHaveTextContent("已通过 · 1 条");
    expect(screen.getByText("执行能力").parentElement).toHaveTextContent("已锁定");
  });

  it("keeps compact asset totals visible while source details open on demand", () => {
    const { container } = render(<PortfolioValuationSummary provider="fosun" snapshot={snapshot} currency="HKD" compact />);
    expect(screen.getByText("HKD 27,500.00")).toBeVisible();
    expect(screen.getByText("复星（只读）")).not.toBeVisible();
    fireEvent.click(screen.getByText("配置、来源与估值口径"));
    expect(screen.getByText("复星（只读）")).toBeVisible();
    expect(screen.getByText("2026-08-23 16:00:00 北京时间")).toBeVisible();
    const bars = container.querySelectorAll<HTMLElement>(".portfolio-allocation__meter > span");
    expect([...bars].map((bar) => bar.style.inlineSize)).toEqual(["29.09%", "70.91%"]);
  });

  it("does not invent a bounded allocation bar for negative or leveraged shares", () => {
    const { container } = render(<PortfolioValuationSummary provider="fosun" snapshot={{ ...snapshot, cash: [{ ...snapshot.cash[0], cash_balance: "-1000.00" }] }} currency="HKD" compact />);
    fireEvent.click(screen.getByText("配置、来源与估值口径"));
    expect(screen.getByText("-5.41%")).toBeVisible();
    expect(screen.getByText("105.41%")).toBeVisible();
    expect(container.querySelectorAll(".portfolio-allocation__meter")).toHaveLength(0);
  });

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

    expect(screen.getByText(`快照展示：${label}`)).toBeVisible();
    expect(screen.getByRole("region", { name: "组合事实页面" })).toHaveAttribute("data-state", state);
  });

  it("keeps original currencies separate and labels both cost definitions", () => {
    render(<PortfolioSummary portfolio={portfolio} snapshot={snapshot} currency="HKD" />);

    const summary = screen.getByRole("region", { name: "Fosun read-only HKD 组合摘要" });
    expect(summary).toHaveTextContent("HKD");
    expect(summary).toHaveTextContent("8,000.00");
    expect(summary).toHaveTextContent("平均成本");
    expect(summary).toHaveTextContent("6.2");
    expect(summary).toHaveTextContent("摊薄成本");
    expect(summary).toHaveTextContent("5.75");
    expect(summary).not.toHaveTextContent("USD");
    expect(summary).not.toHaveTextContent("9,000");
    expect(screen.queryByText(/折算总资产/)).not.toBeInTheDocument();
  });

  it("prioritizes value and allocation while frozen quantities remain visible before opening details", () => {
    render(<PortfolioSummary portfolio={portfolio} snapshot={snapshot} currency="HKD" showValuation={false} />);
    const holdings = screen.getByRole("region", { name: "HKD 持仓表，可横向滚动" });
    expect(within(holdings).getAllByRole("columnheader").map((cell) => cell.textContent)).toEqual(["标的", "持仓市值", "资产占比（含现金）", "现价", "总量", "Agent 研究", "账户明细"]);
    expect(screen.getByText("70.91%")).toBeVisible();
    expect(screen.getByText("冻结 200")).toBeVisible();
    expect(screen.getByText("平均成本")).not.toBeVisible();
    fireEvent.click(screen.getByText("成本与数量 · 只读"));
    expect(screen.getByText("平均成本")).toBeVisible();
    expect(screen.getByText("HKD 6.2")).toBeVisible();
    expect(screen.getByText("摊薄成本")).toBeVisible();
  });

  it("condenses cash-only currencies without hiding available cash or adding a holdings table", () => {
    render(<PortfolioSummary portfolio={portfolio} snapshot={snapshot} currency="USD" showValuation={false} />);
    expect(screen.getByText("USD 1,000")).toBeVisible();
    expect(screen.getByText("USD 900")).toBeVisible();
    expect(screen.getByText("无持仓 · 无未结订单 · 只读")).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("keeps incomplete valuation warnings visible with details collapsed", () => {
    render(<PortfolioValuationSummary provider="fosun" snapshot={{ ...snapshot, positions: [{ ...snapshot.positions[0], current_price: null }] }} currency="HKD" compact />);
    expect(screen.getByText(/缺失现价/)).toBeVisible();
    expect(screen.getByText("复星（只读）")).not.toBeVisible();
  });

  it("does not condense currencies with open orders into a cash-only row", () => {
    render(<PortfolioSummary portfolio={portfolio} snapshot={{ ...snapshot, positions: [] }} currency="HKD" showValuation={false} />);
    expect(screen.getByRole("region", { name: "HKD 未结订单表，可横向滚动" })).toBeVisible();
    expect(screen.queryByText("无持仓 · 无未结订单 · 只读")).not.toBeInTheDocument();
  });

  it("shows deterministic original-currency valuation and allocation with source time", () => {
    render(<PortfolioSummary portfolio={portfolio} snapshot={snapshot} currency="HKD" />);

    const valuation = screen.getByRole("region", { name: "HKD 原币种资产估值" });
    expect(valuation).toHaveTextContent("复星（只读）");
    expect(valuation).toHaveTextContent("HKD 27,500.00");
    expect(valuation).toHaveTextContent("HKD 19,500.00");
    expect(valuation).toHaveTextContent("HKD 8,000.00");
    expect(within(valuation).getByText("现金", { selector: "li > span" }).closest("li")).toHaveTextContent("29.09%");
    expect(within(valuation).getByText("00656.HK", { selector: "li > span" }).closest("li")).toHaveTextContent("70.91%");
    expect(valuation).toHaveTextContent("2026-08-23 16:00:00 北京时间");
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
      snapshot={{ ...snapshot, status: "STALE", decision_readiness: "DATA_STALE" }} selectedPortfolioId={portfolioId}
      notice={null} onSelect={vi.fn()} onRefresh={vi.fn()} />);

    expect(screen.getByText("显示最后已知快照，不触发业务动作。", { exact: true })).toBeVisible();
    expect(screen.getByRole("region", { name: "HKD 原币种资产估值" })).toHaveTextContent("STALE");
    expect(screen.queryByRole("link", { name: /优化|方案|下单|交易/ })).not.toBeInTheDocument();
  });

  it("separates display availability from decision readiness", () => {
    render(<PortfolioPanel state="partial" portfolios={[{ ...portfolio, latest_snapshot_status: "PARTIAL" }]}
      snapshot={{ ...snapshot, status: "PARTIAL", decision_readiness: "RECONCILIATION_PENDING" }}
      selectedPortfolioId={portfolioId} notice={null} onSelect={vi.fn()} onRefresh={vi.fn()} />);

    expect(screen.getByText("资产可展示", { exact: true })).toBeVisible();
    expect(screen.getByText("决策待对账", { exact: true })).toBeVisible();
    expect(screen.getByText("资产事实可展示；订单或现金流尚未完成对账，不能作为优化基线。", { exact: true })).toBeVisible();
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
    expect(guidance).toHaveTextContent("uv run --package nexus-engine nexus-fosun-local refresh");
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
