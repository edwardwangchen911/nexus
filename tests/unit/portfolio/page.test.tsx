import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import PortfolioPage, {
  PortfolioPanel,
  type PortfolioItem,
  type PortfolioSnapshot,
} from "../../../app/(protected)/portfolio/page";
import { PortfolioSummary } from "../../../app/components/portfolio-summary";
import type { DataStateKind } from "../../../app/components/data-state";

const portfolioId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701";
const snapshotId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702";

const portfolio: PortfolioItem = {
  portfolio_id: portfolioId,
  name: "Fosun read-only",
  kind: "real_read_only",
  coverage_scope: "broker_account",
  provider: "fosun",
  latest_snapshot_status: "READY",
  latest_snapshot_as_of: "2026-08-23T08:00:00Z",
  currencies: ["HKD", "USD"],
  last_sync_status: "READY",
};

const snapshot: PortfolioSnapshot = {
  portfolio_snapshot_id: snapshotId,
  portfolio_id: portfolioId,
  coverage_scope: "broker_account",
  status: "READY",
  as_of: "2026-08-23T08:00:00Z",
  cash: [
    { currency: "HKD", cash_balance: "8000.00", available_cash: "7000.00", provider_as_of: "2026-08-23T08:00:00Z" },
    { currency: "USD", cash_balance: "1000.00", available_cash: "900.00", provider_as_of: "2026-08-23T08:00:00Z" },
  ],
  positions: [{
    instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703",
    symbol: "00656.HK",
    exchange: "XHKG",
    currency: "HKD",
    quantity: "3000",
    available_quantity: "2800",
    frozen_quantity: "200",
    avg_cost: "6.20",
    diluted_cost: "5.75",
    current_price: "6.50",
    provider_as_of: "2026-08-23T08:00:00Z",
    read_only: true,
  }],
  open_orders: [{
    broker_order_snapshot_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e704",
    instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703",
    symbol: "00656.HK",
    side: "BUY",
    quantity: "100",
    filled_quantity: "0",
    price: "6.10",
    status: "SUBMITTED",
    updated_at: "2026-08-23T07:59:00Z",
    read_only: true,
  }],
  converted_view: null,
};

const stateLabels: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"], ["empty", "暂无数据"], ["ready", "数据已就绪"],
  ["partial", "数据不完整"], ["stale", "数据已过期"], ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("portfolio vertical slice", () => {
  it.each(stateLabels)("renders the visible %s data state", (state, label) => {
    render(<PortfolioPanel
      state={state}
      portfolios={["ready", "partial", "stale"].includes(state) ? [portfolio] : []}
      snapshot={["ready", "partial", "stale"].includes(state) ? snapshot : null}
      selectedPortfolioId={portfolioId}
      syncing={false}
      notice={null}
      onSelect={vi.fn()}
      onRefresh={vi.fn()}
      onSync={vi.fn()}
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

  it("reconciles an unknown sync result with the same idempotency key", async () => {
    let syncCalls = 0;
    const keys: Array<string | null> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") {
        syncCalls += 1;
        keys.push(new Headers(init?.headers).get("idempotency-key"));
        if (syncCalls === 1) return Response.json({ error: { code: "INTERNAL_ERROR", message: "safe", request_id: responseMeta().request_id, details: {} } }, { status: 500 });
        return Response.json({ data: { broker_sync_run_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705", portfolio_id: portfolioId, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: "2026-08-23T08:01:00Z", completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null }, meta: responseMeta() }, { status: 202, headers: { location: "/api/v1/broker-sync-runs/018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705" } });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    await act(async () => { await Promise.resolve(); });

    expect(await screen.findByText("首次结果未知；已用同一幂等键确认同步任务。")).toBeVisible();
    expect(syncCalls).toBe(2);
    expect(keys[0]).toBeTruthy();
    expect(new Set(keys).size).toBe(1);
  });
});

function responseMeta() {
  return { request_id: "0198f7a0-1111-7777-8111-111111111114", schema_version: "1", as_of: "2026-08-23T08:00:00Z" };
}
