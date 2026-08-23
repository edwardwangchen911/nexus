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
      syncing={false}
      unresolvedSync={false}
      notice={null}
      onSelect={vi.fn()}
      onRefresh={vi.fn()}
      onSync={vi.fn()}
      onResolveSync={vi.fn()}
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
      snapshot={orderOnlyCurrencySnapshot} selectedPortfolioId={portfolioId} syncing={false}
      unresolvedSync={false} notice={null} onSelect={vi.fn()} onRefresh={vi.fn()}
      onSync={vi.fn()} onResolveSync={vi.fn()} />);

    expect(screen.getByRole("region", { name: "Fosun read-only USD 组合摘要" })).toHaveTextContent("MSFT");
  });

  it("keeps the portfolio rail usable when the selected portfolio has no snapshot", () => {
    const replacement = { ...portfolio, portfolio_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799", name: "Replacement portfolio" };
    render(<PortfolioPanel state="error" portfolios={[portfolio, replacement]} snapshot={null}
      selectedPortfolioId={portfolioId} syncing={false} unresolvedSync={false} notice={null}
      onSelect={vi.fn()} onRefresh={vi.fn()} onSync={vi.fn()} onResolveSync={vi.fn()} />);

    expect(screen.getByRole("navigation", { name: "组合选择" })).toBeVisible();
    expect(screen.getByRole("button", { name: /Replacement portfolio/ })).toBeVisible();
    expect(screen.getAllByText("加载失败").length).toBeGreaterThan(0);
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

  it("keeps a twice-unknown sync locked to the original idempotency key", async () => {
    let syncCalls = 0;
    const keys: Array<string | null> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") {
        syncCalls += 1;
        keys.push(new Headers(init?.headers).get("idempotency-key"));
        if (syncCalls <= 2) return Response.json({ error: { code: "INTERNAL_ERROR", message: "safe", request_id: responseMeta().request_id, details: {} } }, { status: 500 });
        const runId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705";
        return Response.json({ data: { broker_sync_run_id: runId, portfolio_id: portfolioId, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: "2026-08-23T08:01:00Z", completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null }, meta: responseMeta() }, { status: 202, headers: { location: `/api/v1/broker-sync-runs/${runId}` } });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    const resolve = await screen.findByRole("button", { name: "使用同一幂等键继续确认" });
    expect(screen.queryByRole("button", { name: "同步复星只读事实" })).not.toBeInTheDocument();
    fireEvent.click(resolve);

    expect(await screen.findByText("复星只读同步任务已登记。当前快照不会被静默覆盖。")).toBeVisible();
    expect(syncCalls).toBe(3);
    expect(new Set(keys).size).toBe(1);
  });

  it("keeps timeout and fetch-rejection recovery on the original idempotency key", async () => {
    let syncCalls = 0;
    const keys: Array<string | null> = [];
    const runId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705";
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") {
        syncCalls += 1;
        keys.push(new Headers(init?.headers).get("idempotency-key"));
        if (syncCalls === 1) throw new DOMException("timed out", "AbortError");
        if (syncCalls === 2) throw new TypeError("network rejected");
        return Response.json({ data: { broker_sync_run_id: runId, portfolio_id: portfolioId, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: "2026-08-23T08:01:00Z", completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null }, meta: responseMeta() }, { status: 202, headers: { location: `/api/v1/broker-sync-runs/${runId}` } });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    fireEvent.click(await screen.findByRole("button", { name: "使用同一幂等键继续确认" }));

    expect(await screen.findByRole("heading", { name: "同步详情" })).toBeVisible();
    expect(syncCalls).toBe(3);
    expect(new Set(keys).size).toBe(1);
  });

  it("does not carry an unresolved portfolio A request into portfolio B", async () => {
    const replacement = { ...portfolio, portfolio_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799", name: "Replacement portfolio" };
    const replacementSnapshot = { ...snapshot, portfolio_id: replacement.portfolio_id };
    let syncCalls = 0;
    const bodies: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio, replacement], next_cursor: null }, meta: responseMeta() });
      if (url.includes(replacement.portfolio_id)) return Response.json({ data: replacementSnapshot, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") {
        syncCalls += 1; bodies.push(String(init?.body));
        if (syncCalls <= 2) throw new TypeError("unknown result");
        const runId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e709";
        return Response.json({ data: { broker_sync_run_id: runId, portfolio_id: replacement.portfolio_id, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: "2026-08-23T08:03:00Z", completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null }, meta: responseMeta() }, { status: 202, headers: { location: `/api/v1/broker-sync-runs/${runId}` } });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    expect(await screen.findByRole("button", { name: "使用同一幂等键继续确认" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: /Replacement portfolio/ }));
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));

    expect(await screen.findByRole("heading", { name: "同步详情" })).toBeVisible();
    expect(bodies.at(-1)).toBe(JSON.stringify({ portfolio_id: replacement.portfolio_id }));
  });

  it("retries an ACTIVE sync poll after a transient lookup failure", async () => {
    vi.useFakeTimers();
    const runId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705";
    const run = { broker_sync_run_id: runId, portfolio_id: portfolioId, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: "2026-08-23T08:01:00Z", completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null };
    let lookups = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") return Response.json({ data: run, meta: responseMeta() }, { status: 202, headers: { location: `/api/v1/broker-sync-runs/${runId}` } });
      if (url === `/api/v1/broker-sync-runs/${runId}`) {
        lookups += 1;
        return lookups === 1
          ? Response.json({ error: { code: "INTERNAL_ERROR" } }, { status: 500 })
          : Response.json({ data: { ...run, status: "READY", completed_at: "2026-08-23T08:02:00Z", attempt_count: 1, cash_flow_quality: "PASS" }, meta: responseMeta() });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "同步复星只读事实" }));
      await Promise.resolve();
    });
    fireEvent.click(screen.getByRole("button", { name: "刷新同步状态" }));
    await act(async () => { await Promise.resolve(); });
    expect(screen.getByText("无法刷新同步详情。冻结组合快照保持不变。")).toBeVisible();
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000); });

    expect(screen.getByText("PASS")).toBeVisible();
    expect(lookups).toBe(2);
    expect(screen.queryByText("无法刷新同步详情。冻结组合快照保持不变。")).not.toBeInTheDocument();
  });

  it("prevents double-clicked sync admissions and explains cooldown", async () => {
    let syncCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") {
        syncCalls += 1;
        return Response.json({ error: { code: "VALIDATION_ERROR", message: "safe", request_id: responseMeta().request_id, details: {} } }, { status: 429 });
      }
      throw new Error(`unexpected request: ${url}`);
    }));
    render(<PortfolioPage />);
    const sync = await screen.findByRole("button", { name: "同步复星只读事实" });
    fireEvent.click(sync);
    fireEvent.click(sync);

    expect(await screen.findByText("只读同步冷却中。当前快照保持不变。")).toBeVisible();
    expect(syncCalls).toBe(1);
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
    fireEvent.click(screen.getByRole("button", { name: "刷新快照" }));
    expect(await screen.findByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    await act(async () => { releaseOldList?.({ data: { items: [portfolio], next_cursor: null } }); await Promise.resolve(); });

    expect(screen.getByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "Fosun read-only HKD 组合摘要" })).not.toBeInTheDocument();
  });

  it("consumes the admitted sync Location and exposes terminal sync details", async () => {
    const runId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705";
    const run = { broker_sync_run_id: runId, portfolio_id: portfolioId, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: "2026-08-23T08:01:00Z", completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null };
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") return Response.json({ data: run, meta: responseMeta() }, { status: 202, headers: { location: `/api/v1/broker-sync-runs/${runId}` } });
      if (url === `/api/v1/broker-sync-runs/${runId}`) return Response.json({ data: { ...run, status: "READY", completed_at: "2026-08-23T08:02:00Z", attempt_count: 1, cash_flow_quality: "PASS" }, meta: responseMeta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    expect(await screen.findByRole("heading", { name: "同步详情" })).toBeVisible();
    expect(screen.getByText("ACTIVE")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "刷新同步状态" }));

    expect(await screen.findByText("READY")).toBeVisible();
    expect(screen.getByText("PASS")).toBeVisible();
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
    fireEvent.click(screen.getByRole("button", { name: "刷新快照" }));

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
    fireEvent.click(screen.getByRole("button", { name: "刷新快照" }));
    expect(await screen.findByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    await act(async () => { releaseOldBody?.({ data: snapshot }); await Promise.resolve(); });

    expect(screen.getByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "Fosun read-only HKD 组合摘要" })).not.toBeInTheDocument();
  });

  it("uses one atomic polite live region for async portfolio outcomes", () => {
    render(<PortfolioPanel state="ready" portfolios={[portfolio]} snapshot={snapshot}
      selectedPortfolioId={portfolioId} syncing={false} unresolvedSync={false}
      notice={{ kind: "success", text: "同步任务已登记。" }} onSelect={vi.fn()}
      onRefresh={vi.fn()} onSync={vi.fn()} onResolveSync={vi.fn()} />);
    const live = screen.getByRole("status", { name: "组合操作状态" });
    expect(live).toHaveAttribute("aria-live", "polite");
    expect(live).toHaveAttribute("aria-atomic", "true");
    expect(screen.getAllByRole("status")).toHaveLength(1);
  });
});
