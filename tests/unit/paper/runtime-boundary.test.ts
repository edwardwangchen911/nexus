import { afterEach, describe, expect, it, vi } from "vitest";

import { engineFetch, sanitizeEngineResponse } from "../../../app/lib/engine/client";

const requestId = "0198f7a0-1111-7777-8111-111111111111";
const portfolioId = "0198f7a0-1111-7777-8111-111111111112";
const orderId = "0198f7a0-1111-7777-8111-111111111113";
const instrumentId = "0198f7a0-1111-7777-8111-111111111114";
const meta = { request_id: requestId, schema_version: "1", as_of: "2026-08-26T20:00:00Z" };
const actor = { userId: "allowed-user", requestId };
const cycleSummary = {
  decision_session: "2026-08-26", status: "in_progress", cycle_count: 1, completed_count: 0,
  latest_result_code: "PAPER_ORDERS_AWAITING_MARKET", last_completed_decision_at: null,
  next_eligible_at: null, next_eligible_reason: "CURRENT_CYCLE_IN_PROGRESS", execution_locked: true,
};

afterEach(() => {
  vi.unstubAllGlobals();
  delete process.env.NEXUS_ENGINE_BASE_URL;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  delete process.env.NEXUS_ENV;
});

describe("internal-paper Engine response boundary", () => {
  it("accepts only a bounded locked Agent cycle", async () => {
    const data = {
      cycle_id: portfolioId, status: "awaiting_market", result_code: "PAPER_ORDERS_AWAITING_MARKET",
      decision_session: "2026-08-26", decision_time: "2026-08-26T21:00:00Z",
      decision_slot_at: "2026-08-26T21:00:00Z",
      execution_session: "2026-08-27", execution_locked: true, candidate_count: 1,
      candidates: [{ instrument_id: instrumentId, symbol: "AAPL", selection_reasons: ["quality_rank"], research_status: "completed", stance: "hold", sizing_action: "maintain", intent_hash: "a".repeat(64), output_id: orderId, rejection_code: null }],
      decision: { decision_id: requestId, sizing_policy_version: "agent_paper_sizing_v1", target_cash_weight: "1.000000000000", risk_outcomes: [] },
      decision_market: null,
      execution_market: { source: "alpaca", feed: "iex", delivery_mode: "real_time", window_minutes: 5, status: "pending" },
      order_ids: [],
    };
    const path = "/internal/v1/paper/cycles/latest";
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "GET")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { ...data, execution_locked: false }, meta }), undefined, path, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { ...data, candidate_count: 2 }, meta }), undefined, path, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { ...data, execution_market: { ...data.execution_market, source: "schwab" } }, meta }), undefined, path, "GET")).status).toBe(500);

    const listPath = "/internal/v1/paper/cycles";
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary }, meta }), undefined, listPath, "GET")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: { ...cycleSummary, cycle_count: 0 } }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: Array.from({ length: 21 }, () => data), summary: cycleSummary }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [{ ...data, execution_locked: false }], summary: cycleSummary }, meta }), undefined, listPath, "GET")).status).toBe(500);
  });
  it("accepts the strict portfolio projection and rejects nested extras", async () => {
    const data = portfolio();
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, "/internal/v1/paper/portfolio")).status).toBe(200);

    const leaked = structuredClone(data);
    Object.assign(leaked.positions[0], { account_id: "must-not-escape" });
    expect((await sanitizeEngineResponse(json({ data: leaked, meta }), undefined, "/internal/v1/paper/portfolio")).status).toBe(500);

    const inconsistentCash = structuredClone(data);
    inconsistentCash.performance.reserved_cash = "501.000000000000";
    expect((await sanitizeEngineResponse(json({ data: inconsistentCash, meta }), undefined, "/internal/v1/paper/portfolio")).status).toBe(500);
  });

  it("uses the bounded paper response budget for a legal projection above the default cap", async () => {
    const data = portfolio();
    data.attributions = Array.from({ length: 500 }, (_, index) => ({
      ...data.attributions[0],
      dimension_key: `${String(index).padStart(3, "0")}-${"a".repeat(96)}`,
    }));
    const envelope = { data, meta };
    expect(JSON.stringify(envelope).length).toBeGreaterThan(64 * 1024);
    expect((await sanitizeEngineResponse(json(envelope), undefined, "/internal/v1/paper/portfolio")).status).toBe(200);
  });

  it("accepts bounded orders and rejects forged cancellation authority", async () => {
    const data = { items: [order()], next_cursor: "signed-cursor" };
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(200);

    const leaked = structuredClone(data);
    Object.assign(leaked.items[0], { account_id: "must-not-escape" });
    expect((await sanitizeEngineResponse(json({ data: leaked, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);

    const forged = structuredClone(data);
    Object.assign(forged.items[0], { cancellable: false, status: "submitted" });
    expect((await sanitizeEngineResponse(json({ data: forged, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);

    const inconsistent = structuredClone(data);
    inconsistent.items[0].remaining_quantity = "4.000000000000";
    expect((await sanitizeEngineResponse(json({ data: inconsistent, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);

    const brokenTimeline = structuredClone(data);
    brokenTimeline.items[0].events[2].from_status = "proposed";
    expect((await sanitizeEngineResponse(json({ data: brokenTimeline, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);
  });

  it("binds cancellation to method and requested order identity", async () => {
    const data = {
      order_id: orderId,
      status: "cancelled",
      filled_quantity: "2.000000000000",
      remaining_quantity: "3.000000000000",
      result_event_seq: 8,
      replayed: false,
    };
    const path = `/internal/v1/paper/orders/${orderId}/cancel`;
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "POST")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { ...data, order_id: portfolioId }, meta }), undefined, path, "POST")).status).toBe(500);
  });

  it("caps order collections independently of schema-shaped content", async () => {
    const data = { items: Array.from({ length: 51 }, order), next_cursor: null };
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);
  });

  it("allows only canonical paper targets and rebuilds protected headers", async () => {
    setEngineEnvironment();
    const calls: Array<{ url: string; init: RequestInit }> = [];
    vi.stubGlobal("fetch", async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: input.toString(), init: init ?? {} });
      if (input.toString().includes("/paper/cycles")) {
        return json({ data: { items: [], summary: { ...cycleSummary, status: "ready", cycle_count: 0, completed_count: 0, latest_result_code: null, next_eligible_at: "2026-08-26T21:00:00Z", next_eligible_reason: "CURRENT_SLOT" } }, meta });
      }
      return json({ data: { items: [order()], next_cursor: null }, meta });
    });

    const response = await engineFetch({
      template: "/internal/v1/paper/orders",
      path: "/internal/v1/paper/orders",
      query: "status=submitted&limit=25",
    }, {
      method: "GET",
      actor,
      headers: { authorization: "must-not-survive", "x-nexus-signature": "forged" },
    });
    expect(response.status).toBe(200);
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe("http://127.0.0.1:8000/internal/v1/paper/orders?limit=25&status=submitted");
    const headers = new Headers(calls[0].init.headers);
    expect(headers.get("x-nexus-signature")).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(headers.get("x-nexus-actor-id")).toBe(actor.userId);
    expect(headers.get("authorization")).toBeNull();

    calls.length = 0;
    const cycles = await engineFetch({
      template: "/internal/v1/paper/cycles",
      path: "/internal/v1/paper/cycles",
      query: "decision_session=2026-09-14&limit=20",
    }, { method: "GET", actor });
    expect(cycles.status).toBe(200);
    expect(calls[0].url).toBe("http://127.0.0.1:8000/internal/v1/paper/cycles?decision_session=2026-09-14&limit=20");

    calls.length = 0;
    const unsafeTargets = [
      {
        template: "/internal/v1/paper/orders",
        path: "/internal/v1/paper/orders",
        query: "account_id=secret",
      },
      {
        template: "/internal/v1/paper/orders/{order_id}/cancel",
        path: "/internal/v1/paper/orders/../../system-status/cancel",
      },
    ];
    const unsafeFetch = engineFetch as unknown as (
      target: unknown,
      init: { method: string; actor: typeof actor },
    ) => Promise<Response>;
    for (const target of unsafeTargets) {
      expect((await unsafeFetch(target, { method: "GET", actor })).status).toBe(500);
    }
    expect(calls).toHaveLength(0);
  });
});

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { status: 200, headers: { "content-type": "application/json" } });
}

function portfolio() {
  return {
    portfolio_id: portfolioId,
    name: "NEXUS Internal Paper",
    mode: "internal_paper" as const,
    currency: "USD" as const,
    initial_capital: "100000.000000000000",
    data_state: "READY" as const,
    performance: {
      as_of: "2026-08-26T20:00:00Z",
      cash: "90000.000000000000",
      available_cash: "89500.000000000000",
      reserved_cash: "500.000000000000",
      market_value: "10500.000000000000",
      nav: "100500.000000000000",
      peak_nav: "101000.000000000000",
      realized_pnl: "100.000000000000",
      unrealized_pnl: "400.000000000000",
      fees: "2.000000000000",
      drawdown_ratio: "0.004950495050",
    },
    positions: [{
      instrument_id: instrumentId,
      symbol: "AAPL",
      quantity: "50.000000000000",
      reserved_quantity: "5.000000000000",
      available_quantity: "45.000000000000",
      cost_basis: "10100.000000000000",
      average_cost: "202.000000000000",
      reference_close: "210.000000000000",
      market_value: "10500.000000000000",
      unrealized_pnl: "400.000000000000",
      price_source: "alpaca",
      price_feed: "iex",
      delivery_mode: "delayed" as const,
      price_as_of: "2026-08-26T20:00:00Z",
    }],
    attributions: [{
      dimension: "instrument" as const,
      dimension_key: instrumentId,
      market_value: "10500.000000000000",
      realized_pnl: "100.000000000000",
      unrealized_pnl: "400.000000000000",
      fees: "2.000000000000",
      slippage: "5.000000000000",
      completeness: "FULL" as const,
    }],
  };
}

function order() {
  return {
    order_id: orderId,
    instrument_id: instrumentId,
    symbol: "AAPL",
    side: "buy" as const,
    quantity: "5.000000000000",
    filled_quantity: "2.000000000000",
    remaining_quantity: "3.000000000000",
    worst_price: "220.000000000000",
    status: "partially_filled" as const,
    cancellable: true,
    current_event_seq: 4,
    submitted_at: "2026-08-26T19:30:00Z",
    strategy_version: "baseline_v1",
    risk_policy_version: "paper_risk_v1",
    events: [
      {
        event_seq: 1,
        from_status: null,
        to_status: "proposed" as const,
        actor_kind: "task18" as const,
        reason_code: "paper_order_proposed",
        occurred_at: "2026-08-26T19:27:00Z",
      },
      {
        event_seq: 2,
        from_status: "proposed" as const,
        to_status: "risk_approved" as const,
        actor_kind: "task18" as const,
        reason_code: "paper_risk_approved",
        occurred_at: "2026-08-26T19:28:00Z",
      },
      {
        event_seq: 3,
        from_status: "risk_approved" as const,
        to_status: "submitted" as const,
        actor_kind: "task19" as const,
        reason_code: "paper_order_submitted",
        occurred_at: "2026-08-26T19:30:00Z",
      },
      {
        event_seq: 4,
        from_status: "submitted" as const,
        to_status: "partially_filled" as const,
        actor_kind: "task19" as const,
        reason_code: "paper_partial_fill",
        occurred_at: "2026-08-26T20:00:00Z",
      },
    ],
    fills: [{
      fill_id: "0198f7a0-1111-7777-8111-111111111115",
      fill_seq: 1,
      quantity: "2.000000000000",
      price: "210.000000000000",
      notional: "420.000000000000",
      fee: "1.000000000000",
      occurred_at: "2026-08-26T20:00:00Z",
      source: "alpaca",
      feed: "iex",
      delivery_mode: "delayed" as const,
      source_as_of: "2026-08-26T19:59:00Z",
      execution_model_hash: "a".repeat(64),
      slippage_bps: 10 as const,
      slippage: "0.420000000000",
    }],
  };
}

function setEngineEnvironment(): void {
  process.env.NEXUS_ENGINE_BASE_URL = "http://127.0.0.1:8000";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID = "test-current";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET = "nexus-test-hmac-key-32-bytes!!!!";
  process.env.NEXUS_ENV = "test";
}
