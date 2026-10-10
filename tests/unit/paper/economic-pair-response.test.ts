import { afterEach, describe, expect, it, vi } from "vitest";
import { rebuildAllowedEngineResponse } from "../../../app/lib/engine/errors";

import { engineFetch } from "../../../app/lib/engine/client";

afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs(); });

const pairId = "0198f7a0-1111-7777-8111-111111111110";
const first = "0198f7a0-1111-7777-8111-111111111112";
const second = "0198f7a0-1111-7777-8111-111111111113";
const path = `/internal/v1/paper/economic-pairs/${pairId}`;
function sample() {
  return {
    data: {
      pair_slot_id: pairId, evaluation_id: first, ordinal: 1, planned_slots: 24,
      decision_session: "2026-10-09", decision_at: "2026-10-09T14:00:00Z",
      execution_locked: true, economic_status: "not_evaluated",
      lanes: [
        { lane: "incumbent", cycle_id: first, cycle_status: "completed" },
        { lane: "challenger", cycle_id: second, cycle_status: "completed" },
      ],
      execution: { status: "completed", window_id: null, orders: [], comparison_status: "not_evaluated",
        lanes: [{ lane: "incumbent", status: "no_trade", order_count: 0 }, { lane: "challenger", status: "no_trade", order_count: 0 }] },
      gap_code: null,
    },
    meta: { request_id: first, schema_version: "1", as_of: "2026-10-09T14:10:00Z" },
  };
}
function read(value: unknown, method="GET") {
  return rebuildAllowedEngineResponse(200, value, new Headers(), `https://nexus.test${path}`, undefined, method);
}
describe("economic pair BFF persisted result contract", () => {
  it.each(["no_trade", "not_required", "risk_rejected", "filled"])("preserves a zero-active-order %s lane beside a filled lane", async (status) => {
    const value = sample();
    Object.assign(value.data.execution, { window_id: first, orders: [
      { lane: "challenger", order_id: second, action: "filled", status: "completed", code: "OK" },
    ] });
    Object.assign(value.data.execution.lanes[0], { status });
    Object.assign(value.data.execution.lanes[1], { status: "filled", order_count: 1 });
    if (status === "risk_rejected") Object.assign(value.data.execution, { status: "partial", comparison_status: "incomplete" });
    expect(await read(value)?.json()).toEqual(value);
  });
  it.each(["no_trade", "not_required", "risk_rejected"])("rejects an active order labeled %s in a real window", (status) => {
    const value = sample();
    Object.assign(value.data.execution, { window_id: first, orders: [
      { lane: "incumbent", order_id: first, action: "filled", status: "completed", code: "OK" },
      { lane: "challenger", order_id: second, action: "filled", status: "completed", code: "OK" },
    ] });
    Object.assign(value.data.execution.lanes[0], { status, order_count: 1 });
    Object.assign(value.data.execution.lanes[1], { status: "filled", order_count: 1 });
    expect(read(value)).toBeNull();
  });
  it("rejects case variants of the same cycle identity", () => {
    const value = sample();
    value.data.lanes[1].cycle_id = first.toUpperCase();
    expect(read(value)).toBeNull();
  });
  it("rejects case variants of the same order identity", () => {
    const value = sample();
    Object.assign(value.data.execution, { window_id: first, orders: [
      { lane: "incumbent", order_id: first, action: "filled", status: "completed", code: "OK" },
      { lane: "challenger", order_id: first.toUpperCase(), action: "filled", status: "completed", code: "OK" },
    ] });
    value.data.execution.lanes.forEach((lane) => Object.assign(lane, { status: "filled", order_count: 1 }));
    expect(read(value)).toBeNull();
  });
  it("preserves a qualified locked no-window no-trade result", async () => {
    const value = sample();
    expect(await read(value)?.json()).toEqual(value);
    expect(read(value, "POST")).toBeNull();
  });
  it.each(["wrong_pair", "unlocked", "denominator", "extra_account", "duplicate_lane", "duplicate_cycle", "missing_gap", "no_trade_order", "incomplete_cycle", "no_cycles_with_execution", "missing_lock", "missing_slots", "missing_economic"])("rejects %s", (mutation) => {
    const value = sample();
    switch (mutation) {
      case "missing_lock": Reflect.deleteProperty(value.data, "execution_locked"); break;
      case "missing_slots": Reflect.deleteProperty(value.data, "planned_slots"); break;
      case "missing_economic": Reflect.deleteProperty(value.data, "economic_status"); break;
      case "wrong_pair": value.data.pair_slot_id = second; break;
      case "unlocked": value.data.execution_locked = false; break;
      case "denominator": value.data.planned_slots = 23; break;
      case "extra_account": Object.assign(value.data, { account_id: "private" }); break;
      case "duplicate_lane": value.data.lanes[1].lane = "incumbent"; break;
      case "duplicate_cycle": value.data.lanes[1].cycle_id = first; break;
      case "missing_gap": Object.assign(value.data, { execution: null }); break;
      case "no_trade_order": Object.assign(value.data.execution, { orders: [{ lane: "incumbent", order_id: first, action: "filled", status: "completed", code: "OK" }] }); break;
      case "no_cycles_with_execution": Object.assign(value.data.lanes[0], { cycle_id: null }); Object.assign(value.data.lanes[1], { cycle_id: null }); break;
      case "incomplete_cycle": value.data.lanes[1].cycle_status = "unavailable"; break;
    }
    expect(read(value)).toBeNull();
  });
});

it("uses the normal Engine target registry, HMAC GET and response sanitizer", async () => {
  vi.stubEnv("NEXUS_ENGINE_BASE_URL", "http://127.0.0.1:8000");
  vi.stubEnv("NEXUS_ENGINE_HMAC_CURRENT_KEY_ID", "test-current");
  vi.stubEnv("NEXUS_ENGINE_HMAC_CURRENT_SECRET", "nexus-test-hmac-key-32-bytes!!!!");
  vi.stubEnv("NEXUS_ENV", "test");
  const fetch = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    expect(init?.method).toBe("GET");
    expect(new Headers(init?.headers).get("X-Nexus-Signature")).toBeTruthy();
    return Response.json(sample());
  });
  vi.stubGlobal("fetch", fetch);
  const actor = { userId: "allowed-user", requestId: first };
  const target = { template: "/internal/v1/paper/economic-pairs/{pair_slot_id}" as const, path };
  const response = await engineFetch(target, { method: "GET", actor });
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual(sample());
  expect(fetch).toHaveBeenCalledTimes(1);
  expect((await engineFetch({ ...target, path: path + "?account_id=foreign" }, { method: "GET", actor })).status).toBe(500);
  expect(fetch).toHaveBeenCalledTimes(1);
});
