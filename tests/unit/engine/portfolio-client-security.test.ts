import { afterEach, describe, expect, it, vi } from "vitest";
import { engineFetch, sanitizeEngineResponse } from "../../../app/lib/engine/client";

const actor = { userId: "nexus-test-user", requestId: "0198f7a0-1111-7777-8111-111111111114" };
const portfolioId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701";
const snapshotId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702";
const runId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  for (const key of ["NEXUS_ENGINE_BASE_URL", "NEXUS_ENGINE_HMAC_CURRENT_KEY_ID", "NEXUS_ENGINE_HMAC_CURRENT_SECRET", "NEXUS_ENV"]) delete process.env[key];
});

describe("portfolio engine client boundary", () => {
  it("signs only a canonical bounded portfolio query", async () => {
    setEngineEnvironment();
    let sent = "";
    vi.stubGlobal("fetch", async (input: RequestInfo | URL) => {
      sent = String(input);
      return jsonResponse(portfolioPage());
    });

    const response = await engineFetch({
      template: "/internal/v1/portfolios",
      path: "/internal/v1/portfolios",
      query: "limit=50&kind=real_read_only&cursor=opaque%2Bcursor",
    }, { method: "GET", actor });

    expect(response.status).toBe(200);
    expect(sent).toBe("http://127.0.0.1:8000/internal/v1/portfolios?cursor=opaque%2Bcursor&kind=real_read_only&limit=50");
  });

  it("rejects unauthorized portfolio queries and dynamic paths before fetch", async () => {
    setEngineEnvironment();
    const fetchSpy = vi.fn(async () => jsonResponse(portfolioPage()));
    vi.stubGlobal("fetch", fetchSpy);
    const unsafeTargets = [
      { template: "/internal/v1/portfolios", path: "/internal/v1/portfolios", query: "limit=51" },
      { template: "/internal/v1/portfolios", path: "/internal/v1/portfolios", query: "redirect=https%3A%2F%2Fevil.test" },
      { template: "/internal/v1/portfolios/{portfolio_id}/snapshots/latest", path: "/internal/v1/portfolios/not-a-uuid/snapshots/latest" },
      { template: "/internal/v1/broker-sync-runs/{run_id}", path: "/internal/v1/broker-sync-runs/../system-status" },
    ];
    for (const target of unsafeTargets) {
      const response = await engineFetch(target as never, { method: "GET", actor } as never);
      expect(response.status).toBe(500);
    }
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("accepts the closed snapshot schema and rejects nested or top-level secret fields", async () => {
    const payload = snapshotEnvelope();
    const accepted = await sanitizeEngineResponse(jsonResponse(payload), undefined, `/internal/v1/portfolios/${portfolioId}/snapshots/latest`);
    expect(accepted.status).toBe(200);

    const nestedSecret = snapshotEnvelope();
    Object.assign(nestedSecret.data.open_orders[0], { provider_order_id: "secret" });
    expect((await sanitizeEngineResponse(jsonResponse(nestedSecret), undefined, `/internal/v1/portfolios/${portfolioId}/snapshots/latest`)).status).toBe(500);

    const topLevelSecret = snapshotEnvelope();
    Object.assign(topLevelSecret.data, { account_id: "secret" });
    expect((await sanitizeEngineResponse(jsonResponse(topLevelSecret), undefined, `/internal/v1/portfolios/${portfolioId}/snapshots/latest`)).status).toBe(500);
  });

  it("drops engine Location so only the validated BFF may rebuild it", async () => {
    setEngineEnvironment();
    vi.stubGlobal("fetch", async () => jsonResponse(syncEnvelope(), 202, {
      location: `/api/v1/broker-sync-runs/${runId}`,
      "x-provider-secret": "discard",
    }));
    const body = JSON.stringify({ portfolio_id: portfolioId });

    const response = await engineFetch("/internal/v1/broker-sync-runs", {
      method: "POST", actor, body,
      headers: { "content-type": "application/json", "idempotency-key": "sync-key" },
    });

    expect(response.status).toBe(202);
    expect(response.headers.get("location")).toBeNull();
    expect(response.headers.get("x-provider-secret")).toBeNull();
  });

  it("passes a sanitized cooldown while discarding upstream details", async () => {
    const response = await sanitizeEngineResponse(jsonResponse({ error: {
      code: "VALIDATION_ERROR", message: "provider cooldown secret", request_id: actor.requestId,
      details: { cooldown_seconds: 60, provider: "secret" },
    } }, 429), "https://nexus.test/api/v1/broker-sync-runs");

    expect(response.status).toBe(429);
    expect(await response.json()).toEqual({ error: {
      code: "VALIDATION_ERROR", message: "The request is invalid.", request_id: actor.requestId, details: {},
    } });
  });
});

function portfolioPage() {
  return { data: { items: [{ portfolio_id: portfolioId, name: "Fosun read-only", kind: "real_read_only", coverage_scope: "broker_account", provider: "fosun", latest_snapshot_status: "READY", latest_snapshot_as_of: "2026-08-23T08:00:00Z", original_currencies: ["HKD"], last_sync_run_status: "READY" }], next_cursor: null }, meta: meta() };
}

function snapshotEnvelope() {
  return { data: { portfolio_snapshot_id: snapshotId, portfolio_id: portfolioId, coverage_scope: "broker_account", status: "READY", cash: [{ currency: "HKD", cash_balance: "8000.00", available_cash: "7000.00", provider_as_of: "2026-08-23T08:00:00Z" }], positions: [], open_orders: [{ broker_order_snapshot_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e704", instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703", symbol: "00656.HK", currency: "HKD", side: "BUY", quantity: "100", filled_quantity: "0", price: "6.10", status: "SUBMITTED", updated_at: "2026-08-23T07:59:00Z", read_only: true }], converted_view: null, as_of: "2026-08-23T08:00:00Z" }, meta: meta() };
}

function syncEnvelope() {
  return { data: { broker_sync_run_id: runId, portfolio_id: portfolioId, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: "2026-08-23T08:01:00Z", completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null }, meta: meta() };
}

function meta() { return { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-23T08:00:00Z" }; }
function jsonResponse(value: unknown, status = 200, headers: HeadersInit = {}): Response { return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json", ...headers } }); }
function setEngineEnvironment(): void { process.env.NEXUS_ENGINE_BASE_URL = "http://127.0.0.1:8000"; process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID = "test-current"; process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET = "nexus-test-hmac-key-32-bytes!!!!"; process.env.NEXUS_ENV = "test"; }
