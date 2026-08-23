import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { engineFetch, sanitizeEngineResponse } from "../../../app/lib/engine/client";

const actor = { userId: "nexus-test-user", requestId: "0198f7a0-1111-7777-8111-111111111114" };
const portfolioId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701";
const snapshotId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702";
const runId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705";

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

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

  it("rejects every omitted safety discriminator that generated types require", async () => {
    const cases: Array<[unknown, string]> = [
      [portfolioPage(), "kind"],
      [portfolioPage(), "coverage_scope"],
      [snapshotEnvelope(), "coverage_scope"],
      [snapshotEnvelope(), "converted_view"],
      [snapshotEnvelope(), "symbol"],
      [snapshotEnvelope(), "read_only"],
    ];
    for (const [payload, field] of cases) {
      if ("items" in (payload as ReturnType<typeof portfolioPage>).data) {
        delete ((payload as ReturnType<typeof portfolioPage>).data.items[0] as Record<string, unknown>)[field];
        expect((await sanitizeEngineResponse(jsonResponse(payload), undefined, "/internal/v1/portfolios")).status).toBe(500);
      } else {
        const snapshot = (payload as ReturnType<typeof snapshotEnvelope>).data;
        const target = ["symbol", "read_only"].includes(field) ? snapshot.open_orders[0] : snapshot;
        delete (target as Record<string, unknown>)[field];
        expect((await sanitizeEngineResponse(jsonResponse(payload), undefined, `/internal/v1/portfolios/${portfolioId}/snapshots/latest`)).status).toBe(500);
      }
    }
  });

  it("accepts the maximum legal collection sizes and rejects limit plus one", async () => {
    const position = { instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703", symbol: null, exchange: "XNAS", currency: "USD", quantity: "1", available_quantity: "1", frozen_quantity: "0", avg_cost: null, diluted_cost: null, current_price: null, provider_as_of: null, read_only: true };
    const maximum = snapshotEnvelope() as unknown as { data: { positions: Array<typeof position>; open_orders: Array<Record<string, unknown>> } };
    maximum.data.positions = Array.from({ length: 2000 }, () => ({ ...position }));
    maximum.data.open_orders = Array.from({ length: 2000 }, () => ({ ...maximum.data.open_orders[0] }));

    expect((await sanitizeEngineResponse(jsonResponse(maximum), undefined, `/internal/v1/portfolios/${portfolioId}/snapshots/latest`)).status).toBe(200);
    maximum.data.open_orders.push({ ...maximum.data.open_orders[0] });
    expect((await sanitizeEngineResponse(jsonResponse(maximum), undefined, `/internal/v1/portfolios/${portfolioId}/snapshots/latest`)).status).toBe(500);
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

  it("rebuilds exactly one public Location through the real BFF sanitization chain", async () => {
    setEngineEnvironment();
    vi.stubGlobal("fetch", async () => jsonResponse(syncEnvelope(), 202, {
      location: "/internal/v1/untrusted-next",
      "x-provider-secret": "discard",
    }));
    const { POST } = await import("../../../app/api/v1/broker-sync-runs/route");
    const response = await POST(new Request("https://nexus.test/api/v1/broker-sync-runs", {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "content-type": "application/json",
        "idempotency-key": "sync-key",
      },
      body: JSON.stringify({ portfolio_id: portfolioId }),
    }));

    expect(response.status).toBe(202);
    expect(response.headers.get("location")).toBe(`/api/v1/broker-sync-runs/${runId}`);
    expect(response.headers.get("x-provider-secret")).toBeNull();
    expect((await response.json()).data.broker_sync_run_id).toBe(runId);
  });

  it("does not follow a cross-origin 307 with signed headers or exact POST bytes", async () => {
    let destinationCalls = 0;
    const destination = createServer((_request, response) => {
      destinationCalls += 1;
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(syncEnvelope()));
    });
    const destinationOrigin = await listen(destination);
    let originBody = "";
    let originSignature: string | undefined;
    const origin = createServer((request, response) => {
      originSignature = request.headers["x-nexus-signature"] as string | undefined;
      request.setEncoding("utf8");
      request.on("data", (chunk) => { originBody += chunk; });
      request.on("end", () => {
        response.writeHead(307, { location: `${destinationOrigin}/capture` });
        response.end();
      });
    });
    const originBaseUrl = await listen(origin);
    setEngineEnvironment();
    process.env.NEXUS_ENGINE_BASE_URL = originBaseUrl;
    const body = JSON.stringify({ portfolio_id: portfolioId });
    try {
      const response = await engineFetch("/internal/v1/broker-sync-runs", {
        method: "POST", actor, body,
        headers: { "content-type": "application/json", "idempotency-key": "sync-key" },
      });
      expect(response.status).toBe(500);
      expect(originSignature).toBeTruthy();
      expect(originBody).toBe(body);
      expect(destinationCalls).toBe(0);
    } finally {
      await close(origin);
      await close(destination);
    }
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
async function listen(server: Server): Promise<string> { await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve)); const address = server.address() as AddressInfo; return `http://127.0.0.1:${address.port}`; }
async function close(server: Server): Promise<void> { if (server.listening) await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve())); }
