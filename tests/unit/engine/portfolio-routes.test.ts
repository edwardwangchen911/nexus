import { beforeEach, describe, expect, it, vi } from "vitest";

const actor = { userId: "allowed-user", requestId: "0198f7a0-1111-7777-8111-111111111111" };
const engineFetch = vi.hoisted(() => vi.fn());
const portfolioId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701";
const runId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705";

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

vi.mock("../../../app/lib/engine/client", () => ({
  engineFetch,
  sanitizeEngineResponse: vi.fn(async (response: Response) => response),
}));

describe("portfolio BFF routes", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockImplementation(async () => Response.json({
      data: { broker_sync_run_id: runId },
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-23T08:00:00Z" },
    }));
  });

  it("forwards one canonical bounded portfolio query", async () => {
    const { GET } = await import("../../../app/api/v1/portfolios/route");
    const response = await GET(new Request(
      "https://nexus.test/api/v1/portfolios?limit=50&kind=real_read_only&cursor=opaque%2Bcursor",
    ));

    expect(response.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/portfolios",
      path: "/internal/v1/portfolios",
      query: "cursor=opaque%2Bcursor&kind=real_read_only&limit=50",
    }, { method: "GET", actor });
  });

  it("rejects unknown, duplicate, or unsupported portfolio filters", async () => {
    const { GET } = await import("../../../app/api/v1/portfolios/route");
    const responses = await Promise.all([
      GET(new Request("https://nexus.test/api/v1/portfolios?redirect=https://attacker.invalid")),
      GET(new Request("https://nexus.test/api/v1/portfolios?limit=10&limit=20")),
      GET(new Request("https://nexus.test/api/v1/portfolios?kind=broker_write")),
    ]);

    expect(responses.map((response) => response.status)).toEqual([400, 400, 400]);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("validates and forwards a latest snapshot path", async () => {
    const route = await import("../../../app/api/v1/portfolios/[portfolioId]/snapshots/latest/route");
    await route.GET(new Request(`https://nexus.test/api/v1/portfolios/${portfolioId}/snapshots/latest`));

    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/portfolios/{portfolio_id}/snapshots/latest",
      path: `/internal/v1/portfolios/${portfolioId}/snapshots/latest`,
    }, { method: "GET", actor });

    engineFetch.mockClear();
    const invalid = await route.GET(new Request("https://nexus.test/api/v1/portfolios/not-a-uuid/snapshots/latest"));
    expect(invalid.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("forwards exact sync bytes and only the idempotency key", async () => {
    const { POST } = await import("../../../app/api/v1/broker-sync-runs/route");
    const body = JSON.stringify({ portfolio_id: portfolioId });
    engineFetch.mockResolvedValue(Response.json({
      data: { broker_sync_run_id: runId },
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-23T08:00:00Z" },
    }, { status: 202 }));
    const response = await POST(new Request("https://nexus.test/api/v1/broker-sync-runs", {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "content-type": "application/json",
        "idempotency-key": "sync-key-1",
        authorization: "must-not-forward",
      },
      body,
    }));

    expect(engineFetch).toHaveBeenCalledTimes(1);
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toBe("/internal/v1/broker-sync-runs");
    expect(init).toMatchObject({ method: "POST", actor });
    expect(new TextDecoder().decode(init.body)).toBe(body);
    expect(new Headers(init.headers)).toEqual(new Headers({
      "content-type": "application/json",
      "idempotency-key": "sync-key-1",
    }));
    expect(response.status).toBe(202);
    expect(response.headers.get("location")).toBe(`/api/v1/broker-sync-runs/${runId}`);
  });

  it("rejects missing idempotency, cross-origin writes, and malformed bodies", async () => {
    const { POST } = await import("../../../app/api/v1/broker-sync-runs/route");
    const requests = [
      new Request("https://nexus.test/api/v1/broker-sync-runs", { method: "POST", headers: { origin: "https://nexus.test" }, body: "{}" }),
      new Request("https://nexus.test/api/v1/broker-sync-runs", { method: "POST", headers: { origin: "https://evil.test", "idempotency-key": "x" }, body: "{}" }),
      new Request("https://nexus.test/api/v1/broker-sync-runs", { method: "POST", headers: { origin: "https://nexus.test", "idempotency-key": "x", "content-type": "application/json" }, body: JSON.stringify({ portfolio_id: "not-a-uuid" }) }),
    ];
    const responses = await Promise.all(requests.map((request) => POST(request)));

    expect(responses.map((response) => response.status)).toEqual([400, 403, 400]);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("validates and forwards a sync-run lookup path", async () => {
    const route = await import("../../../app/api/v1/broker-sync-runs/[runId]/route");
    await route.GET(new Request(`https://nexus.test/api/v1/broker-sync-runs/${runId}`));
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/broker-sync-runs/{run_id}",
      path: `/internal/v1/broker-sync-runs/${runId}`,
    }, { method: "GET", actor });

    engineFetch.mockClear();
    const invalid = await route.GET(new Request("https://nexus.test/api/v1/broker-sync-runs/not-a-uuid"));
    expect(invalid.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });
});
