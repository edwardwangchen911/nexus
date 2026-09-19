import { beforeEach, describe, expect, it, vi } from "vitest";

const actor = { userId: "allowed-user", requestId: "0198f7a0-1111-7777-8111-111111111111" };
const cycleId = "0198f7a0-1111-7777-8111-111111111110";
const orderId = "0198f7a0-1111-7777-8111-111111111112";
const planId = "0198f7a0-1111-7777-8111-111111111113";
const engineFetch = vi.hoisted(() => vi.fn());

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

vi.mock("../../../app/lib/engine/client", () => ({
  engineFetch,
  sanitizeEngineResponse: vi.fn(async (response: Response) => response),
}));

describe("internal-paper BFF routes", () => {
  it("forwards the latest cycle as a query-free GET", async () => {
    const { GET } = await import("../../../app/api/v1/paper/cycles/latest/route");
    await GET(new Request("https://nexus.test/api/v1/paper/cycles/latest"));
    expect(engineFetch).toHaveBeenCalledWith(
      "/internal/v1/paper/cycles/latest",
      expect.objectContaining({ method: "GET" }),
    );
    const rejected = await GET(
      new Request("https://nexus.test/api/v1/paper/cycles/latest?account_id=secret"),
    );
    expect(rejected.status).toBe(400);
  });

  it("keeps the legacy cycle detail route on the legacy Engine contract", async () => {
    const { GET } = await import("../../../app/api/v1/paper/cycles/[cycleId]/route");
    await GET(new Request(`https://nexus.test/api/v1/paper/cycles/${cycleId}`));

    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/paper/cycles/{cycle_id}",
      path: `/internal/v1/paper/cycles/${cycleId}`,
    }, { method: "GET", actor });

    engineFetch.mockClear();
    const malformed = await GET(new Request("https://nexus.test/api/v1/paper/cycles/not-a-uuid"));
    expect(malformed.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();

    const queried = await GET(new Request(`https://nexus.test/api/v1/paper/cycles/${cycleId}?account_id=secret`));
    expect(queried.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("forwards the additive management cycle route without query parameters", async () => {
    const { GET } = await import("../../../app/api/v1/paper/management/cycles/[cycleId]/route");
    await GET(new Request(`https://nexus.test/api/v1/paper/management/cycles/${cycleId}`));

    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/paper/management/cycles/{cycle_id}",
      path: `/internal/v1/paper/management/cycles/${cycleId}`,
    }, { method: "GET", actor });

    engineFetch.mockClear();
    const malformed = await GET(new Request("https://nexus.test/api/v1/paper/management/cycles/not-a-uuid"));
    expect(malformed.status).toBe(400);
    const queried = await GET(new Request(`https://nexus.test/api/v1/paper/management/cycles/${cycleId}?account_id=secret`));
    expect(queried.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("canonicalizes only the bounded Agent cycle list filters", async () => {
    const { GET } = await import("../../../app/api/v1/paper/cycles/route");
    await GET(new Request("https://nexus.test/api/v1/paper/cycles?limit=20&decision_session=2026-09-14"));

    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/paper/cycles",
      path: "/internal/v1/paper/cycles",
      query: "decision_session=2026-09-14&limit=20",
    }, { method: "GET", actor });
  });

  it.each([
    "limit=0",
    "limit=21",
    "decision_session=2026-02-30",
    "decision_session=2026-09-14&decision_session=2026-09-15",
    "account_id=secret",
  ])("rejects invalid Agent cycle list filters before Engine: %s", async (query) => {
    const { GET } = await import("../../../app/api/v1/paper/cycles/route");
    const response = await GET(new Request(`https://nexus.test/api/v1/paper/cycles?${query}`));
    expect(response.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json({
      data: {},
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-26T00:00:00Z" },
    }));
  });

  it("forwards the portfolio read without query or client headers", async () => {
    const { GET } = await import("../../../app/api/v1/paper/portfolio/route");
    await GET(new Request("https://nexus.test/api/v1/paper/portfolio"));

    expect(engineFetch).toHaveBeenCalledWith(
      "/internal/v1/paper/portfolio",
      { method: "GET", actor },
    );

    engineFetch.mockClear();
    const rejected = await GET(new Request("https://nexus.test/api/v1/paper/portfolio?account_id=secret"));
    expect(rejected.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("forwards the management summary as one query-free GET", async () => {
    const { GET } = await import("../../../app/api/v1/paper/management/portfolio/route");
    await GET(new Request("https://nexus.test/api/v1/paper/management/portfolio"));

    expect(engineFetch).toHaveBeenCalledWith(
      "/internal/v1/paper/management/portfolio",
      { method: "GET", actor },
    );

    engineFetch.mockClear();
    const rejected = await GET(new Request("https://nexus.test/api/v1/paper/management/portfolio?account_id=secret"));
    expect(rejected.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("forwards one exact plan read and never calls a mutation", async () => {
    const { GET } = await import("../../../app/api/v1/paper/plans/[planId]/route");
    await GET(new Request(`https://nexus.test/api/v1/paper/plans/${planId}`));
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/paper/plans/{batch_id}",
      path: `/internal/v1/paper/plans/${planId}`,
    }, { method: "GET", actor });
    expect(engineFetch).not.toHaveBeenCalledWith(
      expect.objectContaining({ template: expect.stringContaining("cancel") }),
      expect.objectContaining({ method: "POST" }),
    );

    engineFetch.mockClear();
    for (const url of [
      "https://nexus.test/api/v1/paper/plans/not-a-uuid",
      `https://nexus.test/api/v1/paper/plans/${planId}?account_id=secret`,
    ]) {
      expect((await GET(new Request(url))).status).toBe(400);
    }
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("reads only the existing cancellation attempt and never retries cancellation", async () => {
    const { GET } = await import("../../../app/api/v1/paper/orders/[orderId]/cancel-result/route");
    await GET(new Request(`https://nexus.test/api/v1/paper/orders/${orderId}/cancel-result`, {
      headers: { "idempotency-key": "cancel-paper-19", authorization: "must-not-forward" },
    }));
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/paper/orders/{order_id}/cancel-result",
      path: `/internal/v1/paper/orders/${orderId}/cancel-result`,
    }, {
      method: "GET",
      actor,
      headers: { "idempotency-key": "cancel-paper-19" },
    });
    expect(engineFetch).not.toHaveBeenCalledWith(
      expect.objectContaining({ template: "/internal/v1/paper/orders/{order_id}/cancel" }),
      expect.anything(),
    );

    engineFetch.mockClear();
    for (const request of [
      new Request("https://nexus.test/api/v1/paper/orders/not-a-uuid/cancel-result", {
        headers: { "idempotency-key": "cancel-paper-19" },
      }),
      new Request(`https://nexus.test/api/v1/paper/orders/${orderId}/cancel-result?retry=true`, {
        headers: { "idempotency-key": "cancel-paper-19" },
      }),
      new Request(`https://nexus.test/api/v1/paper/orders/${orderId}/cancel-result`),
    ]) {
      expect((await GET(request)).status).toBe(400);
    }
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("canonicalizes the closed order filters", async () => {
    const { GET } = await import("../../../app/api/v1/paper/orders/route");
    await GET(new Request("https://nexus.test/api/v1/paper/orders?status=partially_filled&limit=25&cursor=cursor%2Bvalue"));

    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/paper/orders",
      path: "/internal/v1/paper/orders",
      query: "cursor=cursor%2Bvalue&limit=25&status=partially_filled",
    }, { method: "GET", actor });
  });

  it.each([
    "status=unknown",
    "limit=0",
    "limit=51",
    "limit=1&limit=2",
    "cursor=",
    "account_id=0198f7a0-1111-7777-8111-111111111113",
  ])("rejects invalid order filters before Engine: %s", async (query) => {
    const { GET } = await import("../../../app/api/v1/paper/orders/route");
    const response = await GET(new Request(`https://nexus.test/api/v1/paper/orders?${query}`));
    expect(response.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("forwards only a canonical versioned cancellation", async () => {
    const { POST } = await import("../../../app/api/v1/paper/orders/[orderId]/cancel/route");
    const response = await POST(new Request(`https://nexus.test/api/v1/paper/orders/${orderId}/cancel`, {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "content-type": "application/json; charset=utf-8",
        "idempotency-key": "cancel-paper-19",
        authorization: "must-not-forward",
      },
      body: "{\n  \"expected_event_seq\": 7\n}",
    }));

    expect(response.status).toBe(200);
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toEqual({
      template: "/internal/v1/paper/orders/{order_id}/cancel",
      path: `/internal/v1/paper/orders/${orderId}/cancel`,
    });
    expect(init).toMatchObject({
      method: "POST",
      actor,
      headers: {
        "content-type": "application/json",
        "idempotency-key": "cancel-paper-19",
      },
    });
    expect(new TextDecoder().decode(init.body)).toBe("{\"expected_event_seq\":7}");
  });

  it.each([
    { path: "not-a-uuid", body: { expected_event_seq: 1 }, key: "cancel-1", type: "application/json" },
    { path: orderId, body: { expected_event_seq: 0 }, key: "cancel-1", type: "application/json" },
    { path: orderId, body: { expected_event_seq: 1.5 }, key: "cancel-1", type: "application/json" },
    { path: orderId, body: { expected_event_seq: 1, force: true }, key: "cancel-1", type: "application/json" },
    { path: orderId, body: { expected_event_seq: 1 }, key: "bad key", type: "application/json" },
    { path: orderId, body: { expected_event_seq: 1 }, key: "cancel-1", type: "text/plain" },
  ])("rejects unsafe cancellation input before Engine", async ({ path, body, key, type }) => {
    const { POST } = await import("../../../app/api/v1/paper/orders/[orderId]/cancel/route");
    const response = await POST(new Request(`https://nexus.test/api/v1/paper/orders/${path}/cancel`, {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "content-type": type,
        "idempotency-key": key,
      },
      body: JSON.stringify(body),
    }));
    expect(response.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });
});
