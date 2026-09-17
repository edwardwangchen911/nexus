import { beforeEach, describe, expect, it, vi } from "vitest";

import { bodyHash, envelope, intentId, orderId, proposalGroupId, proposalId, runId } from "./fixtures";

const actor = { userId: "allowed-user", requestId: "0198f7a0-1111-7777-8111-111111111211" };
const engineFetch = vi.hoisted(() => vi.fn());

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

vi.mock("../../../app/lib/engine/client", () => ({
  engineFetch,
  sanitizeEngineResponse: vi.fn(async (response: Response) => response),
}));

function writeRequest(path: string, body: unknown, ifMatch = '"1"'): Request {
  return new Request(`https://nexus.test${path}`, {
    method: "POST",
    headers: {
      origin: "https://nexus.test",
      "content-type": "application/json",
      "idempotency-key": "broker-simulation-action-1",
      "if-match": ifMatch,
    },
    body: JSON.stringify(body),
  });
}

describe("broker-simulation BFF", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json(envelope()));
  });

  it("reads only the exact workbench route", async () => {
    const { GET } = await import("../../../app/api/v1/broker-simulation/route");
    const response = await GET(new Request("https://nexus.test/api/v1/broker-simulation"));
    expect(response.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith(
      "/internal/v1/broker-simulation",
      { method: "GET", actor, headers: { accept: "application/json" } },
    );

    engineFetch.mockClear();
    const rejected = await GET(new Request("https://nexus.test/api/v1/broker-simulation?account_id=escape"));
    expect(rejected.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("prepares only from one exact proposal or Agent Paper source", async () => {
    const { POST } = await import("../../../app/api/v1/broker-simulation/intents/route");
    await POST(writeRequest("/api/v1/broker-simulation/intents", { proposal_group_id: proposalGroupId, proposal_id: proposalId }));
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toBe("/internal/v1/broker-simulation/intents");
    expect(new TextDecoder().decode(init.body)).toBe(JSON.stringify({ proposal_group_id: proposalGroupId, proposal_id: proposalId }));
    expect(init.headers).toEqual({
      "content-type": "application/json",
      "idempotency-key": "broker-simulation-action-1",
    });

    for (const forbidden of ["account_id", "symbol", "price", "quantity", "provider_id", "url", "method", "headers"]) {
      engineFetch.mockClear();
      const response = await POST(writeRequest("/api/v1/broker-simulation/intents", {
        proposal_group_id: proposalGroupId,
        proposal_id: proposalId,
        [forbidden]: "escape",
      }));
      expect(response.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }

    engineFetch.mockClear();
    const agentPaper = {
      agent_paper_cycle_id: intentId,
      account_binding_id: orderId,
      portfolio_snapshot_id: runId,
    };
    await POST(writeRequest("/api/v1/broker-simulation/intents", agentPaper));
    expect(new TextDecoder().decode(engineFetch.mock.calls[0][1].body)).toBe(JSON.stringify(agentPaper));

    for (const invalid of [
      { agent_paper_cycle_id: intentId, account_binding_id: orderId },
      { ...agentPaper, proposal_group_id: proposalGroupId, proposal_id: proposalId },
      { ...agentPaper, quantity: "10" },
    ]) {
      engineFetch.mockClear();
      const response = await POST(writeRequest("/api/v1/broker-simulation/intents", invalid));
      expect(response.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("binds approval to the intent body hash, not browser order facts", async () => {
    const { POST } = await import("../../../app/api/v1/broker-simulation/intents/[intentId]/approve/route");
    await POST(writeRequest(`/api/v1/broker-simulation/intents/${intentId}/approve`, { version: 1 }, bodyHash));
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toEqual({
      template: "/internal/v1/broker-simulation/intents/{intent_id}/approve",
      path: `/internal/v1/broker-simulation/intents/${intentId}/approve`,
    });
    expect(new TextDecoder().decode(init.body)).toBe('{"version":1}');
    expect(init.headers["if-match"]).toBe(bodyHash);

    for (const invalid of [
      { path: intentId, body: { version: 1, quantity: "100" }, etag: bodyHash },
      { path: intentId, body: { version: 1 }, etag: '"1"' },
      { path: "not-a-uuid", body: { version: 1 }, etag: bodyHash },
    ]) {
      engineFetch.mockClear();
      const response = await POST(writeRequest(`/api/v1/broker-simulation/intents/${invalid.path}/approve`, invalid.body, invalid.etag));
      expect(response.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("forwards only fixed reject, modify, cancel and reconciliation commands", async () => {
    const cases = [
      {
        route: await import("../../../app/api/v1/broker-simulation/intents/[intentId]/reject/route"),
        path: `/api/v1/broker-simulation/intents/${intentId}/reject`,
        target: { template: "/internal/v1/broker-simulation/intents/{intent_id}/reject", path: `/internal/v1/broker-simulation/intents/${intentId}/reject` },
        body: { version: 1, reason: "user_rejected" },
        etag: '"1"',
      },
      {
        route: await import("../../../app/api/v1/broker-simulation/orders/[orderId]/modify/route"),
        path: `/api/v1/broker-simulation/orders/${orderId}/modify`,
        target: { template: "/internal/v1/broker-simulation/orders/{order_id}/modify", path: `/internal/v1/broker-simulation/orders/${orderId}/modify` },
        body: { expected_event_sequence: 0, reason: "replace_price" },
        etag: '"0"',
      },
      {
        route: await import("../../../app/api/v1/broker-simulation/orders/[orderId]/cancel/route"),
        path: `/api/v1/broker-simulation/orders/${orderId}/cancel`,
        target: { template: "/internal/v1/broker-simulation/orders/{order_id}/cancel", path: `/internal/v1/broker-simulation/orders/${orderId}/cancel` },
        body: { expected_event_sequence: 0, reason: "user_cancel" },
        etag: '"0"',
      },
      {
        route: await import("../../../app/api/v1/broker-simulation/reconciliation/[runId]/acknowledge/route"),
        path: `/api/v1/broker-simulation/reconciliation/${runId}/acknowledge`,
        target: { template: "/internal/v1/broker-simulation/reconciliation/{run_id}/acknowledge", path: `/internal/v1/broker-simulation/reconciliation/${runId}/acknowledge` },
        body: {},
        etag: '"matched"',
      },
    ];

    for (const item of cases) {
      engineFetch.mockClear();
      const response = await item.route.POST(writeRequest(item.path, item.body, item.etag));
      expect(response.status).toBe(200);
      expect(engineFetch.mock.calls[0][0]).toEqual(item.target);
      expect(new TextDecoder().decode(engineFetch.mock.calls[0][1].body)).toBe(JSON.stringify(item.body));
      expect(engineFetch.mock.calls[0][1].headers["if-match"]).toBe(item.etag);

      engineFetch.mockClear();
      const rejected = await item.route.POST(writeRequest(item.path, { ...item.body, account_id: "escape" }, item.etag));
      expect(rejected.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("rejects cross-origin and malformed write authority before Engine", async () => {
    const { POST } = await import("../../../app/api/v1/broker-simulation/intents/[intentId]/reject/route");
    const unsafe = writeRequest(`/api/v1/broker-simulation/intents/${intentId}/reject`, { version: 1, reason: "user_rejected" });
    unsafe.headers.set("origin", "https://evil.test");
    expect((await POST(unsafe)).status).toBe(403);

    for (const [body, etag] of [
      [{ version: 0, reason: "user_rejected" }, '"0"'],
      [{ version: 1, reason: "anything" }, '"1"'],
      [{ version: 1, reason: "user_rejected" }, 'W/"1"'],
    ] as const) {
      engineFetch.mockClear();
      const response = await POST(writeRequest(`/api/v1/broker-simulation/intents/${intentId}/reject`, body, etag));
      expect(response.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });
});
