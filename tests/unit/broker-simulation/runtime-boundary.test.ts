import { afterEach, describe, expect, it, vi } from "vitest";

import { engineFetch, sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { bodyHash, envelope, intentId, orderId, readyWorkbench, runId } from "./fixtures";

const actor = { userId: "nexus-test-user", requestId: "0198f7a0-1111-7777-8111-111111111211" };

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete process.env.NEXUS_ENGINE_BASE_URL;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  delete process.env.NEXUS_ENV;
});

describe("broker simulation Engine boundary", () => {
  it("accepts the closed workbench and rejects leaked or inconsistent facts", async () => {
    expect((await sanitizeEngineResponse(json(envelope()), undefined, "/internal/v1/broker-simulation", "GET")).status).toBe(200);

    const leaked = structuredClone(readyWorkbench) as unknown as Record<string, unknown>;
    (leaked.capability as Record<string, unknown>).api_key = "must-not-escape";
    await expectSafeFailure(await sanitizeEngineResponse(json(envelope(leaked as never)), undefined, "/internal/v1/broker-simulation", "GET"));

    const unsafe = structuredClone(readyWorkbench);
    unsafe.actions.can_dispatch = true as false;
    await expectSafeFailure(await sanitizeEngineResponse(json(envelope(unsafe)), undefined, "/internal/v1/broker-simulation", "GET"));

    const mismatched = structuredClone(readyWorkbench);
    mismatched.events[0].order_id = "0198f7a0-1111-7777-8111-111111111299";
    await expectSafeFailure(await sanitizeEngineResponse(json(envelope(mismatched)), undefined, "/internal/v1/broker-simulation", "GET"));
  });

  it("binds action responses to their path and rejects success on disabled write routes", async () => {
    const approvePath = `/internal/v1/broker-simulation/intents/${intentId}/approve`;
    const approve = actionEnvelope(intentId, "approve");
    expect((await sanitizeEngineResponse(json(approve), undefined, approvePath, "POST")).status).toBe(200);
    expect((await sanitizeEngineResponse(json(actionEnvelope(orderId, "approve")), undefined, approvePath, "POST")).status).toBe(500);
    expect((await sanitizeEngineResponse(json(approve), undefined, approvePath, "GET")).status).toBe(500);

    const ackPath = `/internal/v1/broker-simulation/reconciliation/${runId}/acknowledge`;
    expect((await sanitizeEngineResponse(json(actionEnvelope(runId, "acknowledge")), undefined, ackPath, "POST")).status).toBe(200);

    const cancelPath = `/internal/v1/broker-simulation/orders/${orderId}/cancel`;
    expect((await sanitizeEngineResponse(json(actionEnvelope(orderId, "approve")), undefined, cancelPath, "POST")).status).toBe(500);
  });

  it("allows only UUID-bound broker targets and sanitizes the disabled-write error", async () => {
    setEngineEnvironment();
    const calls: string[] = [];
    vi.stubGlobal("fetch", async (input: RequestInfo | URL) => {
      const path = new URL(String(input)).pathname;
      calls.push(path);
      return path.endsWith("/approve") ? json(actionEnvelope(intentId, "approve")) : json(envelope());
    });

    expect((await engineFetch("/internal/v1/broker-simulation", { method: "GET", actor })).status).toBe(200);
    expect((await engineFetch({
      template: "/internal/v1/broker-simulation/intents/{intent_id}/approve",
      path: `/internal/v1/broker-simulation/intents/${intentId}/approve`,
    }, { method: "POST", actor, body: '{"version":1}', headers: { "if-match": bodyHash } })).status).toBe(200);
    expect((await engineFetch({
      template: "/internal/v1/broker-simulation/intents/{intent_id}/approve",
      path: "/internal/v1/broker-simulation/intents/not-a-uuid/approve",
    }, { method: "POST", actor })).status).toBe(500);
    expect(calls).toHaveLength(2);

    const error = {
      error: {
        code: "SIMULATION_WRITE_DISABLED",
        message: "provider detail must not escape",
        request_id: actor.requestId,
        details: { provider: "secret" },
      },
    };
    const sanitized = await sanitizeEngineResponse(json(error, 409), undefined, `/internal/v1/broker-simulation/orders/${orderId}/cancel`, "POST");
    expect(sanitized.status).toBe(409);
    expect(await sanitized.json()).toMatchObject({
      error: { code: "SIMULATION_WRITE_DISABLED", message: "External broker simulation writes are disabled.", details: {} },
    });
  });
});

function actionEnvelope(resourceId: string, action: "approve" | "reject" | "acknowledge") {
  return {
    data: { resource_id: resourceId, action, status: action === "approve" ? "approved" : action === "reject" ? "rejected" : "acknowledged", version: action === "acknowledge" ? null : 2, replayed: false, dispatch_created: false },
    meta: envelope().meta,
  };
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}

async function expectSafeFailure(response: Response): Promise<void> {
  expect(response.status).toBe(500);
  expect(await response.json()).toMatchObject({ error: { code: "INTERNAL_ERROR", details: {} } });
}

function setEngineEnvironment(): void {
  process.env.NEXUS_ENGINE_BASE_URL = "http://127.0.0.1:8000";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID = "test-current";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET = "nexus-test-hmac-key-32-bytes!!!!";
  process.env.NEXUS_ENV = "test";
}

