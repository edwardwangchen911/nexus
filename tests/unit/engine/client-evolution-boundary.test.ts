import { afterEach, describe, expect, it, vi } from "vitest";

import { engineFetch, sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { actionEnvelope, detailEnvelope, overviewEnvelope } from "../strategy-evolution/fixtures";

const actor = { userId: "nexus-test-user", requestId: "0198f7a0-1111-7777-8111-111111111114" };

function jsonResponse(value: unknown, status = 200, headers: HeadersInit = {}): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
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

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete process.env.NEXUS_ENGINE_BASE_URL;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  delete process.env.NEXUS_ENV;
});

describe("strategy-evolution Engine boundary", () => {
  it.each([
    ["/internal/v1/strategy-evolution", "GET", overviewEnvelope()],
    ["/internal/v1/strategy-experiments/0198d500-1000-7000-8000-000000002202", "GET", detailEnvelope()],
    ["/internal/v1/strategy-experiments", "POST", actionEnvelope()],
    ["/internal/v1/strategy-registry/control", "POST", actionEnvelope("pause_auto_promotion")],
  ])("accepts only the generated closed response for %s", async (path, method, payload) => {
    const accepted = await sanitizeEngineResponse(jsonResponse(payload), undefined, path, method);
    expect(accepted.status).toBe(200);
    expect(await accepted.json()).toEqual(payload);

    const unsafe = structuredClone(payload) as Record<string, unknown>;
    (unsafe.data as Record<string, unknown>).prompt = "must-not-escape";
    await expectSafeFailure(await sanitizeEngineResponse(jsonResponse(unsafe), undefined, path, method));
  });

  it("binds a detail response to the requested experiment identity", async () => {
    const payload = detailEnvelope();
    payload.data.experiment.experiment_id = "0198d500-1000-7000-8000-000000002299";
    await expectSafeFailure(await sanitizeEngineResponse(
      jsonResponse(payload),
      undefined,
      "/internal/v1/strategy-experiments/0198d500-1000-7000-8000-000000002202",
      "GET",
    ));
  });

  it("rejects broken family and event-chain relationships", async () => {
    const brokenDetail = detailEnvelope();
    brokenDetail.data.family.family_id = "0198d500-1000-7000-8000-000000002299";
    await expectSafeFailure(await sanitizeEngineResponse(
      jsonResponse(brokenDetail), undefined,
      "/internal/v1/strategy-experiments/0198d500-1000-7000-8000-000000002202", "GET",
    ));

    const brokenOverview = overviewEnvelope();
    brokenOverview.data.registry_events[0].sequence = 2;
    await expectSafeFailure(await sanitizeEngineResponse(
      jsonResponse(brokenOverview), undefined, "/internal/v1/strategy-evolution", "GET",
    ));
  });

  it("accepts the evolution error codes only at their declared status", async () => {
    const error = {
      error: {
        code: "EVOLUTION_BLOCKED",
        message: "unsafe upstream detail",
        request_id: actor.requestId,
        details: { prompt: "must-not-escape" },
      },
    };
    const accepted = await sanitizeEngineResponse(
      jsonResponse(error, 422), undefined, "/internal/v1/strategy-experiments", "POST",
    );
    expect(accepted.status).toBe(422);
    expect(await accepted.json()).toMatchObject({
      error: { code: "EVOLUTION_BLOCKED", details: {}, message: "The strategy evolution action is blocked." },
    });
    await expectSafeFailure(await sanitizeEngineResponse(
      jsonResponse(error, 503), undefined, "/internal/v1/strategy-experiments", "POST",
    ));
  });

  it("canonicalizes the bounded overview query and rejects unknown evolution targets", async () => {
    setEngineEnvironment();
    const calls: Request[] = [];
    vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push(new Request(input, init));
      return jsonResponse(overviewEnvelope(), 200, { etag: '"g1-v1"' });
    });

    const response = await engineFetch({
      template: "/internal/v1/strategy-evolution",
      path: "/internal/v1/strategy-evolution",
      query: "limit=20&cursor=abc%2Fdef",
    }, { method: "GET", actor });
    expect(response.status).toBe(200);
    expect(calls[0].url).toBe("http://127.0.0.1:8000/internal/v1/strategy-evolution?cursor=abc%2Fdef&limit=20");

    const invalid = await engineFetch({
      template: "/internal/v1/strategy-evolution",
      path: "/internal/v1/strategy-evolution",
      query: "limit=101",
    }, { method: "GET", actor });
    expect(invalid.status).toBe(500);
    expect(calls).toHaveLength(1);
  });

  it("rejects an evolution response larger than one MiB", async () => {
    setEngineEnvironment();
    const payload = overviewEnvelope();
    payload.data.champion.strategy_version = "x".repeat(1024 * 1024);
    vi.stubGlobal("fetch", async () => jsonResponse(payload));
    await expectSafeFailure(await engineFetch("/internal/v1/strategy-evolution", { method: "GET", actor }));
  });
});
