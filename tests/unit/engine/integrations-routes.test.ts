import { beforeEach, describe, expect, it, vi } from "vitest";

const actor = { userId: "allowed-user", requestId: "0198f7a0-1111-7777-8111-111111111111" };
const engineFetch = vi.hoisted(() => vi.fn());

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

vi.mock("../../../app/lib/engine/client", () => ({
  engineFetch,
  sanitizeEngineResponse: vi.fn(async (response: Response) => response),
}));

const success = () =>
  Response.json({
    data: {
      provider: "fosun",
      mode: "fixture",
      quality: "PASS",
      as_of: "2026-08-22T12:00:00Z",
      capability_expires_at: null,
      contract_hash: null,
      next_action: null,
      version: 1,
    },
    meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-22T12:00:00Z" },
  });

describe("integration BFF routes", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockImplementation(async () => success());
  });

  it("maps list status to the generated internal target", async () => {
    const { GET } = await import("../../../app/api/v1/integrations/route");
    const response = await GET(new Request("https://nexus.test/api/v1/integrations"));

    expect(response.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith("/internal/v1/integrations", {
      method: "GET",
      actor,
    });
  });

  it("forwards exact fixture-test bytes and write controls to a closed provider route", async () => {
    const { POST } = await import("../../../app/api/v1/integrations/[provider]/test/route");
    const request = new Request("https://nexus.test/api/v1/integrations/fosun/test", {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "content-type": "application/json",
        "idempotency-key": "test-key",
        "if-match": '"1"',
      },
      body: "{}",
    });

    const response = await POST(request);

    expect(response.status).toBe(200);
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toEqual({
      template: "/internal/v1/integrations/{provider}/test",
      path: "/internal/v1/integrations/fosun/test",
    });
    expect(init).toMatchObject({
      method: "POST",
      actor,
      headers: {
        "content-type": "application/json",
        "idempotency-key": "test-key",
        "if-match": '"1"',
      },
    });
    expect([...init.body]).toEqual([...new TextEncoder().encode("{}")]);
  });

  it("forwards exact mode bytes and rejects writes missing version controls before engine", async () => {
    const { POST } = await import("../../../app/api/v1/integrations/[provider]/mode/route");
    const body = '{"mode":"disabled"}';
    const valid = new Request("https://nexus.test/api/v1/integrations/fosun/mode", {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "content-type": "application/json",
        "idempotency-key": "mode-key",
        "if-match": '"1"',
      },
      body,
    });

    expect((await POST(valid)).status).toBe(200);
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toEqual({
      template: "/internal/v1/integrations/{provider}/mode",
      path: "/internal/v1/integrations/fosun/mode",
    });
    expect(init).toMatchObject({ method: "POST", actor });
    expect([...init.body]).toEqual([...new TextEncoder().encode(body)]);

    engineFetch.mockClear();
    const missingVersion = new Request("https://nexus.test/api/v1/integrations/fosun/mode", {
      method: "POST",
      headers: { origin: "https://nexus.test", "idempotency-key": "mode-key-2" },
      body,
    });
    const rejected = await POST(missingVersion);

    expect(rejected.status).toBe(400);
    expect((await rejected.json()).error.code).toBe("VALIDATION_ERROR");
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("rejects provider path injection before the engine boundary", async () => {
    const { POST } = await import("../../../app/api/v1/integrations/[provider]/test/route");
    const request = new Request("https://nexus.test/api/v1/integrations/unknown/test", {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "idempotency-key": "bad-provider",
        "if-match": '"0"',
      },
      body: "{}",
    });

    const response = await POST(request);

    expect(response.status).toBe(400);
    expect((await response.json()).error.code).toBe("VALIDATION_ERROR");
    expect(engineFetch).not.toHaveBeenCalled();
  });
});
