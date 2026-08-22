import { afterEach, describe, expect, it, vi } from "vitest";
import { engineFetch } from "../../../app/lib/engine/client";

const actor = { userId: "nexus-test-user", requestId: "0198f7a0-1111-7777-8111-111111111114" };

function setEngineEnvironment(): void {
  process.env.NEXUS_ENGINE_BASE_URL = "http://127.0.0.1:8000";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID = "test-current";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET = "nexus-test-hmac-key-32-bytes!!!!";
  process.env.NEXUS_ENV = "test";
}

afterEach(() => {
  vi.unstubAllGlobals();
  delete process.env.NEXUS_ENGINE_BASE_URL;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  delete process.env.NEXUS_ENV;
});

describe("engineFetch runtime boundary", () => {
  it.each(["//attacker.invalid", "https://attacker.invalid/x", "/internal/v1/system-status?x=1", "/internal/v1/unknown"])(
    "rejects unsafe path %s before any network call",
    async (path) => {
      setEngineEnvironment();
      let calls = 0;
      vi.stubGlobal("fetch", async () => {
        calls += 1;
        return Response.json({});
      });

      await expect(engineFetch(path as "/internal/v1/system-status", { method: "GET", actor })).resolves.toMatchObject({ status: 500 });
      expect(calls).toBe(0);
    },
  );

  it("rebuilds only the strict system-status success envelope", async () => {
    setEngineEnvironment();
    vi.stubGlobal(
      "fetch",
      async () =>
        new Response(
          JSON.stringify({
            data: { status: "ok" },
            meta: {
              request_id: "0198f7a0-1111-7777-8111-111111111114",
              schema_version: "1",
              as_of: "2026-08-22T00:00:00Z",
            },
          }),
          { status: 200, headers: { "content-type": "application/json", "x-secret": "discard" } },
        ),
    );

    const response = await engineFetch("/internal/v1/system-status", { method: "GET", actor });

    expect(response.status).toBe(200);
    expect(response.headers.get("x-secret")).toBeNull();
    expect(await response.json()).toEqual({
      data: { status: "ok" },
      meta: {
        request_id: "0198f7a0-1111-7777-8111-111111111114",
        schema_version: "1",
        as_of: "2026-08-22T00:00:00Z",
      },
    });
  });
});
