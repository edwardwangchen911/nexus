// @vitest-environment node
import { afterEach, expect, it, vi } from "vitest";
import { GET, POST } from "../../../app/api/v1/paper/manual-requests/route";

const id = "0198f7a0-1111-7777-8111-111111111111";
const meta = { request_id: id, schema_version: "1", as_of: "2026-10-09T14:00:00Z" };
afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); });
it.each(["GET", "POST"] as const)("%s traverses the real BFF client path with the original key", async method => {
  vi.stubEnv("NEXUS_ENGINE_BASE_URL", "http://127.0.0.1:8000");
  vi.stubEnv("NEXUS_ENGINE_HMAC_CURRENT_SECRET", "isolated-test-secret");
  const data = method === "GET" ? null : { request_id: id, mode: "research_only", requested_at: meta.as_of, status: "queued", code: "MANUAL_REQUEST_QUEUED", cycle_id: null, execution_locked: true };
  const fetch = vi.fn(async () => Response.json({ data, meta }));
  vi.stubGlobal("fetch", fetch);
  const response = await (method === "GET" ? GET : POST)(new Request("http://127.0.0.1:3000/api/v1/paper/manual-requests", {
    method, headers: { "Idempotency-Key": "original-key", ...(method === "POST" ? { origin: "http://127.0.0.1:3000", "content-type": "application/json" } : {}) },
    ...(method === "POST" ? { body: JSON.stringify({ mode: "research_only" }) } : {}),
  }));
  expect(response.status).toBe(200);
  expect(fetch).toHaveBeenCalledTimes(1);
  const [url, init] = fetch.mock.calls[0] as unknown as [URL, RequestInit];
  expect(url.pathname).toBe("/internal/v1/paper/manual-requests");
  expect(init.method).toBe(method);
  expect(new Headers(init.headers).get("Idempotency-Key")).toBe("original-key");
  expect(await response.json()).toEqual({ data, meta });
});
