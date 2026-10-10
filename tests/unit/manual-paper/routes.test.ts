import { beforeEach, expect, it, vi } from "vitest";
const actor = { userId: "allowed-user", requestId: "0198f7a0-1111-7777-8111-111111111111" };
const engineFetch = vi.hoisted(() => vi.fn());
vi.mock("../../../app/lib/auth/nexus-user", () => ({ NexusApiAuthError: class extends Error {}, requireNexusApiActor: vi.fn(async () => actor) }));
vi.mock("../../../app/lib/engine/client", () => ({ engineFetch, sanitizeEngineResponse: async (r: Response) => r }));
beforeEach(() => { engineFetch.mockReset(); engineFetch.mockResolvedValue(Response.json({})); });
it("forwards only explicit mode and original key", async () => {
  const { POST } = await import("../../../app/api/v1/paper/manual-requests/route");
  const response = await POST(new Request("https://nexus.test/api/v1/paper/manual-requests", { method: "POST", headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "original" }, body: '{"mode":"research_only"}' }));
  expect(response.status).toBe(200);
  expect(engineFetch).toHaveBeenCalledWith("/internal/v1/paper/manual-requests", expect.objectContaining({ method: "POST", actor, headers: { "content-type": "application/json", "idempotency-key": "original" } }));
});
it.each([{ mode: "research_only", account_id: "injected" }, { mode: "live" }, {}])("rejects invalid intent before Engine %j", async (body) => {
  const { POST } = await import("../../../app/api/v1/paper/manual-requests/route");
  const r = await POST(new Request("https://nexus.test/api/v1/paper/manual-requests", { method: "POST", headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "original" }, body: JSON.stringify(body) }));
  expect(r.status).toBe(400); expect(engineFetch).not.toHaveBeenCalled();
});
it("queries an original key without writing and rejects missing keys and queries", async () => {
  const { GET } = await import("../../../app/api/v1/paper/manual-requests/route");
  await GET(new Request("https://nexus.test/api/v1/paper/manual-requests", { headers: { "idempotency-key": "original" } }));
  expect(engineFetch).toHaveBeenCalledWith("/internal/v1/paper/manual-requests", { method: "GET", actor, headers: { "idempotency-key": "original" } });
  engineFetch.mockClear();
  expect((await GET(new Request("https://nexus.test/api/v1/paper/manual-requests"))).status).toBe(400);
  expect((await GET(new Request("https://nexus.test/api/v1/paper/manual-requests?account_id=injected", { headers: { "idempotency-key": "original" } }))).status).toBe(400);
  expect(engineFetch).not.toHaveBeenCalled();
});
it("rejects cross-origin starts", async () => {
  const { POST } = await import("../../../app/api/v1/paper/manual-requests/route");
  expect((await POST(new Request("https://nexus.test/api/v1/paper/manual-requests", { method: "POST", headers: { origin: "https://evil.test", "idempotency-key": "original" } }))).status).toBe(403);
  expect(engineFetch).not.toHaveBeenCalled();
});
