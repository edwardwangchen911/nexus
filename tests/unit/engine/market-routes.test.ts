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

const instrumentId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781";

describe("market BFF routes", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockImplementation(async () => Response.json({
      data: { instrument_id: instrumentId, watchlisted: true },
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-22T12:00:00Z" },
    }));
  });

  it("forwards one allowlisted market query with exact encoded bytes", async () => {
    const { GET } = await import("../../../app/api/v1/market/route");
    const response = await GET(new Request(
      `https://nexus.test/api/v1/market?watchlist=true&limit=50&selected_instrument_id=${instrumentId}&cursor=opaque%2Bcursor`,
    ));

    expect(response.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/market",
      path: "/internal/v1/market",
      query: `cursor=opaque%2Bcursor&limit=50&selected_instrument_id=${instrumentId}&watchlist=true`,
    }, { method: "GET", actor });
  });

  it("rejects unknown or duplicate query parameters before signing", async () => {
    const { GET } = await import("../../../app/api/v1/market/route");
    const unknown = await GET(new Request("https://nexus.test/api/v1/market?redirect=https://attacker.invalid"));
    const duplicate = await GET(new Request("https://nexus.test/api/v1/market?limit=10&limit=20"));

    expect(unknown.status).toBe(400);
    expect((await unknown.json()).error.code).toBe("VALIDATION_ERROR");
    expect(duplicate.status).toBe(400);
    expect((await duplicate.json()).error.code).toBe("VALIDATION_ERROR");
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it.each(["PUT", "DELETE"] as const)("forwards idempotent %s without inventing a body or key", async (method) => {
    const route = await import("../../../app/api/v1/watchlist/[instrumentId]/route");
    const handler = route[method];
    const response = await handler(new Request(`https://nexus.test/api/v1/watchlist/${instrumentId}`, {
      method,
      headers: { origin: "https://nexus.test" },
    }));

    expect(response.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/watchlist/{instrument_id}",
      path: `/internal/v1/watchlist/${instrumentId}`,
    }, { method, actor });
  });

  it("rejects malformed instrument IDs and cross-origin writes before engine", async () => {
    const { PUT, DELETE } = await import("../../../app/api/v1/watchlist/[instrumentId]/route");
    const malformed = await PUT(new Request("https://nexus.test/api/v1/watchlist/not-a-uuid", {
      method: "PUT",
      headers: { origin: "https://nexus.test" },
    }));
    const crossOrigin = await DELETE(new Request(`https://nexus.test/api/v1/watchlist/${instrumentId}`, {
      method: "DELETE",
      headers: { origin: "https://attacker.invalid" },
    }));

    expect(malformed.status).toBe(400);
    expect((await malformed.json()).error.code).toBe("VALIDATION_ERROR");
    expect(crossOrigin.status).toBe(403);
    expect((await crossOrigin.json()).error.code).toBe("FORBIDDEN");
    expect(engineFetch).not.toHaveBeenCalled();
  });
});
