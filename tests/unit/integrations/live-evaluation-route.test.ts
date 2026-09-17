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

describe("live-evaluation BFF route", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json({
      data: {},
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-09-17T01:00:00Z" },
    }));
  });

  it("forwards only the query-free read endpoint", async () => {
    const { GET } = await import("../../../app/api/v1/live-evaluation/route");
    const response = await GET(new Request("https://nexus.test/api/v1/live-evaluation"));

    expect(response.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith(
      "/internal/v1/live-evaluation",
      { method: "GET", actor },
    );

    engineFetch.mockClear();
    const rejected = await GET(new Request("https://nexus.test/api/v1/live-evaluation?unlock=true"));
    expect(rejected.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });
});
