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

describe("strategy BFF route", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json({
      data: [],
      meta: {
        request_id: actor.requestId,
        schema_version: "1",
        as_of: "2026-08-26T00:00:00Z",
      },
    }));
  });

  it("forwards the read-only strategy registry to the closed internal target", async () => {
    const { GET } = await import("../../../app/api/v1/strategies/route");

    const response = await GET(new Request("https://nexus.test/api/v1/strategies"));

    expect(response.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith("/internal/v1/strategies", {
      method: "GET",
      actor,
    });
  });
});
