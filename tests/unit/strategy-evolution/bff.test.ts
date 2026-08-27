import { beforeEach, describe, expect, it, vi } from "vitest";

import { actionEnvelope, detailEnvelope, experimentId, overviewEnvelope } from "./fixtures";

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

function writeRequest(path: string, body: string, headers: HeadersInit = {}): Request {
  return new Request(`https://nexus.test${path}`, {
    method: "POST",
    headers: {
      origin: "https://nexus.test",
      "content-type": "application/json",
      "idempotency-key": "evolution-action-1",
      "if-match": '"g1-v1"',
      ...headers,
    },
    body,
  });
}

describe("strategy-evolution BFF", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json(overviewEnvelope(), { headers: { etag: '"g1-v1"' } }));
  });

  it("forwards only a canonical bounded overview query", async () => {
    const { GET } = await import("../../../app/api/v1/strategy-evolution/route");
    await GET(new Request("https://nexus.test/api/v1/strategy-evolution?cursor=abc%2Fdef&limit=20"));

    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/strategy-evolution",
      path: "/internal/v1/strategy-evolution",
      query: "cursor=abc%2Fdef&limit=20",
    }, { method: "GET", actor, headers: { accept: "application/json" } });

    for (const query of ["limit=101", "limit=20&limit=21", "redirect=https%3A%2F%2Fevil.test"]) {
      engineFetch.mockClear();
      const response = await GET(new Request(`https://nexus.test/api/v1/strategy-evolution?${query}`));
      expect(response.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("creates canonical empty experiment bytes and rejects browser-selected strategy input", async () => {
    engineFetch.mockResolvedValue(Response.json(actionEnvelope(), { status: 202, headers: { location: "/internal/v1/strategy-evolution", etag: '"g1-v1"' } }));
    const { POST } = await import("../../../app/api/v1/strategy-evolution/experiments/route");
    const accepted = await POST(writeRequest("/api/v1/strategy-evolution/experiments", "{  }"));

    expect(accepted.status).toBe(202);
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toBe("/internal/v1/strategy-experiments");
    expect(new TextDecoder().decode(init.body)).toBe("{}");
    expect(init.headers).toEqual({
      "content-type": "application/json",
      "idempotency-key": "evolution-action-1",
      "if-match": '"g1-v1"',
    });
    expect(accepted.headers.get("location")).toBe("/api/v1/strategy-evolution");

    engineFetch.mockClear();
    const rejected = await POST(writeRequest(
      "/api/v1/strategy-evolution/experiments",
      JSON.stringify({ strategy: { version: "escape" } }),
    ));
    expect(rejected.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("binds experiment reads and vetoes to one canonical UUID", async () => {
    engineFetch.mockResolvedValue(Response.json(detailEnvelope(), { headers: { etag: '"g1-v1"' } }));
    const detailRoute = await import("../../../app/api/v1/strategy-evolution/experiments/[experimentId]/route");
    await detailRoute.GET(new Request(`https://nexus.test/api/v1/strategy-evolution/experiments/${experimentId}`));
    expect(engineFetch).toHaveBeenLastCalledWith({
      template: "/internal/v1/strategy-experiments/{experiment_id}",
      path: `/internal/v1/strategy-experiments/${experimentId}`,
    }, { method: "GET", actor, headers: { accept: "application/json" } });

    engineFetch.mockResolvedValue(Response.json(actionEnvelope("veto_experiment"), { headers: { etag: '"g1-v2"' } }));
    const vetoRoute = await import("../../../app/api/v1/strategy-evolution/experiments/[experimentId]/veto/route");
    await vetoRoute.POST(writeRequest(
      `/api/v1/strategy-evolution/experiments/${experimentId}/veto`,
      '{ "reason": "USER_VETO" }',
    ));
    const [target, init] = engineFetch.mock.calls.at(-1)!;
    expect(target).toEqual({
      template: "/internal/v1/strategy-experiments/{experiment_id}/veto",
      path: `/internal/v1/strategy-experiments/${experimentId}/veto`,
    });
    expect(new TextDecoder().decode(init.body)).toBe('{"reason":"USER_VETO"}');

    engineFetch.mockClear();
    const invalid = await detailRoute.GET(new Request("https://nexus.test/api/v1/strategy-evolution/experiments/not-a-uuid"));
    expect(invalid.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it.each([
    ["control", { action: "pause_auto_promotion" }, "/internal/v1/strategy-registry/control"],
    ["rollback", { reason: "USER_REQUESTED" }, "/internal/v1/strategy-registry/rollback"],
  ] as const)("forwards only the closed %s action", async (routeName, value, target) => {
    engineFetch.mockResolvedValue(Response.json(
      actionEnvelope(routeName === "control" ? "pause_auto_promotion" : "rollback_registry"),
      { status: routeName === "rollback" ? 202 : 200, headers: { location: "/internal/v1/strategy-evolution", etag: '"g1-v2"' } },
    ));
    const route = routeName === "control"
      ? await import("../../../app/api/v1/strategy-evolution/registry/control/route")
      : await import("../../../app/api/v1/strategy-evolution/registry/rollback/route");
    const path = `/api/v1/strategy-evolution/registry/${routeName}`;
    const response = await route.POST(writeRequest(path, JSON.stringify(value)));

    const [, init] = engineFetch.mock.calls[0];
    expect(engineFetch.mock.calls[0][0]).toBe(target);
    expect(new TextDecoder().decode(init.body)).toBe(JSON.stringify(value));
    if (routeName === "rollback") expect(response.headers.get("location")).toBe("/api/v1/strategy-evolution");

    engineFetch.mockClear();
    const rejected = await route.POST(writeRequest(path, JSON.stringify({ ...value, prompt: "escape" })));
    expect(rejected.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("requires a strong registry ETag and one printable idempotency key", async () => {
    const { POST } = await import("../../../app/api/v1/strategy-evolution/experiments/route");
    const invalidHeaders: HeadersInit[] = [
      { "if-match": 'W/"g1-v1"' },
      { "if-match": '"g1-v1"', "idempotency-key": "bad key" },
    ];
    for (const headers of invalidHeaders) {
      engineFetch.mockClear();
      const response = await POST(writeRequest("/api/v1/strategy-evolution/experiments", "{}", headers));
      expect(response.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });
});
