import { beforeEach, describe, expect, it, vi } from "vitest";

import { groupId, portfolioId, proposalIds } from "./fixtures";

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

describe("optimization BFF routes", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json({
      data: { proposal_group_id: groupId, status: "compared", version: 4 },
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-24T00:00:00Z" },
    }));
  });

  it("forwards only canonical overview filters", async () => {
    const { GET } = await import("../../../app/api/v1/overview/route");
    await GET(new Request(`https://nexus.test/api/v1/overview?session_date=2026-08-24&portfolio_id=${portfolioId}`));

    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/overview",
      path: "/internal/v1/overview",
      query: `portfolio_id=${portfolioId}&session_date=2026-08-24`,
    }, { method: "GET", actor });

    engineFetch.mockClear();
    const invalid = await GET(new Request("https://nexus.test/api/v1/overview?session_date=08%2F24%2F2026&redirect=bad"));
    expect(invalid.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();

    const impossibleDate = await GET(new Request("https://nexus.test/api/v1/overview?session_date=2026-99-99"));
    expect(impossibleDate.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("validates the diagnostic and proposal lookup identities", async () => {
    const diagnostics = await import("../../../app/api/v1/portfolios/[portfolioId]/diagnostics/latest/route");
    const proposals = await import("../../../app/api/v1/optimization-proposals/[proposalGroupId]/route");

    await diagnostics.GET(new Request(`https://nexus.test/api/v1/portfolios/${portfolioId}/diagnostics/latest`));
    expect(engineFetch).toHaveBeenLastCalledWith({
      template: "/internal/v1/portfolios/{portfolio_id}/diagnostics/latest",
      path: `/internal/v1/portfolios/${portfolioId}/diagnostics/latest`,
    }, { method: "GET", actor });
    await proposals.GET(new Request(`https://nexus.test/api/v1/optimization-proposals/${groupId}`));
    expect(engineFetch).toHaveBeenLastCalledWith({
      template: "/internal/v1/optimization-proposals/{group_id}",
      path: `/internal/v1/optimization-proposals/${groupId}`,
    }, { method: "GET", actor });

    engineFetch.mockClear();
    const invalid = await proposals.GET(new Request("https://nexus.test/api/v1/optimization-proposals/not-a-uuid"));
    expect(invalid.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("forwards exact proposal-admission bytes with one idempotency key", async () => {
    const jobId = "0198d500-1000-7000-8000-000000001398";
    engineFetch.mockResolvedValueOnce(Response.json({
      data: { job_id: jobId, status: "QUEUED" },
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-24T00:00:00Z" },
    }, { status: 202 }));
    const { POST } = await import("../../../app/api/v1/optimization-proposals/route");
    const body = JSON.stringify({
      portfolio_snapshot_id: "0198d500-1000-7000-8000-000000001302",
      data_snapshot_id: "0198d500-1000-7000-8000-000000001309",
      cost_policy_id: "0198d500-1000-7000-8000-000000001311",
    });
    const response = await POST(new Request("https://nexus.test/api/v1/optimization-proposals", {
      method: "POST",
      headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "proposal-13" },
      body,
    }));

    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toBe("/internal/v1/optimization-proposals");
    expect(init).toMatchObject({ method: "POST", actor, headers: { "content-type": "application/json", "idempotency-key": "proposal-13" } });
    expect(new TextDecoder().decode(init.body)).toBe(body);
    expect(response.status).toBe(202);
    expect(response.headers.get("location")).toBe(`/api/v1/jobs/${jobId}`);
  });

  it.each(["shadow", "reject"] as const)("binds %s to exact If-Match and version", async (action) => {
    const route = action === "shadow"
      ? await import("../../../app/api/v1/optimization-proposals/[proposalGroupId]/shadow/route")
      : await import("../../../app/api/v1/optimization-proposals/[proposalGroupId]/reject/route");
    const body = JSON.stringify({ version: 4 });
    await route.POST(new Request(`https://nexus.test/api/v1/optimization-proposals/${groupId}/${action}`, {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "content-type": "application/json",
        "idempotency-key": `${action}-13`,
        "if-match": '"4"',
      },
      body,
    }));

    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toEqual({
      template: `/internal/v1/optimization-proposals/{group_id}/${action}`,
      path: `/internal/v1/optimization-proposals/${groupId}/${action}`,
    });
    expect(init).toMatchObject({
      method: "POST",
      actor,
      headers: {
        "content-type": "application/json",
        "idempotency-key": `${action}-13`,
        "if-match": '"4"',
      },
    });
    expect(new TextDecoder().decode(init.body)).toBe(body);

    engineFetch.mockClear();
    const mismatch = await route.POST(new Request(`https://nexus.test/api/v1/optimization-proposals/${groupId}/${action}`, {
      method: "POST",
      headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "bad", "if-match": '"3"' },
      body,
    }));
    expect(mismatch.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("accepts the persisted draft version zero while rejecting invalid lifecycle versions", async () => {
    const route = await import("../../../app/api/v1/optimization-proposals/[proposalGroupId]/reject/route");
    const accepted = await route.POST(new Request(`https://nexus.test/api/v1/optimization-proposals/${groupId}/reject`, {
      method: "POST",
      headers: {
        origin: "https://nexus.test",
        "content-type": "application/json",
        "idempotency-key": "reject-draft-13",
        "if-match": '"0"',
      },
      body: JSON.stringify({ version: 0 }),
    }));
    expect(accepted.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledTimes(1);

    for (const version of [-1, 0.5, "0"]) {
      engineFetch.mockClear();
      const rejected = await route.POST(new Request(`https://nexus.test/api/v1/optimization-proposals/${groupId}/reject`, {
        method: "POST",
        headers: {
          origin: "https://nexus.test",
          "content-type": "application/json",
          "idempotency-key": "invalid-version-13",
          "if-match": `"${version}"`,
        },
        body: JSON.stringify({ version }),
      }));
      expect(rejected.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("forwards a proposal-anchored what-if body without adding identities or weights", async () => {
    engineFetch.mockResolvedValueOnce(Response.json({
      data: {
        proposal_group_id: groupId,
        proposal_id: proposalIds.minimum_change,
        policy_version: "optimizer_v1",
        instrument_count: 1,
        result: {
          diagnostics: {},
          constraints: { violation_count: { status: "known", value: 0, unit: "count", reason: null } },
        },
      },
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-24T00:00:00Z" },
    }));
    const { POST } = await import("../../../app/api/v1/what-if/evaluate/route");
    const body = JSON.stringify({
      proposal_group_id: groupId,
      proposal_id: proposalIds.minimum_change,
      policy_version: "optimizer_v1",
      positions: [{ instrument_id: "0198d500-1000-7000-8000-000000001307", weight: "0.1400" }],
    });
    await POST(new Request("https://nexus.test/api/v1/what-if/evaluate", {
      method: "POST",
      headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "what-if-13" },
      body,
    }));

    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toBe("/internal/v1/what-if/evaluate");
    expect(new TextDecoder().decode(init.body)).toBe(body);
    expect(init.headers).toEqual({ "content-type": "application/json", "idempotency-key": "what-if-13" });
  });

  it("fails closed when a what-if response is bound to another proposal", async () => {
    engineFetch.mockResolvedValueOnce(Response.json({
      data: {
        proposal_group_id: groupId,
        proposal_id: proposalIds.hold_current,
        policy_version: "optimizer_v1",
        instrument_count: 1,
        result: {
          diagnostics: {},
          constraints: { violation_count: { status: "known", value: 0, unit: "count", reason: null } },
        },
      },
      meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-24T00:00:00Z" },
    }));
    const { POST } = await import("../../../app/api/v1/what-if/evaluate/route");
    const response = await POST(new Request("https://nexus.test/api/v1/what-if/evaluate", {
      method: "POST",
      headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "mismatch-13" },
      body: JSON.stringify({
        proposal_group_id: groupId,
        proposal_id: proposalIds.minimum_change,
        policy_version: "optimizer_v1",
        positions: [{ instrument_id: "0198d500-1000-7000-8000-000000001307", weight: "0.14" }],
      }),
    }));

    expect(response.status).toBe(500);
    expect(await response.json()).toMatchObject({ error: { code: "INTERNAL_ERROR" } });
  });

  it("rejects duplicate or non-decimal what-if positions before Engine", async () => {
    const { POST } = await import("../../../app/api/v1/what-if/evaluate/route");
    const invalidBodies = [
      {
        proposal_group_id: groupId,
        proposal_id: proposalIds.minimum_change,
        policy_version: "optimizer_v1",
        positions: [
          { instrument_id: "0198d500-1000-7000-8000-000000001307", weight: "0.5" },
          { instrument_id: "0198d500-1000-7000-8000-000000001307", weight: "0.5" },
        ],
      },
      {
        proposal_group_id: groupId,
        proposal_id: proposalIds.minimum_change,
        policy_version: "optimizer_v1",
        positions: [{ instrument_id: "0198d500-1000-7000-8000-000000001307", weight: "5e-1" }],
      },
    ];

    for (const value of invalidBodies) {
      engineFetch.mockClear();
      const response = await POST(new Request("https://nexus.test/api/v1/what-if/evaluate", {
        method: "POST",
        headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "invalid-what-if" },
        body: JSON.stringify(value),
      }));
      expect(response.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });
});
