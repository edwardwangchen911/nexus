import { beforeEach, describe, expect, it, vi } from "vitest";

const actor = { userId: "allowed-user", requestId: "0198f7a0-1111-7777-8111-111111111111" };
const jobId = "0198f7a0-1111-7777-8111-111111111112";
const engineFetch = vi.hoisted(() => vi.fn());

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

vi.mock("../../../app/lib/engine/client", () => ({
  engineFetch,
  sanitizeEngineResponse: vi.fn(async (response: Response) => response),
  sanitizeAuditExportResponse: vi.fn(async (response: Response) => response),
}));

describe("risk-audit BFF routes", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json({ data: {}, meta: {} }));
  });

  it("forwards readiness without query", async () => {
    const { GET } = await import("../../../app/api/v1/readiness/route");
    await GET(new Request("https://nexus.test/api/v1/readiness"));
    expect(engineFetch).toHaveBeenCalledWith("/health/ready", { method: "GET", actor });

    engineFetch.mockClear();
    expect((await GET(new Request("https://nexus.test/api/v1/readiness?verbose=true"))).status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("canonicalizes the closed audit filters", async () => {
    const { GET } = await import("../../../app/api/v1/risk-audit/route");
    await GET(new Request("https://nexus.test/api/v1/risk-audit?to=2026-08-27T00%3A00%3A00Z&limit=25&kind=risk&from=2026-08-20T00%3A00%3A00Z"));
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/risk-audit",
      path: "/internal/v1/risk-audit",
      query: "from=2026-08-20T00%3A00%3A00Z&kind=risk&limit=25&to=2026-08-27T00%3A00%3A00Z",
    }, { method: "GET", actor });
  });

  it.each([
    "kind=unknown", "limit=0", "limit=101", "kind=risk&kind=job", "from=yesterday",
    "to=2026-08-27T00%3A00%3A00%2B08%3A00", "status=failed", "cursor=",
  ])("rejects unsafe audit filters before Engine: %s", async (query) => {
    const { GET } = await import("../../../app/api/v1/risk-audit/route");
    expect((await GET(new Request(`https://nexus.test/api/v1/risk-audit?${query}`))).status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("requires a bounded export window and never forwards caller headers", async () => {
    const { GET } = await import("../../../app/api/v1/risk-audit/export/route");
    await GET(new Request("https://nexus.test/api/v1/risk-audit/export?to=2026-08-27T00%3A00%3A00Z&from=2026-08-20T00%3A00%3A00Z&kind=audit", {
      headers: { authorization: "must-not-forward" },
    }));
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/risk-audit/export",
      path: "/internal/v1/risk-audit/export",
      query: "from=2026-08-20T00%3A00%3A00Z&kind=audit&to=2026-08-27T00%3A00%3A00Z",
    }, { method: "GET", actor, headers: { accept: "application/x-ndjson" } });
  });

  it("forwards an empty, versioned manual-safe retry", async () => {
    const { POST } = await import("../../../app/api/v1/jobs/[jobId]/retry/route");
    await POST(writeRequest(`/api/v1/jobs/${jobId}/retry`, {}, {
      "idempotency-key": "retry-job-1", "if-match": '"7"', authorization: "must-not-forward",
    }));
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toEqual({ template: "/internal/v1/jobs/{job_id}/retry", path: `/internal/v1/jobs/${jobId}/retry` });
    expect(init).toMatchObject({
      method: "POST", actor, headers: { "content-type": "application/json", "idempotency-key": "retry-job-1", "if-match": '"7"' },
    });
    expect(new TextDecoder().decode(init.body)).toBe("{}");
  });

  it("forwards only a matching versioned drawdown acknowledgement", async () => {
    const { POST } = await import("../../../app/api/v1/risk/drawdown-lock/acknowledge/route");
    await POST(writeRequest("/api/v1/risk/drawdown-lock/acknowledge", { expected_event_seq: 12 }, {
      "idempotency-key": "ack-lock-1", "if-match": '"12"',
    }));
    const [, init] = engineFetch.mock.calls[0];
    expect(init).toMatchObject({
      method: "POST", actor, headers: { "content-type": "application/json", "idempotency-key": "ack-lock-1", "if-match": '"12"' },
    });
    expect(new TextDecoder().decode(init.body)).toBe("{\"expected_event_seq\":12}");
  });

  it.each([
    { path: `/api/v1/jobs/not-a-uuid/retry`, body: {}, key: "retry-1", version: '"1"' },
    { path: `/api/v1/jobs/${jobId}/retry`, body: { force: true }, key: "retry-1", version: '"1"' },
    { path: "/api/v1/risk/drawdown-lock/acknowledge", body: { expected_event_seq: 0 }, key: "ack-1", version: '"0"' },
    { path: "/api/v1/risk/drawdown-lock/acknowledge", body: { expected_event_seq: 2 }, key: "ack-1", version: '"1"' },
  ])("rejects unsafe recovery input before Engine", async ({ path, body, key, version }) => {
    const route = path.includes("/jobs/")
      ? await import("../../../app/api/v1/jobs/[jobId]/retry/route")
      : await import("../../../app/api/v1/risk/drawdown-lock/acknowledge/route");
    expect((await route.POST(writeRequest(path, body, { "idempotency-key": key, "if-match": version }))).status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });
});

function writeRequest(path: string, body: unknown, headers: Record<string, string>): Request {
  return new Request(`https://nexus.test${path}`, {
    method: "POST",
    headers: { origin: "https://nexus.test", "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}
