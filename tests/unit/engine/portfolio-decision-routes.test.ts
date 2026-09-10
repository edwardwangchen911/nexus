import { beforeEach, describe, expect, it, vi } from "vitest";

import { rebuildAllowedEngineResponse } from "../../../app/lib/engine/errors";

const actor = {
  userId: "allowed-user",
  requestId: "0198f7a0-1111-7777-8111-111111111111",
};
const engineFetch = vi.hoisted(() => vi.fn());
const runId = "0198d500-1000-7000-8000-000000000241";
const portfolioId = "0198d500-1000-7000-8000-000000000341";
const snapshotId = "0198d500-1000-7000-8000-000000000041";
const reportId = "0198d500-1000-7000-8000-000000000141";
const expectedHash = "b".repeat(64);

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

vi.mock("../../../app/lib/engine/client", () => ({
  engineFetch,
  sanitizeEngineResponse: vi.fn(async (response: Response) => response),
}));

function payload() {
  return {
    schema_version: "portfolio_decision_create_v1",
    portfolio_snapshot_id: snapshotId,
    diagnostic_report_id: reportId,
    expected_scenario_result_hash: expectedHash,
    scenario_request: {
      schema_version: "portfolio_scenario_request_v1",
      portfolio_snapshot_id: snapshotId,
      diagnostic_report_id: reportId,
      as_of: "2026-09-10T07:59:00Z",
      constraints: {
        schema_version: "adjustment_scenario_constraints_v1",
        objective: "reduce_security_concentration",
        max_turnover: "0.10",
        minimum_cash: "0.10",
        universe_policy: "current_holdings_only",
        allow_new_positions: false,
      },
    },
  };
}

function runEnvelope(overrides: Record<string, unknown> = {}) {
  return {
    data: {
      schema_version: "portfolio_decision_run_v1",
      run_id: runId,
      portfolio_id: portfolioId,
      portfolio_snapshot_id: snapshotId,
      diagnostic_report_id: reportId,
      decision_time: "2026-09-10T08:00:00Z",
      scenario_request_hash: "a".repeat(64),
      expected_scenario_result_hash: expectedHash,
      objective: "reduce_security_concentration",
      status: "prepared",
      reserved_cost_usd: "0.01",
      settled_cost_usd: null,
      input_tokens: null,
      output_tokens: null,
      output: null,
      citations: [],
      execution_locked: true,
      ...overrides,
    },
    meta: {
      request_id: actor.requestId,
      schema_version: "1",
      as_of: "2026-09-10T08:00:00Z",
    },
  };
}

function completedRunEnvelope() {
  const citationId = "d".repeat(64);
  return runEnvelope({
    status: "completed",
    settled_cost_usd: "0.004",
    input_tokens: 80,
    output_tokens: 40,
    output: {
      status: "completed",
      scenario_result_hash: expectedHash,
      error_code: null,
      intent: {
        schema_version: "portfolio_research_intent_v1",
        run_id: runId,
        portfolio_id: portfolioId,
        portfolio_snapshot_id: snapshotId,
        diagnostic_report_id: reportId,
        decision_time: "2026-09-10T08:00:00Z",
        assessment: "explore_adjustment",
        objective: "reduce_security_concentration",
        horizon: "medium_term",
        confidence: "medium",
        rationale: [{ text: "Concentration improves.", citation_ids: [citationId] }],
        invalidation_conditions: [{
          signal: "portfolio.scenario_objective_status",
          operator: "eq",
          threshold: "unchanged",
          unit: "state",
          citation_ids: [citationId],
        }],
        limitations: [{ code: "estimated_cost", text: "Estimated cost remains unknown." }],
        scenario_result_hash: expectedHash,
        citation_ids: [citationId],
        execution_locked: true,
      },
    },
    citations: [{
      citation_id: citationId,
      evidence: {
        source: "portfolio_diagnostic",
        object_id: reportId,
        content_hash: "e".repeat(64),
        location: "memory://portfolio-diagnostic",
        available_at: "2026-09-10T07:59:00Z",
      },
    }],
  });
}

function createRequest(value: unknown) {
  return new Request("https://nexus.test/api/v1/portfolio-decision-runs", {
    method: "POST",
    headers: {
      origin: "https://nexus.test",
      "content-type": "application/json",
      "idempotency-key": "decision-once",
    },
    body: JSON.stringify(value),
  });
}

function readRequest(scope: Partial<Record<
  "portfolio_snapshot_id" | "diagnostic_report_id" | "expected_scenario_result_hash",
  string
>> = {}) {
  const query = new URLSearchParams({
    portfolio_snapshot_id: snapshotId,
    diagnostic_report_id: reportId,
    expected_scenario_result_hash: expectedHash,
    ...scope,
  });
  return new Request(`https://nexus.test/api/v1/portfolio-decision-runs/${runId}?${query}`);
}

function latestRequest(scope: Partial<Record<
  "portfolio_snapshot_id" | "diagnostic_report_id" | "expected_scenario_result_hash",
  string
>> = {}) {
  const query = new URLSearchParams({
    portfolio_snapshot_id: snapshotId,
    diagnostic_report_id: reportId,
    expected_scenario_result_hash: expectedHash,
    ...scope,
  });
  return new Request(`https://nexus.test/api/v1/portfolio-decision-runs/latest?${query}`);
}

describe("portfolio decision BFF routes", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json(runEnvelope(), { status: 202 }));
  });

  it("validates the closed create request and forwards its exact bytes", async () => {
    const { POST } = await import("../../../app/api/v1/portfolio-decision-runs/route");
    const value = payload();
    const result = await POST(createRequest(value));

    expect(result.status).toBe(202);
    expect(result.headers.get("location")).toBe(`/api/v1/portfolio-decision-runs/${runId}`);
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toBe("/internal/v1/portfolio-decision-runs");
    expect(new TextDecoder().decode(init.body)).toBe(JSON.stringify(value));
    expect(init.headers["idempotency-key"]).toBe("decision-once");
  });

  it("rejects weights, security fields, extras, and scope drift before Engine", async () => {
    const { POST } = await import("../../../app/api/v1/portfolio-decision-runs/route");
    for (const value of [
      { ...payload(), weight: "1" },
      { ...payload(), security: "AAPL" },
      { ...payload(), scenario_request: { ...payload().scenario_request, positions: [] } },
      { ...payload(), diagnostic_report_id: runId },
    ]) {
      engineFetch.mockClear();
      expect((await POST(createRequest(value))).status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("binds GET to run and caller-provided scenario scope", async () => {
    engineFetch.mockResolvedValueOnce(Response.json(runEnvelope()));
    const { GET } = await import("../../../app/api/v1/portfolio-decision-runs/[runId]/route");
    const result = await GET(readRequest());

    expect(result.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/portfolio-decision-runs/{run_id}",
      path: `/internal/v1/portfolio-decision-runs/${runId}`,
    }, { method: "GET", actor });

    for (const scope of [
      { portfolio_snapshot_id: runId },
      { diagnostic_report_id: runId },
      { expected_scenario_result_hash: "c".repeat(64) },
    ]) {
      engineFetch.mockReset();
      engineFetch.mockResolvedValueOnce(Response.json(runEnvelope()));
      const rejected = await GET(readRequest(scope));
      expect(rejected.status).toBe(500);
    }
  });

  it("rejects missing, duplicate, or extra GET scope before Engine", async () => {
    const { GET } = await import("../../../app/api/v1/portfolio-decision-runs/[runId]/route");
    for (const query of [
      "",
      `?portfolio_snapshot_id=${snapshotId}&diagnostic_report_id=${reportId}`,
      `?portfolio_snapshot_id=${snapshotId}&portfolio_snapshot_id=${snapshotId}&diagnostic_report_id=${reportId}&expected_scenario_result_hash=${expectedHash}`,
      `?portfolio_snapshot_id=${snapshotId}&diagnostic_report_id=${reportId}&expected_scenario_result_hash=${expectedHash}&extra=1`,
    ]) {
      engineFetch.mockClear();
      const result = await GET(new Request(`https://nexus.test/api/v1/portfolio-decision-runs/${runId}${query}`));
      expect(result.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("discovers the latest persisted run through an exact read-only scope", async () => {
    engineFetch.mockResolvedValueOnce(Response.json(runEnvelope()));
    const { GET } = await import("../../../app/api/v1/portfolio-decision-runs/latest/route");
    const result = await GET(latestRequest());

    expect(result.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledWith({
      template: "/internal/v1/portfolio-decision-runs/latest",
      path: "/internal/v1/portfolio-decision-runs/latest",
      query: new URLSearchParams({
        portfolio_snapshot_id: snapshotId,
        diagnostic_report_id: reportId,
        expected_scenario_result_hash: expectedHash,
      }).toString(),
    }, { method: "GET", actor });
    expect(engineFetch.mock.calls.some(([, init]) => init?.method === "POST")).toBe(false);

    engineFetch.mockReset();
    engineFetch.mockResolvedValueOnce(Response.json(runEnvelope({
      expected_scenario_result_hash: "c".repeat(64),
    })));
    expect((await GET(latestRequest())).status).toBe(500);

    engineFetch.mockReset();
    engineFetch.mockResolvedValueOnce(Response.json(runEnvelope({ status: "completed" })));
    expect((await GET(latestRequest())).status).toBe(500);

    for (const query of [
      `portfolio_snapshot_id=${snapshotId}&diagnostic_report_id=${reportId}`,
      `portfolio_snapshot_id=${snapshotId}&portfolio_snapshot_id=${snapshotId}&diagnostic_report_id=${reportId}&expected_scenario_result_hash=${expectedHash}`,
      `portfolio_snapshot_id=${snapshotId}&diagnostic_report_id=${reportId}&expected_scenario_result_hash=${expectedHash}&extra=1`,
    ]) {
      engineFetch.mockClear();
      const rejected = await GET(new Request(
        `https://nexus.test/api/v1/portfolio-decision-runs/latest?${query}`,
      ));
      expect(rejected.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("admits only the generated locked response and stable decision errors", async () => {
    expect(rebuildAllowedEngineResponse(
      202,
      runEnvelope(),
      new Headers({ location: `/api/v1/portfolio-decision-runs/${runId}` }),
      undefined,
      "/internal/v1/portfolio-decision-runs",
      "POST",
    )).not.toBeNull();

    expect(rebuildAllowedEngineResponse(
      200,
      runEnvelope({ execution_locked: false }),
      new Headers(),
      undefined,
      `/internal/v1/portfolio-decision-runs/${runId}`,
      "GET",
    )).toBeNull();

    expect(rebuildAllowedEngineResponse(
      200,
      runEnvelope({ status: "completed" }),
      new Headers(),
      undefined,
      "/internal/v1/portfolio-decision-runs/latest",
      "GET",
    )).toBeNull();

    expect(rebuildAllowedEngineResponse(
      200,
      completedRunEnvelope(),
      new Headers(),
      undefined,
      "/internal/v1/portfolio-decision-runs/latest",
      "GET",
    )).not.toBeNull();
    expect(rebuildAllowedEngineResponse(
      200,
      runEnvelope({
        status: "failed",
        output: {
          status: "failed",
          scenario_result_hash: null,
          intent: null,
          error_code: "PORTFOLIO_DECISION_MODEL_FAILED",
        },
      }),
      new Headers(),
      undefined,
      "/internal/v1/portfolio-decision-runs/latest",
      "GET",
    )).not.toBeNull();

    const error = {
      error: {
        code: "PORTFOLIO_DECISION_BUDGET_EXHAUSTED",
        message: "raw provider text",
        request_id: actor.requestId,
        details: { provider_response: "secret" },
      },
    };
    const safe = rebuildAllowedEngineResponse(409, error);
    expect(await safe?.json()).toEqual({
      error: {
        code: "PORTFOLIO_DECISION_BUDGET_EXHAUSTED",
        message: "The portfolio decision budget is exhausted.",
        request_id: actor.requestId,
        details: {},
      },
    });
  });
});
