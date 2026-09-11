import { beforeEach, describe, expect, it, vi } from "vitest";

import { rebuildAllowedEngineResponse } from "../../../app/lib/engine/errors";

const actor = {
  userId: "allowed-user",
  requestId: "0198f7a0-1111-7777-8111-111111111111",
};
const engineFetch = vi.hoisted(() => vi.fn());
const snapshotId = "0198d500-1000-7000-8000-000000000041";
const reportId = "0198d500-1000-7000-8000-000000000141";

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

vi.mock("../../../app/lib/engine/client", () => ({
  engineFetch,
  sanitizeEngineResponse: vi.fn(async (response: Response) => response),
}));

function request(value: unknown) {
  return new Request("https://nexus.test/api/v1/portfolio-scenarios/evaluate", {
    method: "POST",
    headers: {
      origin: "https://nexus.test",
      "content-type": "application/json",
      "idempotency-key": "scenario-once",
    },
    body: JSON.stringify(value),
  });
}

function payload() {
  return {
    schema_version: "portfolio_scenario_request_v1",
    portfolio_snapshot_id: snapshotId,
    diagnostic_report_id: reportId,
    as_of: "2026-08-24T03:59:00Z",
    constraints: {
      schema_version: "adjustment_scenario_constraints_v1",
      objective: "reduce_security_concentration",
      max_turnover: "0.1",
      minimum_cash: "0.1",
      universe_policy: "current_holdings_only",
      allow_new_positions: false,
    },
  };
}

function response(overrides: Record<string, unknown> = {}) {
  return Response.json({
    data: {
      portfolio_snapshot_id: snapshotId,
      diagnostic_report_id: reportId,
      valuation_time: "2026-08-24T03:59:00Z",
      execution_locked: true,
      ...overrides,
    },
    meta: {
      request_id: actor.requestId,
      schema_version: "1",
      as_of: "2026-08-24T03:59:00Z",
    },
  });
}

function scenarioEnvelope() {
  const known = (value: string) => ({ status: "known", value, reason: null });
  const metrics = {
    security_top1: known("1"),
    security_top3: known("1"),
    security_hhi: known("1"),
    cash_weight: known("0.2"),
    one_way_turnover: known("0"),
    estimated_cost: { status: "unknown", value: null, reason: "COST_EVIDENCE_INCOMPLETE" },
  };
  return {
    data: {
      schema_version: "portfolio_scenario_evaluation_v2",
      request_hash: "a".repeat(64),
      result_hash: "b".repeat(64),
      portfolio_snapshot_id: snapshotId,
      diagnostic_report_id: reportId,
      diagnostic_input_hash: "c".repeat(64),
      valuation_time: "2026-08-24T03:59:00Z",
      valuation_scope: "currency:USD",
      objective: "reduce_security_concentration",
      analysis_status: "partial",
      analysis_comparable: true,
      execution_eligible: false,
      objective_status: "infeasible",
      baseline: metrics,
      candidate: metrics,
      positions: [{
        instrument_id: "0198d500-1000-7000-8000-000000000701",
        current_weight: "0.8",
        target_weight: "0.8",
        action: "maintain",
        reason_code: "BASELINE_WEIGHT",
      }],
      cash_current_weight: "0.2",
      cash_target_weight: "0.2",
      hard_constraint_reasons: ["STRUCTURAL_CONCENTRATION_FLOOR"],
      unknown_metrics: ["estimated_cost"],
      cost_breakdown: {
        currency: "USD", valuation_time: "2026-08-24T03:59:00Z", basis: "portfolio_value_ratio",
        evidence_version: null, evidence_hash: null, available_at: null,
        commission: { status: "unknown", value: null, reason: "BROKER_COMMISSION_SCHEDULE_UNAVAILABLE" },
        spread: { status: "unknown", value: null, reason: "BID_ASK_EVIDENCE_UNAVAILABLE" },
        slippage: { status: "unknown", value: null, reason: "SLIPPAGE_MODEL_UNAVAILABLE" },
        regulatory_fees: { status: "unknown", value: null, reason: "REGULATORY_FEE_SCHEDULE_UNAVAILABLE" },
        tax_impact: { status: "unknown", value: null, reason: "TAX_DATA_UNAVAILABLE" },
        total_estimated_cost_ex_tax: { status: "unknown", value: null, reason: "COST_EVIDENCE_INCOMPLETE" },
      },
      execution_locked: true,
    },
    meta: {
      request_id: actor.requestId,
      schema_version: "1",
      as_of: "2026-08-24T03:59:00Z",
    },
  };
}

describe("portfolio scenario BFF route", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(response());
  });

  it("forwards exact bytes and the one idempotency identity", async () => {
    const { POST } = await import("../../../app/api/v1/portfolio-scenarios/evaluate/route");
    const value = payload();
    const body = JSON.stringify(value);
    const result = await POST(request(value));

    expect(result.status).toBe(200);
    expect(engineFetch).toHaveBeenCalledTimes(1);
    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toBe("/internal/v1/portfolio-scenarios/evaluate");
    expect(new TextDecoder().decode(init.body)).toBe(body);
    expect(init.headers).toEqual({
      "content-type": "application/json",
      "idempotency-key": "scenario-once",
    });
  });

  it("rejects client weights and noncanonical scenario fields before Engine", async () => {
    const { POST } = await import("../../../app/api/v1/portfolio-scenarios/evaluate/route");
    for (const value of [
      { ...payload(), positions: [{ instrument_id: snapshotId, weight: "1" }] },
      { ...payload(), constraints: { ...payload().constraints, allow_new_positions: true } },
      { ...payload(), constraints: { ...payload().constraints, max_turnover: 0.1 } },
    ]) {
      engineFetch.mockClear();
      const result = await POST(request(value));
      expect(result.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("preserves the shared 38-place ratio boundary without accepting out-of-range values", async () => {
    const { POST } = await import("../../../app/api/v1/portfolio-scenarios/evaluate/route");
    const precise = payload();
    precise.constraints.max_turnover = `0.${"0".repeat(37)}1`;
    expect((await POST(request(precise))).status).toBe(200);
    expect(engineFetch).toHaveBeenCalledTimes(1);

    for (const invalid of ["-0.1", "1.1", `0.${"0".repeat(38)}1`]) {
      engineFetch.mockClear();
      const value = payload();
      value.constraints.max_turnover = invalid;
      expect((await POST(request(value))).status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });

  it("fails closed on response identity, time, or execution-lock drift", async () => {
    const { POST } = await import("../../../app/api/v1/portfolio-scenarios/evaluate/route");
    for (const drift of [
      { diagnostic_report_id: "0198d500-1000-7000-8000-000000000142" },
      { valuation_time: "2026-08-24T04:00:00Z" },
      { execution_locked: false },
    ]) {
      engineFetch.mockReset();
      engineFetch.mockResolvedValueOnce(response(drift));
      const result = await POST(request(payload()));
      expect(result.status).toBe(500);
      expect(await result.json()).toMatchObject({ error: { code: "INTERNAL_ERROR" } });
    }
  });

  it("accepts only the generated closed, permanently locked Engine response", async () => {
    const envelope = scenarioEnvelope();
    const accepted = rebuildAllowedEngineResponse(
      200,
      envelope,
      new Headers(),
      undefined,
      "/internal/v1/portfolio-scenarios/evaluate",
      "POST",
    );
    expect(await accepted?.json()).toEqual(envelope);

    const unlocked = scenarioEnvelope();
    unlocked.data.execution_locked = false;
    expect(rebuildAllowedEngineResponse(
      200,
      unlocked,
      new Headers(),
      undefined,
      "/internal/v1/portfolio-scenarios/evaluate",
      "POST",
    )).toBeNull();

    const notComparable = scenarioEnvelope();
    notComparable.data.analysis_comparable = false;
    expect(rebuildAllowedEngineResponse(
      200,
      notComparable,
      new Headers(),
      undefined,
      "/internal/v1/portfolio-scenarios/evaluate",
      "POST",
    )).toBeNull();

    const executionEligible = scenarioEnvelope();
    executionEligible.data.execution_eligible = true;
    expect(rebuildAllowedEngineResponse(
      200,
      executionEligible,
      new Headers(),
      undefined,
      "/internal/v1/portfolio-scenarios/evaluate",
      "POST",
    )).toBeNull();

    const widened = scenarioEnvelope();
    Object.assign(widened.data.positions[0], { weight: "1" });
    expect(rebuildAllowedEngineResponse(
      200,
      widened,
      new Headers(),
      undefined,
      "/internal/v1/portfolio-scenarios/evaluate",
      "POST",
    )).toBeNull();

    const increased = scenarioEnvelope();
    increased.data.positions[0].target_weight = "0.9";
    increased.data.positions[0].action = "reduce";
    increased.data.candidate = {
      ...increased.data.candidate,
      cash_weight: { status: "known", value: "0.1", reason: null },
    };
    increased.data.cash_target_weight = "0.1";
    expect(rebuildAllowedEngineResponse(
      200,
      increased,
      new Headers(),
      undefined,
      "/internal/v1/portfolio-scenarios/evaluate",
      "POST",
    )).toBeNull();
  });

  it("preserves a redacted stable 409 for broken frozen spread evidence", async () => {
    const accepted = rebuildAllowedEngineResponse(
      409,
      {
        error: {
          code: "SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID",
          message: "upstream evidence detail must not escape",
          request_id: "0198f7a0-1111-7777-8111-111111111111",
          details: { quote_id: "must-not-escape" },
        },
      },
      new Headers(),
      undefined,
      "/internal/v1/portfolio-scenarios/evaluate",
      "POST",
    );

    expect(accepted?.status).toBe(409);
    expect(await accepted?.json()).toEqual({
      error: {
        code: "SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID",
        message: "Frozen scenario cost evidence could not be verified.",
        request_id: "0198f7a0-1111-7777-8111-111111111111",
        details: {},
      },
    });
  });

  it("fails closed when cost evidence semantics are tampered", () => {
    const cases = [
      () => {
        const value = scenarioEnvelope();
        value.data.cost_breakdown.valuation_time = "2026-08-24T03:58:00Z";
        return value;
      },
      () => {
        const value = scenarioEnvelope();
        Object.assign(value.data.cost_breakdown, { evidence_version: "broker_cost_evidence_v1" });
        return value;
      },
      () => {
        const value = scenarioEnvelope();
        Object.assign(value.data.cost_breakdown, {
          commission: { status: "known", value: "0.001", reason: null },
        });
        return value;
      },
      () => {
        const value = scenarioEnvelope();
        Object.assign(value.data.candidate, {
          estimated_cost: { status: "unknown", value: null, reason: "DIFFERENT_REASON" },
        });
        return value;
      },
      () => {
        const value = scenarioEnvelope();
        Object.assign(value.data.cost_breakdown, {
          evidence_version: "broker_cost_evidence_v1",
          evidence_hash: "e".repeat(64),
          available_at: "2026-08-24T03:59:00Z",
          commission: { status: "known", value: "0.001", reason: null },
          spread: { status: "known", value: "0.002", reason: null },
          slippage: { status: "known", value: "0.003", reason: null },
          regulatory_fees: { status: "known", value: "0.004", reason: null },
          total_estimated_cost_ex_tax: { status: "known", value: "0.009", reason: null },
        });
        Object.assign(value.data.candidate, {
          estimated_cost: { status: "known", value: "0.009", reason: null },
        });
        return value;
      },
    ];

    for (const build of cases) {
      expect(rebuildAllowedEngineResponse(
        200,
        build(),
        new Headers(),
        undefined,
        "/internal/v1/portfolio-scenarios/evaluate",
        "POST",
      )).toBeNull();
    }
  });
});

describe("portfolio scenario selection BFF route", () => {
  it("forwards one exact locked selection and restores it by result hash", async () => {
    const resultHash = "b".repeat(64);
    const value = {
      schema_version: "portfolio_scenario_selection_v1",
      scenario_request: payload(),
      choice: "accepted_for_analysis",
      version: 0,
    };
    const envelope = {
      data: {
        schema_version: "portfolio_scenario_selection_v1",
        portfolio_snapshot_id: snapshotId,
        diagnostic_report_id: reportId,
        scenario_result_hash: resultHash,
        choice: "accepted_for_analysis",
        version: 1,
        selected_at: "2026-09-11T08:00:00Z",
        execution_eligible: false,
        execution_locked: true,
        execution_authorized: false,
      },
    };
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json(envelope));
    const route = await import("../../../app/api/v1/portfolio-scenarios/[resultHash]/selection/route");
    const url = `https://nexus.test/api/v1/portfolio-scenarios/${resultHash}/selection`;
    const putRequest = new Request(url, {
      method: "PUT",
      headers: {
        origin: "https://nexus.test",
        "content-type": "application/json",
        "idempotency-key": "selection-once",
        "if-match": '"0"',
      },
      body: JSON.stringify(value),
    });

    expect((await route.PUT(putRequest)).status).toBe(200);
    expect(engineFetch.mock.calls[0][0]).toEqual({
      template: "/internal/v1/portfolio-scenarios/{scenario_result_hash}/selection",
      path: `/internal/v1/portfolio-scenarios/${resultHash}/selection`,
    });
    expect(new TextDecoder().decode(engineFetch.mock.calls[0][1].body)).toBe(JSON.stringify(value));
    expect(engineFetch.mock.calls[0][1].headers).toEqual({
      "content-type": "application/json",
      "idempotency-key": "selection-once",
      "if-match": '"0"',
    });

    engineFetch.mockClear();
    engineFetch.mockResolvedValueOnce(Response.json(envelope));
    expect((await route.GET(new Request(url))).status).toBe(200);
    expect(engineFetch.mock.calls[0][0]).toEqual({
      template: "/internal/v1/portfolio-scenarios/{scenario_result_hash}/selection",
      path: `/internal/v1/portfolio-scenarios/${resultHash}/selection`,
    });
  });

  it("admits only a locked upstream selection", async () => {
    const resultHash = "b".repeat(64);
    const data = {
      schema_version: "portfolio_scenario_selection_v1",
      portfolio_snapshot_id: snapshotId,
      diagnostic_report_id: reportId,
      scenario_result_hash: resultHash,
      choice: "accepted_for_analysis",
      version: 1,
      selected_at: "2026-09-11T08:00:00Z",
      execution_eligible: false,
      execution_locked: true,
      execution_authorized: true,
    };

    const accepted = rebuildAllowedEngineResponse(
      200,
      { data: { ...data, execution_authorized: false }, meta: {
        request_id: "0198d500-1000-7000-8000-000000000099",
        schema_version: "1",
        as_of: "2026-09-11T08:00:00Z",
      } },
      new Headers(),
      undefined,
      `/internal/v1/portfolio-scenarios/${resultHash}/selection`,
      "GET",
    );
    expect(accepted).not.toBeNull();
    expect((await accepted!.json()).data.execution_locked).toBe(true);

    expect(rebuildAllowedEngineResponse(
      200,
      { data, meta: {
        request_id: "0198d500-1000-7000-8000-000000000099",
        schema_version: "1",
        as_of: "2026-09-11T08:00:00Z",
      } },
      new Headers(),
      undefined,
      `/internal/v1/portfolio-scenarios/${resultHash}/selection`,
      "GET",
    )).toBeNull();
  });
});
