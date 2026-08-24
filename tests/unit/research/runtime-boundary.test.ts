import { describe, expect, it } from "vitest";

import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { briefEnvelope, instrumentId, researchData, researchEnvelope, runId, sessionDate } from "./fixtures";

describe("research Engine response boundary", () => {
  it("accepts closed research and brief contracts", async () => {
    const research = await sanitizeEngineResponse(json(researchEnvelope()), undefined, `/internal/v1/research-runs/${runId}`);
    expect(research.status).toBe(200);

    const brief = await sanitizeEngineResponse(json(briefEnvelope()), undefined, `/internal/v1/briefs/${sessionDate}`);
    expect(brief.status).toBe(200);
  });

  it.each([
    { credential: "must-not-escape" },
    { account_id: "must-not-escape" },
    { broker_account_id: "must-not-escape" },
    { exact_total_assets: "90000" },
  ])("rejects added sensitive field %j", async (extra) => {
    const data = researchData(extra);
    const response = await sanitizeEngineResponse(json(researchEnvelope(data)), undefined, `/internal/v1/research-runs/${runId}`);
    expect(response.status).toBe(500);
  });

  it("binds returned UUIDs and session date to the requested resource", async () => {
    expect((await sanitizeEngineResponse(json(researchEnvelope()), undefined, `/internal/v1/research-runs/${runId.toUpperCase()}`)).status).toBe(200);
    const mismatchedRun = researchData({ run_id: "0198d600-1000-7000-8000-000000001598" });
    expect((await sanitizeEngineResponse(json(researchEnvelope(mismatchedRun)), undefined, `/internal/v1/research-runs/${runId}`)).status).toBe(500);

    const mismatchedInstrument = researchData({ instrument_id: "0198d600-1000-7000-8000-000000001598" });
    expect((await sanitizeEngineResponse(json(researchEnvelope(mismatchedInstrument)), undefined, `/internal/v1/instruments/${instrumentId}/research`)).status).toBe(500);

    const mismatchedBrief = briefEnvelope({ ...briefEnvelope().data, session_date: "2026-08-23" });
    expect((await sanitizeEngineResponse(json(mismatchedBrief), undefined, `/internal/v1/briefs/${sessionDate}`)).status).toBe(500);
  });

  it("rejects oversized evidence, role-vote and brief collections", async () => {
    expect((await sanitizeEngineResponse(json(researchEnvelope(researchData({ evidence: Array.from({ length: 101 }, () => researchData().evidence[0]) }))), undefined, `/internal/v1/research-runs/${runId}`)).status).toBe(500);
    expect((await sanitizeEngineResponse(json(researchEnvelope(researchData({ role_votes: Array.from({ length: 9 }, () => researchData().role_votes[0]) }))), undefined, `/internal/v1/research-runs/${runId}`)).status).toBe(500);
    expect((await sanitizeEngineResponse(json(briefEnvelope({ ...briefEnvelope().data, candidates: Array.from({ length: 21 }, () => briefEnvelope().data.candidates[0]) })), undefined, `/internal/v1/briefs/${sessionDate}`)).status).toBe(500);
  });

  it("preserves a safe budget-exhausted recovery code from Engine", async () => {
    const response = await sanitizeEngineResponse(new Response(JSON.stringify({
      error: {
        code: "BUDGET_EXHAUSTED",
        message: "upstream details must be redacted",
        request_id: "0198d600-1000-7000-8000-000000001597",
        details: { daily_spend: "must-not-escape" },
      },
    }), { status: 409, headers: { "content-type": "application/json" } }), undefined, "/internal/v1/research-runs");

    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({
      error: {
        code: "BUDGET_EXHAUSTED",
        message: "The configured budget is exhausted.",
        request_id: "0198d600-1000-7000-8000-000000001597",
        details: {},
      },
    });
  });
});

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { status: 200, headers: { "content-type": "application/json" } });
}
