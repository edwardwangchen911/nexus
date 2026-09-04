import { describe, expect, it } from "vitest";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { decisionTime, instrumentId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope, runId } from "./fixtures";

function inputs() {
  return {
    display_symbol: "AAPL", filings: [], facts: [], total_facts: 0, truncated: false,
    company_profiles: [{
      evidence_ref_id: instrumentId, source: "fmp_company_profile", revision: "a".repeat(64),
      content_hash: "a".repeat(64), raw_content_hash: "b".repeat(64), receipt_content_hash: "c".repeat(64),
      symbol: "AAPL", company_name: null, description: "Synthetic company background", cik: null,
      isin: null, cusip: null, industry: "Vendor Industry", sector: null,
      source_url: "https://financialmodelingprep.com/stable/profile?symbol=AAPL",
      received_at: decisionTime, available_at: decisionTime, source_published_at: null,
      valid_until: null, sector_taxonomy: null, revision_semantics: "local_content_hash",
      sector_risk_eligible: false, financial_fact_eligible: false, price_authority_eligible: false,
      historical_backtest_eligible: false, execution_locked: true,
    }],
    macro_observations: [{
      observation_id: instrumentId, evidence_ref_id: runId, calendar_evidence_ref_id: instrumentId,
      source: "fred", revision: "synthetic-local-revision", content_hash: "d".repeat(64),
      raw_content_hash: "e".repeat(64), series: "DGS10", observed_on: "2026-08-26",
      vintage: "2026-08-28", value: "4.25", unit: "Percent", received_at: decisionTime,
      available_at: decisionTime, revision_semantics: "local_content_hash",
      historical_backtest_eligible: false, execution_locked: true,
    }],
  };
}

async function response(value: object, run = false) {
  const envelope = run
    ? researchEnvelope(researchData({ inputs: value }))
    : researchAdmissionEnvelope(researchAdmissionData({ inputs: value }));
  const path = run ? `/research-runs/${runId}`
    : `/instruments/${instrumentId}/research-admission?as_of=${encodeURIComponent(decisionTime)}`;
  return sanitizeEngineResponse(
    new Response(JSON.stringify(envelope), { headers: { "content-type": "application/json" } }),
    `https://nexus.test/api/v1${path}`,
  );
}

describe("frozen provider inputs runtime boundary", () => {
  it.each([false, true])("preserves full values and provenance on exact run=%s", async (run) => {
    const value = inputs();
    const result = await response(value, run);
    expect(result.status).toBe(200);
    expect((await result.json()).data.inputs).toEqual(value);
  });

  it("accepts older responses with neither provider field", async () => {
    const value = { display_symbol: null, filings: [], facts: [], total_facts: 0, truncated: false };
    const result = await response(value);
    expect(result.status).toBe(200);
    expect((await result.json()).data.inputs).toEqual(value);
  });

  it.each(["0", "-0.125", "0.000000000001"])("preserves signed source Decimal %s", async (value) => {
    const data = inputs();
    data.macro_observations[0].value = value;
    const result = await response(data);
    expect(result.status).toBe(200);
    expect((await result.json()).data.inputs.macro_observations[0].value).toBe(value);
  });

  it.each(["profile-authority", "macro-authority", "unlock", "private", "decimal", "url", "calendar", "too-many"])("rejects unsafe %s", async (mutation) => {
    const data = inputs();
    if (mutation === "profile-authority") data.company_profiles[0].sector_risk_eligible = true;
    if (mutation === "macro-authority") data.macro_observations[0].historical_backtest_eligible = true;
    if (mutation === "unlock") data.company_profiles[0].execution_locked = false;
    if (mutation === "private") Object.assign(data.company_profiles[0], { account_id: "private-canary" });
    if (mutation === "decimal") data.macro_observations[0].value = "1e3";
    if (mutation === "url") data.company_profiles[0].source_url += "&apikey=private-canary";
    if (mutation === "calendar") Object.assign(data.macro_observations[0], { calendar_evidence_ref_id: null });
    if (mutation === "too-many") data.macro_observations = Array.from({ length: 9 }, () => data.macro_observations[0]);
    const result = await response(data);
    expect(result.status).toBe(500);
    expect(await result.text()).not.toContain("private-canary");
  });
});
