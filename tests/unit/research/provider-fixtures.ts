import { fundObservationInputs } from "./fixtures";
import type { components } from "../../../app/lib/generated/schema";

export function companyProfile(): components["schemas"]["ResearchInputCompanyProfile"] {
  return {
    evidence_ref_id: "0198d600-1000-7000-8000-000000001591",
    source: "fmp_company_profile" as const,
    revision: "a".repeat(64), content_hash: "a".repeat(64),
    raw_content_hash: "b".repeat(64), receipt_content_hash: "c".repeat(64),
    symbol: "TEST", company_name: "Synthetic Company", description: "Synthetic company background.",
    cik: "0000000001", isin: null, cusip: null, industry: "Synthetic industry", sector: "Synthetic sector",
    source_url: "https://financialmodelingprep.com/stable/profile?symbol=TEST",
    received_at: "2026-08-24T17:00:00Z", available_at: "2026-08-24T18:00:00Z",
    source_published_at: null, valid_until: null, sector_taxonomy: null,
    revision_semantics: "local_content_hash" as const,
    sector_risk_eligible: false as const, financial_fact_eligible: false as const,
    price_authority_eligible: false as const, historical_backtest_eligible: false as const,
    execution_locked: true as const,
  };
}

export function macroObservation(): components["schemas"]["ResearchInputMacroObservation"] {
  return {
    observation_id: "0198d600-1000-7000-8000-000000001592",
    evidence_ref_id: "0198d600-1000-7000-8000-000000001593",
    calendar_evidence_ref_id: "0198d600-1000-7000-8000-000000001594",
    source: "fred" as const, revision: `local-2026-08-21:2026-08-20:${"d".repeat(64)}`,
    content_hash: "d".repeat(64), raw_content_hash: "e".repeat(64), series: "DGS10",
    observed_on: "2026-08-20", vintage: "2026-08-21", value: "4.25", unit: "Percent",
    received_at: "2026-08-24T17:30:00Z", available_at: "2026-08-24T18:30:00Z",
    revision_semantics: "local_content_hash" as const,
    historical_backtest_eligible: false as const, execution_locked: true as const,
  };
}

export function providerInputs(): components["schemas"]["ResearchInputsSummary"] {
  return {
    ...fundObservationInputs(), issuer_fund_observations: [],
    company_profiles: [companyProfile()], macro_observations: [macroObservation()],
  };
}
