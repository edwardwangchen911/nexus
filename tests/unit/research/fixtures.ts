import type { components } from "../../../app/lib/generated/schema";

type Research = components["schemas"]["ResearchRunResponse"];
type ResearchAdmission = components["schemas"]["ResearchAdmissionResponse"];
type Brief = components["schemas"]["DailyBriefResponse"];

export const instrumentId = "0198d600-1000-7000-8000-000000001501";
export const dataSnapshotId = "0198d600-1000-7000-8000-000000001502";
export const runId = "0198d600-1000-7000-8000-000000001503";
export const decisionId = "0198d600-1000-7000-8000-000000001504";
export const requestId = "0198d600-1000-7000-8000-000000001599";
export const decisionTime = "2026-08-24T20:00:00Z";
export const sessionDate = "2026-08-24";

export function fundObservationInputs(): components["schemas"]["ResearchInputsSummary"] {
  return {
    display_symbol: "TEST", filings: [], facts: [], total_facts: 0, truncated: false,
    issuer_nav_histories: [], issuer_distributions: [], issuer_fund_observations: [{
      evidence_ref_id: "0198d600-1000-7000-8000-000000001581", source: "ssga_fund_observation",
      revision: "a".repeat(64), content_hash: "a".repeat(64), as_of: "2026-08-20",
      classification_level: "industry", weight_unit: "percent",
      industries: [{ label: "Synthetic industry", weight_percent: "100.000000" }],
      holdings: [
        { name: "Synthetic holding", ticker: "SYN", classification: null, weight_percent: "-0.125678", local_currency: "USD" },
        { name: "Zero holding", ticker: null, classification: null, weight_percent: "0", local_currency: null },
        { name: "Tiny holding", ticker: "TNY", classification: "Synthetic industry", weight_percent: "0.0000001", local_currency: "USD" },
      ],
      total_industries: 1, total_holdings: 3, truncated: false,
      html_url: "https://www.ssga.com/us/en/individual/etfs/synthetic-test",
      workbook_url: "https://www.ssga.com/library-content/products/fund-data/etfs/us/test.xlsx",
      html_content_hash: "b".repeat(64), workbook_content_hash: "c".repeat(64),
      html_received_at: "2026-08-24T18:00:00Z", workbook_received_at: "2026-08-24T18:30:00Z",
      available_at: "2026-08-24T18:30:00Z", source_published_at: null, valid_until: null,
      denominator: null, residual_classification: null,
      historical_backtest_eligible: false, sector_risk_eligible: false,
    }],
  };
}

export function navHistoryInputs(): components["schemas"]["ResearchInputsSummary"] {
  return {
    ...fundObservationInputs(), issuer_fund_observations: [],
    issuer_nav_histories: [{
      evidence_ref_id: "0198d600-1000-7000-8000-000000001582", source: "ssga_nav_history",
      revision: "d".repeat(64), content_hash: "d".repeat(64), currency: "USD",
      currency_basis: "issuer_page_base_currency", nav_unit: "currency_per_share",
      observations: [
        { date: "2026-08-22", nav: "12.3456789", shares_outstanding: null, total_net_assets: null },
        { date: "2026-08-23", nav: "0.0000001", shares_outstanding: "0", total_net_assets: "2500000" },
      ],
      total_observations: 45, truncated: true, coverage_start: "2026-07-10", coverage_end: "2026-08-23",
      date_semantics: "issuer_observation_date_not_verified_trading_session",
      html_url: "https://www.ssga.com/us/en/individual/etfs/synthetic-test",
      workbook_url: "https://www.ssga.com/library-content/products/fund-data/etfs/us/nav-test.xlsx",
      html_content_hash: "e".repeat(64), workbook_content_hash: "f".repeat(64),
      html_received_at: "2026-08-24T18:00:00Z", workbook_received_at: "2026-08-24T18:30:00Z",
      available_at: "2026-08-24T18:30:00Z", source_published_at: null, valid_until: null,
      revision_method: null, split_adjustment: null, reinvestment_method: null,
      historical_backtest_eligible: false, market_price_eligible: false, total_return_eligible: false,
    }],
  };
}

export const evidence = {
  source: "sec",
  object_id: "0000320193-26-000001",
  available_at: "2026-08-24T18:30:00Z",
  location: `memory://sha256/${"a".repeat(64)}#item-7`,
  content_hash: "a".repeat(64),
};

export function researchData(overrides: Record<string, unknown> = {}): Research {
  const condition = {
    signal: "revenue_growth_ttm",
    operator: "lte",
    threshold: "0",
    unit: "ratio",
    evidence_refs: [evidence],
  };
  return {
    run_id: runId,
    instrument_id: instrumentId,
    data_snapshot_id: dataSnapshotId,
    decision_time: decisionTime,
    status: "completed",
    retry_count: 0,
    reserved_cost_usd: "0.120000000000",
    settled_cost_usd: "0.041000000000",
    intent: {
      schema_version: "research_intent_v1",
      decision_id: decisionId,
      instrument_id: instrumentId,
      display_symbol: "AAPL",
      decision_time: decisionTime,
      data_snapshot_id: dataSnapshotId,
      horizon: "medium_term",
      stance: "hold",
      exposure_action: "maintain",
      theses: [{ text: "现金流质量稳定，但当前证据不足以支持提高暴露。", evidence_refs: [evidence] }],
      counter_theses: [{ text: "增长放缓可能压缩估值空间。", evidence_refs: [evidence] }],
      uncertainties: ["下一期管理层指引尚未披露。"],
      invalidation_conditions: [condition],
      entry_conditions: [],
      exit_conditions: [],
      valid_until: "2026-09-24T20:00:00Z",
      quality_status: "complete",
      evidence_audit: "complete",
      role_votes: [
        { role: "orchestrator", vote: "support" },
        { role: "fundamental", vote: "support" },
        { role: "market_state", vote: "abstain" },
        { role: "macro_sector", vote: "abstain" },
        { role: "bull", vote: "support" },
        { role: "bear", vote: "oppose" },
        { role: "evidence_audit", vote: "support" },
        { role: "portfolio", vote: "support" },
      ],
      disagreement: "空头角色认为增长放缓仍未被当前估值充分反映。",
      strategy_version: "baseline_v1",
      model_version: "fixture-reasoning-v1",
      prompt_version: "committee-v1",
      data_version: "research-data-v1",
      evidence_refs: [evidence],
    },
    evidence: [evidence],
    role_votes: [
      { role: "orchestrator", vote: "support" },
      { role: "fundamental", vote: "support" },
      { role: "market_state", vote: "abstain" },
      { role: "macro_sector", vote: "abstain" },
      { role: "bull", vote: "support" },
      { role: "bear", vote: "oppose" },
      { role: "evidence_audit", vote: "support" },
      { role: "portfolio", vote: "support" },
    ],
    disagreement: "空头角色认为增长放缓仍未被当前估值充分反映。",
    invalidation_conditions: [condition],
    quality_status: "complete",
    budget_reason: null,
    abstention_reason: null,
    ...overrides,
  } as Research;
}

export function researchEnvelope(data = researchData()) {
  return {
    data,
    meta: { request_id: requestId, schema_version: "1", as_of: decisionTime },
  };
}

export function synthesisResearchData(
  overrides: Partial<components["schemas"]["SynthesisAssessment"]> = {},
): Research {
  const base = researchData();
  const assessment: components["schemas"]["SynthesisAssessment"] = {
    stance: "watch",
    claims: [{ text: "冻结资料仍不足以支持完整决策。", evidence_refs: [evidence] }],
    reviews: ["bear:0", "evidence_audit:0"].map((claim_id) => ({
      claim_id, status: "accepted", resolved: true,
      rationale: `合成审查理由 ${claim_id}：认可资料缺口。`, evidence_refs: [evidence],
      missing_evidence: [], prevented_conclusion: null,
    })),
    review_conclusion: "sufficient", uncertainties: ["估值依据仍未验证。"], ...overrides,
  };
  return researchData({
    quality_status: "partial", billing_mode: "chatgpt_subscription", input_tokens: 100, output_tokens: 50,
    intent: { ...base.intent!, stance: "watch", exposure_action: "none", quality_status: "partial",
      evidence_audit: "partial", synthesis_assessment: assessment, theses: assessment.claims },
    stage_trace: { status: "verified", evidence_hash: "a".repeat(64), stages: [{
      stage: "synthesis", status: "complete", input_tokens: 100, output_tokens: 50,
      roles: [{ role: "portfolio", claims: assessment.claims,
        reviewed_claim_ids: assessment.reviews.map((review) => review.claim_id), synthesis_assessment: assessment }],
    }] },
  });
}

export function researchAdmissionData(overrides: Record<string, unknown> = {}): ResearchAdmission {
  return {
    eligible: true,
    reason: null,
    instrument_id: instrumentId,
    data_snapshot_id: dataSnapshotId,
    decision_time: decisionTime,
    ...overrides,
  } as ResearchAdmission;
}

export function researchAdmissionEnvelope(data = researchAdmissionData()) {
  return {
    data,
    meta: { request_id: requestId, schema_version: "1", as_of: decisionTime },
  };
}

export function briefData(overrides: Record<string, unknown> = {}): Brief {
  return {
    session_date: sessionDate,
    generated_at: "2026-08-25T01:00:00Z",
    candidates: [{ run_id: runId, instrument_id: instrumentId, status: "completed", stance: "buy_candidate", reason: null }],
    holding_risks: [],
    rejected: [],
    budget_exhausted_watch: [],
    evidence_completeness: { total_runs: 1, complete: 1, partial: 0, failed: 0 },
    disagreements: [{ run_id: runId, instrument_id: instrumentId, summary: "空头角色仍然反对。" }],
    invalidations: [{
      run_id: runId,
      instrument_id: instrumentId,
      conditions: [{ signal: "revenue_growth_ttm", operator: "lte", threshold: "0", unit: "ratio", evidence_refs: [evidence] }],
    }],
    truncated: false,
    ...overrides,
  } as Brief;
}

export function briefEnvelope(data = briefData()) {
  return {
    data,
    meta: { request_id: requestId, schema_version: "1", as_of: "2026-08-25T01:00:00Z" },
  };
}
