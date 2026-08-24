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
