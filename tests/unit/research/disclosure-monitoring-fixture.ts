export const disclosureAt = "2026-10-24T11:30:00Z";
export const disclosureRunId = "0198d600-1000-7000-8000-000000001583";
export const disclosureBatch = "0198d600-1000-7000-8000-000000001580";
export function disclosureTrigger(overrides: Record<string, unknown> = {}) {
  return {
    trigger_id: "0198d600-1000-7000-8000-000000001581", batch_id: disclosureBatch,
    event_key: "sec:0000320193:0000320193-26-000001", checked_at: disclosureAt,
    status: "completed", blocked_reason: null, run_id: disclosureRunId, usage_status: "known",
    notification_status: "published", event_review: "unresolved", ...overrides,
  };
}
export function disclosureMonitoring(overrides: Record<string, unknown> = {}) {
  return {
    observed_at: disclosureAt, state: "valid", coverage_reason: null,
    plan_id: "0198d600-1000-7000-8000-000000001582",
    condition: { forms: ["8-K", "8-K/A"], max_age_seconds: 300, operator: "exists_new",
      purpose: "review", signal: "sec.issuer_disclosure_added", source: "sec", threshold: true,
      time_authority: "frozen_index_vs_local_receipt", unit: "filing" },
    baseline_as_of: "2026-10-24T10:00:00Z", review_due_at: "2026-10-25T10:00:00Z",
    valid_until: "2026-10-25T12:00:00Z", replacement_run_id: null, latest_check: null,
    trigger_count: 1, triggers: [disclosureTrigger()], execution_locked: true, ...overrides,
  };
}
