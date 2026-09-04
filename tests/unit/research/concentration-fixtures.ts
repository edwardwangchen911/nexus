import { decisionTime, instrumentId, researchData } from "./fixtures";

// Synthetic saved deterministic results; no model or portfolio calculation runs here.
export function concentrationResearch(mode: "known" | "unknown" = "known") {
  const base = researchData();
  const digest = (mode === "unknown" ? "f" : "d").repeat(64);
  const snapshotId = mode === "unknown" ? "0198d600-1000-7000-8000-000000001590" : base.data_snapshot_id;
  const portfolioSnapshotId = mode === "unknown" ? "0198d600-1000-7000-8000-000000001589" : "0198d600-1000-7000-8000-000000001597";
  const evidence = {
    source: "nexus_portfolio_context", object_id: `${portfolioSnapshotId}:${instrumentId}`,
    available_at: decisionTime, location: `memory://sha256/${digest}`, content_hash: digest,
  };
  return researchData({ data_snapshot_id: snapshotId, intent: { ...base.intent!, data_snapshot_id: snapshotId, quality_status: "partial", stance: "watch", exposure_action: "none", evidence_refs: [...base.intent!.evidence_refs, evidence],
    condition_evaluations: [
      { schema_version: "research_condition_evaluation_v1", purpose: "research_observation",
        condition: { signal: "portfolio.top1_warning_exceeded", operator: "is_true", threshold: true, unit: "boolean", evidence_refs: [evidence] },
        result: mode === "unknown" ? "unknown" : "true", reason: mode === "unknown" ? "frozen_concentration_unavailable" : null, observed_at: "2026-08-24T16:00:00Z" },
      { schema_version: "research_condition_evaluation_v1", purpose: "research_observation",
        condition: { signal: "portfolio.top1_critical_exceeded", operator: "is_true", threshold: true, unit: "boolean", evidence_refs: [evidence] },
        result: mode === "unknown" ? "unknown" : "false", reason: mode === "unknown" ? "frozen_concentration_unavailable" : null, observed_at: "2026-08-24T16:00:00Z" },
    ],
  }, quality_status: "partial", evidence: [...base.evidence, evidence] });
}
