import type { components } from "../../../app/lib/generated/schema";
import { agentPaperCycle, instrumentId } from "./fixtures";
import { managementDetail, managementId } from "./management-fixtures";

export const synthesisUsageDetail: components["schemas"]["AgentPaperManagementCycleResponse"] = {
  ...managementDetail,
  cycle: { ...agentPaperCycle, status: "unavailable", result_code: "PORTFOLIO_SYNTHESIS_UNAVAILABLE", decision: null, order_ids: [] },
  orders: [],
  candidate_batch: { ...managementDetail.candidate_batch!, candidate_instrument_ids: [instrumentId] },
  security_research: [{ instrument_id: instrumentId, research_run_id: managementId(20), research_output_id: managementId(21), status: "completed", content_hash: "a".repeat(64), intent_hash: "b".repeat(64), stance: "watch", quality_status: "partial", as_of: "2026-09-18T15:00:00Z" }],
  synthesis: { ...managementDetail.synthesis!, status: "failed", choices: [], cash_rationale: null },
  proposal: null,
  activation_result: null,
  blocker: null,
  blocking_plan: null,
  plan_summary: null,
  synthesis_usage_disposition: { status: "accepted", usage: "unknown", budget_preserved: true, execution_allowed: false },
};
