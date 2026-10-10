import { managementDetail, managementPlan } from "./management-fixtures";
import { paperPortfolio, meta } from "./fixtures";
const pairId = "0198f7a0-1111-7777-8111-111111111110";
export function laneSample() {
  return { data: { lane: "incumbent", pair: {
    pair_slot_id: pairId, evaluation_id: pairId, ordinal: 1, planned_slots: 24,
    decision_session: managementDetail.cycle.decision_session,
    decision_at: managementDetail.cycle.decision_time, execution_locked: true, economic_status: "not_evaluated",
    lanes: [{ lane: "incumbent", cycle_id: managementDetail.cycle.cycle_id, cycle_status: managementDetail.cycle.status },
      { lane: "challenger", cycle_id: "0198f7a0-1111-7777-8111-111111111113", cycle_status: "researching" }],
    execution: null, gap_code: "ECONOMIC_WINDOW_REQUIRED",
  }, detail: { ...structuredClone(managementDetail), candidate_batch: null, security_research: [], synthesis: null, proposal: null, active_target: null, activation_result: null, blocker: null, blocking_plan: null, plan_summary: null }, actual: structuredClone(managementPlan.actual), portfolio: structuredClone(paperPortfolio) }, meta: meta() };
}
