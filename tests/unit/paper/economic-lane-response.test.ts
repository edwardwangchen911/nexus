import { describe, expect, it } from "vitest";
import { rebuildAllowedEngineResponse } from "../../../app/lib/engine/errors";
import { laneSample } from "./economic-fixtures";
const pairId = "0198f7a0-1111-7777-8111-111111111110";
const path = `/internal/v1/paper/economic-pairs/${pairId}/lanes/incumbent`;

const read = (value: unknown, target = path) => rebuildAllowedEngineResponse(200, value, new Headers(), `https://nexus.test${target}`, undefined, "GET");
describe("economic lane protected read contract", () => {
  it("preserves a correctly bound lane", async () => { const value = laneSample(); expect(await read(value)?.json()).toEqual(value); });
  it.each(["lane", "pair", "cycle", "status", "unlocked", "extra_account", "unknown_cash", "empty_performance"])("rejects %s identity leakage", (field) => {
    const value = laneSample();
    if (field === "lane") value.data.lane = "challenger";
    if (field === "pair") value.data.pair.pair_slot_id = value.data.pair.lanes[1].cycle_id;
    if (field === "cycle") value.data.detail.cycle.cycle_id = value.data.pair.lanes[1].cycle_id;
    if (field === "status") value.data.pair.lanes[0].cycle_status = "researching";
    if (field === "unlocked") value.data.pair.execution_locked = false;
    if (field === "extra_account") Object.assign(value.data, { account_id: pairId });
    if (field === "unknown_cash") Object.assign(value.data.actual, { data_state: "UNKNOWN" });
    if (field === "empty_performance") Object.assign(value.data.portfolio, { data_state: "EMPTY" });
    expect(read(value)).toBeNull();
  });
});

it("preserves failed synthesis facts without inventing choices or a proposal", async () => {
  const value = laneSample();
  value.data.detail.cycle.status = "unavailable";
  value.data.detail.cycle.result_code = "RESEARCH_OUTPUT_NOT_ACTIONABLE";
  value.data.pair.lanes[0].cycle_status = "unavailable";
  const asOf = value.data.detail.cycle.decision_time;
  const candidateId = value.data.detail.cycle.candidates[0].instrument_id;
  const id = "0198f7a0-2222-7777-8111-000000000001";
  const hash = "a".repeat(64);
  Object.assign(value.data.detail, {
    candidate_batch: { candidate_batch_id: id, universe_snapshot_id: id, candidate_instrument_ids: [candidateId], source: "explicit_research_scope", batch_hash: hash, as_of: asOf },
    security_research: [{ instrument_id: candidateId, research_run_id: id, research_output_id: id, status: "completed", content_hash: hash, intent_hash: hash, stance: "buy_candidate", quality_status: "complete", as_of: asOf }],
    synthesis: { synthesis_run_id: id, synthesis_output_id: id, candidate_batch_id: id, input_hash: hash, content_hash: hash, binding_hash: hash, status: "failed", choices: [], cash_rationale: null, as_of: asOf },
  });
  expect(await read(value)?.json()).toEqual(value);
  Object.assign(value.data.detail, { proposal: { strategy_decision_id: id } });
  expect(read(value)).toBeNull();
});
