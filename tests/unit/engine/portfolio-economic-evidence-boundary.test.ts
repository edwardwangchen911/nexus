import { expect, it } from "vitest";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";

const path = "/internal/v1/paper/economic-evidence";
const data = {
  as_of: "2026-10-08T09:00:00Z", execution_locked: true,
  protocol_status: "not_implemented", economic_status: "not_evaluated",
  planned_slot_count: null, economic_pair_count: null,
  source_status: "insufficient",
  source_reason: "PORTFOLIO_CHALLENGER_HISTORY_INSUFFICIENT", ordinary_samples: [],
};
const meta = { request_id: "0198d500-1000-7000-8000-000000001399", schema_version: "1", as_of: data.as_of };

it("preserves unknown economic counts through the normal response boundary", async () => {
  const response = await sanitizeEngineResponse(Response.json({ data, meta }), undefined, path, "GET");
  expect(response.status).toBe(200);
  expect((await response.json()).data).toEqual(data);
});

it.each([
  { economic_status: "PASS" }, { economic_pair_count: 0 },
  { planned_slot_count: 0 }, { account_id: "private" }, { prompt_content: "private" },
])("rejects invented economic facts and private fields %j", async (change) => {
  const response = await sanitizeEngineResponse(Response.json({ data: { ...data, ...change }, meta }), undefined, path, "GET");
  expect(response.status).toBe(500);
});


it("preserves registered and unavailable protocol states through the normal BFF boundary", async () => {
  const protocol = {
    protocol_hash: "a".repeat(64), registered_at: data.as_of,
    daily_model_jobs: 6, daily_sdk_calls: 14,
    daily_model_input_cap: 576000, daily_wire_input_cap: 512000,
    daily_output_cap: 49152, serial_invocation_ceiling_seconds: 570,
    activation_blockers: ["COMMON_EXECUTION_WINDOW_UNVERIFIED"],
    slots: Array.from({ length: 24 }, (_, index) => ({
      ordinal: index + 1, phase: index < 4 ? "training" : "oos",
      decision_at: new Date(Date.UTC(2027, 0, 4 + index, 15)).toISOString(),
    })),
  };
  for (const change of [
    { protocol_status: "registered_not_activated", planned_slot_count: 24, protocol },
    { protocol_status: "unavailable", planned_slot_count: null, protocol: null },
  ]) {
    const expected = { ...data, ...change };
    const response = await sanitizeEngineResponse(Response.json({ data: expected, meta }), undefined, path, "GET");
    expect(response.status).toBe(200);
    expect((await response.json()).data).toEqual(expected);
    expect(expected.economic_pair_count).toBeNull();
    expect(expected.economic_status).toBe("not_evaluated");
  }
});
