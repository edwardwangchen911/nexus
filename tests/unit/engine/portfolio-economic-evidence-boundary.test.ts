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
