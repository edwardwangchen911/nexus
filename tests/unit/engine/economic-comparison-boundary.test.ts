import { describe, expect, it } from "vitest";

import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { groupId, proposalGroup } from "../optimize/fixtures";

const meta = {
  request_id: "0198d500-1000-7000-8000-000000001399",
  schema_version: "1", as_of: "2026-08-24T00:00:00Z",
};
const path = `/internal/v1/optimization-proposals/${groupId}`;

async function read(economic: unknown) {
  const data = { ...proposalGroup(), economic_comparison: economic };
  return sanitizeEngineResponse(Response.json({ data, meta }), undefined, path);
}

describe("economic comparison response boundary", () => {
  it.each([undefined, null])("keeps legacy absence %s without inferring hold", async (value) => {
    const response = await read(value);
    expect(response.status).toBe(200);
    expect((await response.json()).data.economic_comparison).toBe(value);
  });

  it("retains a reason without changing execution eligibility", async () => {
    const economic = {
      status: "policy_required", recommendation: null,
      reasons: ["ECONOMIC_COMPARISON_POLICY_MISSING"],
    };
    const response = await read(economic);
    expect(response.status).toBe(200);
    const data = (await response.json()).data;
    expect(data.economic_comparison).toEqual(economic);
    expect(data.shadow_eligibility).toEqual(proposalGroup().shadow_eligibility);
  });

  it.each([
    { status: "incomparable", recommendation: "hold_current", reasons: ["MISSING"] },
    { status: "policy_required", recommendation: null, reasons: [] },
    { status: "compared", recommendation: null, reasons: [] },
  ])("rejects contradictory economic output %j", async (economic) => {
    expect((await read(economic)).status).toBe(500);
  });

  it("accepts a completed comparison without granting execution permission", async () => {
    const response = await read({ status: "compared", recommendation: "hold_current", reasons: [] });
    expect(response.status).toBe(200);
    expect((await response.json()).data.shadow_eligibility.eligible).toBe(false);
  });
});
