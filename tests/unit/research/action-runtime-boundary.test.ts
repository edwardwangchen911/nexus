import { describe, expect, it } from "vitest";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { actionInputs } from "./action-fixtures";
import { decisionTime, instrumentId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope, runId } from "./fixtures";

async function response(inputs: object) {
  return sanitizeEngineResponse(
    new Response(JSON.stringify(researchAdmissionEnvelope(researchAdmissionData({ inputs }))), { headers: { "content-type": "application/json" } }),
    `https://nexus.test/api/v1/instruments/${instrumentId}/research-admission?as_of=${encodeURIComponent(decisionTime)}`,
  );
}

describe("action observation response boundary", () => {
  it("preserves exact source values and read-only semantics", async () => {
    const inputs = actionInputs();
    const result = await response(inputs);
    expect(result.status).toBe(200);
    expect((await result.json()).data.inputs).toEqual(inputs);
  });

  it("accepts saved responses without the optional action field", async () => {
    const { corporate_action_observations: omitted, ...old } = actionInputs();
    void omitted;
    const result = await response(old);
    expect(result.status).toBe(200);
    expect((await result.json()).data.inputs).toEqual(old);
  });

  it.each([true, false])("accepts exact run response with frozen inputs present=%s", async (present) => {
    const run = researchData(present ? { inputs: actionInputs() } : {});
    const result = await sanitizeEngineResponse(
      new Response(JSON.stringify(researchEnvelope(run)), { headers: { "content-type": "application/json" } }),
      `https://nexus.test/api/v1/research-runs/${runId}`,
    );
    expect(result.status).toBe(200);
    expect((await result.json()).data).toEqual(run);
  });

  it.each(["oversized", "ledger", "account", "decimal", "historic"])("rejects unsafe action display: %s", async (mutation) => {
    const inputs = actionInputs();
    const observation = inputs.corporate_action_observations[0];
    if (mutation === "oversized") observation.events = Array.from({ length: 33 }, () => observation.events[0]);
    if (mutation === "ledger") Object.assign(observation, { ledger_eligible: true });
    if (mutation === "historic") Object.assign(observation, { historical_backtest_eligible: true });
    if (mutation === "account") Object.assign(observation, { account_id: "must-not-escape" });
    if (mutation === "decimal") observation.events[0].rate = "1".repeat(41);
    const result = await response(inputs);
    expect(result.status).toBe(500);
    expect(await result.text()).not.toContain("must-not-escape");
  });
});
