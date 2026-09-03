import { describe, expect, it } from "vitest";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { decisionTime, instrumentId, navHistoryInputs, researchAdmissionData, researchAdmissionEnvelope } from "./fixtures";

describe("NAV evidence response boundary", () => {
  async function response(inputs: ReturnType<typeof navHistoryInputs>) {
    return sanitizeEngineResponse(
      new Response(JSON.stringify(researchAdmissionEnvelope(researchAdmissionData({ inputs }))), { headers: { "content-type": "application/json" } }),
      `https://nexus.test/api/v1/instruments/${instrumentId}/research-admission?as_of=${encodeURIComponent(decisionTime)}`,
    );
  }

  it("preserves exact decimals, dates, provenance and unknowns", async () => {
    const inputs = navHistoryInputs();
    const result = await response(inputs);
    expect(result.status).toBe(200);
    expect((await result.json()).data.inputs).toEqual(inputs);
  });

  it.each(["oversized", "market-price", "total-return", "external-url", "account"])("rejects unsafe NAV payload: %s", async (mutation) => {
    const inputs = navHistoryInputs();
    const history = inputs.issuer_nav_histories[0];
    if (mutation === "oversized") history.observations = Array.from({ length: 33 }, () => history.observations[0]);
    if (mutation === "market-price") Object.assign(history, { market_price_eligible: true });
    if (mutation === "total-return") Object.assign(history, { total_return_eligible: true });
    if (mutation === "external-url") history.workbook_url = "https://untrusted.invalid/nav.xlsx";
    if (mutation === "account") Object.assign(history, { account_id: "must-not-escape" });
    const result = await response(inputs);
    expect(result.status).toBe(500);
    expect(await result.text()).not.toContain("must-not-escape");
  });
});
