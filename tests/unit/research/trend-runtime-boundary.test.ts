import { describe, expect, it } from "vitest";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { decisionTime, instrumentId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope, runId } from "./fixtures";
import { trendInputs } from "./trend-fixtures";

async function response(inputs: object, exact: boolean) {
  return sanitizeEngineResponse(Response.json(exact
    ? researchEnvelope(researchData({ inputs }))
    : researchAdmissionEnvelope(researchAdmissionData({ inputs }))), undefined,
  exact ? `/internal/v1/research-runs/${runId}` : `/internal/v1/instruments/${instrumentId}/research-admission`);
}

describe("current price trends BFF boundary", () => {
  it.each(["identity-date", "revision"])("rejects %s mismatch even when cutoff is later", async (kind) => {
    const inputs = trendInputs();
    const item = inputs.current_price_trends[0];
    item.received_at = "2026-08-23T17:00:00Z";
    if (kind === "revision") { item.identity_asof = "2026-08-23"; item.revision = "b".repeat(64); }
    for (const exact of [false, true]) expect((await response(inputs, exact)).status).toBe(500);
  });
  it.each([false, true])("preserves frozen units/null/precision exact=%s", async (exact) => {
    const inputs = trendInputs();
    const result = await response(inputs, exact);
    expect(result.status).toBe(200);
    expect((await result.json()).data.inputs).toEqual(inputs);
  });

  it.each([false, true])("accepts legacy inputs without new field exact=%s", async (exact) => {
    const inputs = trendInputs();
    Reflect.deleteProperty(inputs, "current_price_trends");
    expect((await response(inputs, exact)).status).toBe(200);
  });

  it.each(["key", "currency", "source", "unlock", "backtest", "price", "total-return", "future", "receipt", "window", "counts", "tail", "missing", "truncated", "rsi", "vol", "price-zero", "warmup", "decimal"])("rejects unsafe %s without leaking input", async (mutation) => {
    const inputs = trendInputs();
    const item = inputs.current_price_trends![0];
    if (mutation === "key") Object.assign(item.features, { private_canary: "1" });
    if (mutation === "currency") Object.assign(item, { currency: null });
    if (mutation === "source") Object.assign(item, { source: "private-canary" });
    if (mutation === "unlock") Object.assign(item, { execution_locked: false });
    if (mutation === "backtest") Object.assign(item, { historical_backtest_eligible: true });
    if (mutation === "price") Object.assign(item, { market_price_eligible: true });
    if (mutation === "total-return") Object.assign(item, { total_return_eligible: true });
    if (mutation === "future") item.available_at = "2026-08-25T00:00:00Z";
    if (mutation === "receipt") item.received_at = decisionTime;
    if (mutation === "window") item.query_end = "2025-01-01";
    if (mutation === "counts") item.observed_sessions = 162;
    if (mutation === "tail") item.contiguous_tail_sessions = 161;
    if (mutation === "missing") item.missing_sessions = ["2025-01-01"];
    if (mutation === "truncated") item.missing_sessions_truncated = true;
    if (mutation === "rsi") item.features.rsi_14 = "100.000000000001";
    if (mutation === "vol") item.features.volatility_20 = "-0.000000000001";
    if (mutation === "price-zero") item.features.close = "0";
    if (mutation === "warmup") item.features.sma_200 = "100";
    if (mutation === "decimal") item.features.close = "1e3";
    for (const exact of [false, true]) {
      const result = await response(inputs, exact);
      expect(result.status).toBe(500);
      expect(await result.text()).not.toContain("private-canary");
    }
  });
});
