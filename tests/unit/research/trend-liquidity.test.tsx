import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ResearchInputs } from "../../../app/components/research-inputs";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { trendInputs } from "./trend-fixtures";
import { decisionTime, instrumentId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope, runId } from "./fixtures";

const observation = {
  method: "mean_20_session_close_times_volume", status: "available", value: "1234567.890123",
  currency: "USD", unit: "USD_per_trading_session", window_sessions: 20, observed_sessions: 20,
  window_start: "2026-07-27", window_end: "2026-08-21", missing_sessions: [],
};

function inputsWith(value: object | null) {
  const inputs = trendInputs();
  Object.assign(inputs.current_price_trends[0], { liquidity_observation: value });
  return inputs;
}

describe("frozen trend liquidity observation", () => {
  it.each([
    { ...observation, value: "0" },
    { ...observation, status: "missing_sessions", value: null, observed_sessions: 19, missing_sessions: ["2026-08-20"] },
    { ...observation, status: "insufficient_sessions", value: null, observed_sessions: 8 },
  ])("preserves valid incomplete and zero observations through the BFF", async (liquidity) => {
    const inputs = inputsWith(liquidity);
    for (const exact of [false, true]) {
      const response = await sanitizeEngineResponse(Response.json(exact
        ? researchEnvelope(researchData({ inputs }))
        : researchAdmissionEnvelope(researchAdmissionData({ inputs }))), undefined,
      exact ? `/internal/v1/research-runs/${runId}` : `/internal/v1/instruments/${instrumentId}/research-admission`);
      expect(response.status).toBe(200);
      expect((await response.json()).data.inputs.current_price_trends[0].liquidity_observation).toEqual(liquidity);
    }
  });

  it.each([
    { value: null }, { value: "-0.001" }, { observed_sessions: 19 },
    { status: "missing_sessions" }, { status: "insufficient_sessions", value: null },
    { window_end: "2026-08-20" }, { window_start: "2025-01-01" },
    { missing_sessions: ["2026-08-20"] },
    { status: "missing_sessions", value: null, observed_sessions: 19, missing_sessions: ["2026-08-20", "2026-08-20"] },
  ])("rejects inconsistent frozen liquidity %j on admission and exact replay", async (change) => {
    const inputs = inputsWith({ ...observation, ...change });
    for (const exact of [false, true]) {
      const response = await sanitizeEngineResponse(Response.json(exact
        ? researchEnvelope(researchData({ inputs }))
        : researchAdmissionEnvelope(researchAdmissionData({ inputs }))), undefined,
      exact ? `/internal/v1/research-runs/${runId}` : `/internal/v1/instruments/${instrumentId}/research-admission`);
      expect(response.status).toBe(500);
      expect(await response.text()).not.toContain("mean_20_session_close_times_volume");
    }
  });

  it.each([false, true])("preserves the observation through the BFF into the page, exact=%s", async (exact) => {
    const inputs = inputsWith(observation);
    const response = await sanitizeEngineResponse(Response.json(exact
      ? researchEnvelope(researchData({ inputs }))
      : researchAdmissionEnvelope(researchAdmissionData({ inputs }))), undefined,
    exact ? `/internal/v1/research-runs/${runId}` : `/internal/v1/instruments/${instrumentId}/research-admission`);
    expect(response.status).toBe(200);
    const body = await response.json();
    expect(body.data.inputs.current_price_trends[0].liquidity_observation).toEqual(observation);
    render(<ResearchInputs inputs={body.data.inputs} asOf={decisionTime} />);
    expect(screen.getByRole("region", { name: "成交金额代理观察" })).toHaveTextContent("1234567.89 USD / 交易日");
  });

  it.each([
    ["1234567.890123", "1234567.89 USD / 交易日"],
    ["0", "0 USD / 交易日"],
    ["0.00000001", "<0.001 USD / 交易日"],
  ])("renders decimal %s without changing units or treating zero as unknown", (value, expected) => {
    render(<ResearchInputs inputs={inputsWith({ ...observation, value })} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "成交金额代理观察" });
    expect(within(region).getByText("20 日收盘价×成交量金额代理").nextElementSibling).toHaveTextContent(expected);
    expect(region).toHaveTextContent("2026-07-27 至 2026-08-21");
    expect(region).toHaveTextContent("20 / 20");
    expect(region).toHaveTextContent("不是精确成交额、正式 ADV20 或退出天数");
  });

  it.each([
    ["missing_sessions", 19, ["2026-08-20"], "窗口内缺少交易日"],
    ["insufficient_sessions", 8, [], "查询范围不足 20 个交易日"],
  ])("does not turn %s into a shorter-window mean", (status, observed, missing, reason) => {
    render(<ResearchInputs inputs={inputsWith({ ...observation, status, value: null,
      observed_sessions: observed, missing_sessions: missing })} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "成交金额代理观察" });
    expect(region).toHaveTextContent(reason);
    expect(region).toHaveTextContent(`${observed} / 20`);
    expect(region).not.toHaveTextContent("0 USD / 交易日");
    if (missing.length) expect(region).toHaveTextContent("2026-08-20");
  });

  it("removes new observation when switching to an old frozen document", () => {
    const { rerender } = render(<ResearchInputs inputs={inputsWith(observation)} asOf={decisionTime} />);
    expect(screen.getByRole("region", { name: "成交金额代理观察" })).toBeVisible();
    rerender(<ResearchInputs inputs={trendInputs()} asOf={decisionTime} />);
    expect(screen.queryByRole("region", { name: "成交金额代理观察" })).not.toBeInTheDocument();
    rerender(<ResearchInputs inputs={inputsWith(null)} asOf={decisionTime} />);
    expect(screen.queryByRole("region", { name: "成交金额代理观察" })).not.toBeInTheDocument();
  });
});
