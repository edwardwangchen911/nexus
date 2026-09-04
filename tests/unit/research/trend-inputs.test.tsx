import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ResearchInputs } from "../../../app/components/research-inputs";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { trendInputs } from "./trend-fixtures";
import { decisionTime, instrumentId, researchAdmissionData, researchAdmissionEnvelope, researchData } from "./fixtures";

describe("frozen current price trends", () => {
  it("shows original price units, RSI score, ratio volatility and missing warmup from real BFF sanitizer", async () => {
    const response = await sanitizeEngineResponse(Response.json(researchAdmissionEnvelope(researchAdmissionData({ inputs: trendInputs() }))), undefined, `/internal/v1/instruments/${instrumentId}/research-admission`);
    expect(response.status).toBe(200);
    const body = await response.json();
    render(<ResearchInputs inputs={body.data.inputs} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "拆股调整价格趋势" });
    expect(region).toHaveTextContent("123.457 USD");
    expect(region).toHaveTextContent("120 USD");
    expect(region).toHaveTextContent("200 日均线");
    expect(region).toHaveTextContent("资料不足");
    expect(region).toHaveTextContent("<0.001%");
    expect(region).toHaveTextContent("RSI14（0–100）");
    expect(within(region).getByText("0", { exact: true })).toBeVisible();
    expect(region).not.toHaveTextContent("0%");
    expect(region).toHaveTextContent("不是实时行情");
    expect(region).toHaveTextContent("不是总收益");
  });

  it("keeps query date, identity mapping, observed date and receipt distinct", () => {
    render(<ResearchInputs inputs={trendInputs()} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "拆股调整价格趋势" });
    expect(region).toHaveTextContent("身份映射日期");
    expect(region).toHaveTextContent("2026-08-24");
    expect(region).toHaveTextContent("2026-08-21");
    expect(region).toHaveTextContent("160 / 161");
    expect(region).toHaveTextContent("21 个交易日");
    expect(region).toHaveTextContent("2026-07-01");
    expect(region).toHaveTextContent("2026-08-25 01:00:00 北京时间");
    expect(region).toHaveTextContent("2026-08-25 02:00:00 北京时间");
  });

  it("does not retain new trends when switching to old frozen inputs", () => {
    const inputs = trendInputs();
    const { rerender } = render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    expect(screen.getByRole("region", { name: "拆股调整价格趋势" })).toBeVisible();
    Reflect.deleteProperty(inputs, "current_price_trends");
    rerender(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    expect(screen.queryByRole("region", { name: "拆股调整价格趋势" })).not.toBeInTheDocument();
  });

  it("displays frozen trends without a model result or run permission", () => {
    render(<ResearchPanel state="empty" research={null} admission={researchAdmissionData({
      inputs: trendInputs(), eligible: false, reason: "prompt_authority_unavailable",
    })} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByRole("region", { name: "拆股调整价格趋势" })).toBeVisible();
    expect(screen.getByText("资料已冻结 · 研究待生成")).toBeVisible();
    expect(screen.queryByRole("button", { name: "运行冻结研究" })).not.toBeInTheDocument();
  });

  it("exact old run never joins current admission trends", () => {
    render(<ResearchPanel state="partial" research={researchData({ status: "failed", intent: null, inputs: null })}
      admission={researchAdmissionData({ inputs: trendInputs() })} replay submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.queryByRole("region", { name: "拆股调整价格趋势" })).not.toBeInTheDocument();
  });

  it("keeps no observations unknown rather than rendering zeros or passing research", () => {
    const inputs = trendInputs();
    const trend = inputs.current_price_trends[0];
    Object.assign(trend, { observed_sessions: 0, expected_sessions: 1, contiguous_tail_sessions: 0,
      first_observed_on: null, last_observed_on: null,
      features: { close: null, sma_20: null, sma_200: null, rsi_14: null, volatility_20: null } });
    render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "拆股调整价格趋势" });
    expect(within(region).getAllByText("资料不足", { exact: true })).toHaveLength(5);
    expect(region).toHaveTextContent("无观察");
    expect(region).not.toHaveTextContent("0 USD");
    expect(region).not.toHaveTextContent("研究已完成");
  });
});
