import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { PortfolioScenario } from "../../../app/components/portfolio-scenario";
import type { components } from "../../../app/lib/generated/schema";
import { diagnosticReport } from "../optimize/fixtures";
import { snapshot, snapshotId } from "./fixtures";

type ScenarioResponse = components["schemas"]["PortfolioScenarioResponse"];

const reportId = "0198d500-1000-7000-8000-000000001312";

function report() {
  return {
    ...diagnosticReport("OPTIMIZATION_CANDIDATE"),
    diagnostic_report_id: reportId,
    portfolio_snapshot_id: snapshotId,
    snapshot_as_of: snapshot.as_of!,
  };
}

function scenario(objectiveStatus: ScenarioResponse["objective_status"] = "improved_not_met"): ScenarioResponse {
  const known = (value: string) => ({ status: "known" as const, value, reason: null });
  const unknown = (reason: string) => ({ status: "unknown" as const, value: null, reason });
  return {
    schema_version: "portfolio_scenario_evaluation_v2",
    request_hash: "a".repeat(64), result_hash: "b".repeat(64), diagnostic_input_hash: "c".repeat(64),
    portfolio_snapshot_id: snapshotId, diagnostic_report_id: reportId,
    valuation_time: snapshot.as_of!, valuation_scope: "currency:USD",
    objective: "reduce_security_concentration", objective_status: objectiveStatus,
    analysis_status: "partial", execution_locked: true,
    baseline: {
      security_top1: known("0.7"), security_top3: known("0.9"), security_hhi: known("0.55"),
      cash_weight: known("0.1"), one_way_turnover: known("0"), estimated_cost: unknown("COST_EVIDENCE_INCOMPLETE"),
    },
    candidate: {
      security_top1: known("0.55"), security_top3: known("0.75"), security_hhi: known("0.4"),
      cash_weight: known("0.25"), one_way_turnover: known("0.15"), estimated_cost: unknown("COST_EVIDENCE_INCOMPLETE"),
    },
    positions: [{
      instrument_id: snapshot.positions[0].instrument_id,
      current_weight: "0.7", target_weight: "0.55", action: "reduce", reason_code: "CONCENTRATION_REDUCTION",
    }, {
      instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799",
      current_weight: "0.2", target_weight: "0.2", action: "maintain", reason_code: "BASELINE_WEIGHT",
    }],
    cash_current_weight: "0.1", cash_target_weight: "0.25",
    hard_constraint_reasons: [], unknown_metrics: ["estimated_cost"],
    cost_breakdown: {
      currency: "USD", valuation_time: snapshot.as_of!, basis: "portfolio_value_ratio",
      evidence_version: "us_regulatory_fee_reference_2026_verified_20260911_v1",
      evidence_hash: "e".repeat(64), available_at: "2026-02-28T00:00:00Z",
      commission: unknown("BROKER_COMMISSION_SCHEDULE_UNAVAILABLE"),
      spread: unknown("BID_ASK_EVIDENCE_UNAVAILABLE"),
      slippage: unknown("SLIPPAGE_MODEL_UNAVAILABLE"),
      regulatory_fees: unknown("BROKER_REGULATORY_FEE_PASS_THROUGH_UNAVAILABLE"),
      tax_impact: unknown("TAX_DATA_UNAVAILABLE"),
      total_estimated_cost_ex_tax: unknown("COST_EVIDENCE_INCOMPLETE"),
    },
  };
}

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

it("stays idle until an explicit click and exposes only bounded scenario controls", () => {
  const send = vi.fn();
  vi.stubGlobal("fetch", send);

  render(<PortfolioScenario snapshot={snapshot} report={report()} stale={false} />);

  expect(screen.getByRole("region", { name: "只读调整情景" })).toBeVisible();
  expect(screen.getByRole("spinbutton", { name: "最大单向换手率" })).toHaveValue(0.15);
  expect(screen.getByRole("spinbutton", { name: "最低现金占比" })).toHaveValue(0.1);
  expect(screen.getByRole("button", { name: "生成调整情景" })).toBeEnabled();
  expect(screen.queryByRole("button", { name: "让 Agent 解读" })).not.toBeInTheDocument();
  expect(screen.getByText(/仅在本次页面内保留/)).toBeVisible();
  expect(send).not.toHaveBeenCalled();
});

it("posts intent and constraints without client-authored weights, then compares one candidate", async () => {
  let complete: ((response: Response) => void) | undefined;
  const pending = new Promise<Response>((resolve) => { complete = resolve; });
  const send = vi.fn().mockReturnValue(pending);
  vi.stubGlobal("fetch", send);
  render(<PortfolioScenario snapshot={snapshot} report={report()} stale={false} />);

  fireEvent.change(screen.getByRole("spinbutton", { name: "最大单向换手率" }), { target: { value: "0.2" } });
  fireEvent.change(screen.getByRole("spinbutton", { name: "最低现金占比" }), { target: { value: "0.12" } });
  fireEvent.click(screen.getByRole("button", { name: "生成调整情景" }));
  const running = screen.getByRole("button", { name: "正在生成只读情景…" });
  expect(running).toBeDisabled();
  fireEvent.click(running);
  expect(send).toHaveBeenCalledTimes(1);
  complete?.(Response.json({ data: scenario() }));

  await screen.findByRole("heading", { name: "有所改善，尚未达到阈值" });
  expect(send).toHaveBeenCalledTimes(1);
  expect(send.mock.calls[0][0]).toBe("/api/v1/portfolio-scenarios/evaluate");
  const init = send.mock.calls[0][1] as RequestInit;
  const body = JSON.parse(String(init.body));
  expect(body).toEqual({
    schema_version: "portfolio_scenario_request_v1",
    portfolio_snapshot_id: snapshotId,
    diagnostic_report_id: reportId,
    as_of: snapshot.as_of,
    constraints: {
      schema_version: "adjustment_scenario_constraints_v1",
      objective: "reduce_security_concentration",
      max_turnover: "0.2",
      minimum_cash: "0.12",
      universe_policy: "current_holdings_only",
      allow_new_positions: false,
    },
  });
  expect(body).not.toHaveProperty("positions");
  expect(body).not.toHaveProperty("weights");
  expect(body).not.toHaveProperty("instrument_ids");
  const comparison = screen.getByRole("table", { name: "当前组合与调整情景比较" });
  expect(within(comparison).getByText("70%")).toBeVisible();
  expect(within(comparison).getByText("55%")).toBeVisible();
  const costs = screen.getByRole("table", { name: "调整情景成本证据" });
  expect(within(costs).getByText("券商佣金")).toBeVisible();
  expect(within(costs).getByText("缺少适用的券商佣金费率依据")).toBeVisible();
  expect(within(costs).getByText("缺少冻结时点的买卖报价依据")).toBeVisible();
  expect(within(costs).getByText("税务资料不可用")).toBeVisible();
  expect(screen.getByText(/公开监管费率参考已记录/)).toHaveTextContent(
    "us_regulatory_fee_reference_2026_verified_20260911_v1",
  );
  expect(screen.getByText(/摘要哈希，不代表官方原件已归档，也不等于复星账户实际收费依据/)).toBeVisible();
  expect(screen.getByText(/成本证据未完成，不能进入执行或 Shadow/)).toBeVisible();
  expect(screen.getByText(/Top1、Top3 与 HHI.*证券市值为分母，不含现金/)).toBeVisible();
  expect(screen.getAllByText(/交易保持锁定/).length).toBeGreaterThanOrEqual(2);
  expect(screen.getByRole("button", { name: "让 Agent 解读" })).toBeEnabled();
  expect(screen.getByText(/保持 · 20%/)).toBeVisible();
  expect(screen.getByText("分析不完整")).toBeVisible();

  fireEvent.change(screen.getByRole("spinbutton", { name: "最低现金占比" }), { target: { value: "0.2" } });
  expect(screen.queryByRole("heading", { name: "有所改善，尚未达到阈值" })).not.toBeInTheDocument();
});

it("clears an in-memory result when the frozen snapshot becomes stale", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ data: scenario("infeasible") })));
  const { rerender } = render(<PortfolioScenario snapshot={snapshot} report={report()} stale={false} />);
  fireEvent.click(screen.getByRole("button", { name: "生成调整情景" }));
  expect(await screen.findByRole("heading", { name: "当前约束下无法改善" })).toBeVisible();

  rerender(<PortfolioScenario snapshot={snapshot} report={report()} stale />);

  await waitFor(() => expect(screen.queryByRole("heading", { name: "当前约束下无法改善" })).not.toBeInTheDocument());
  expect(screen.getByRole("button", { name: "冻结快照已过期" })).toBeDisabled();
  rerender(<PortfolioScenario snapshot={snapshot} report={report()} stale={false} />);
  expect(screen.queryByRole("heading", { name: "当前约束下无法改善" })).not.toBeInTheDocument();
});

it("drops an in-flight response after its constraints change", async () => {
  let complete: ((response: Response) => void) | undefined;
  vi.stubGlobal("fetch", vi.fn().mockReturnValue(new Promise<Response>((resolve) => { complete = resolve; })));
  render(<PortfolioScenario snapshot={snapshot} report={report()} stale={false} />);
  fireEvent.click(screen.getByRole("button", { name: "生成调整情景" }));

  fireEvent.change(screen.getByRole("spinbutton", { name: "最低现金占比" }), { target: { value: "0.2" } });
  complete?.(Response.json({ data: scenario() }));

  await waitFor(() => expect(screen.getByRole("button", { name: "生成调整情景" })).toBeEnabled());
  expect(screen.queryByRole("heading", { name: "有所改善，尚未达到阈值" })).not.toBeInTheDocument();
});

it.each([
  ["met", "目标已达到"],
  ["unchanged", "当前约束下无需调整"],
] as const)("renders the %s deterministic outcome without inventing an action", async (status, label) => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ data: scenario(status) })));
  render(<PortfolioScenario snapshot={snapshot} report={report()} stale={false} />);

  fireEvent.click(screen.getByRole("button", { name: "生成调整情景" }));

  expect(await screen.findByRole("heading", { name: label })).toBeVisible();
});

it("explains why a reconciled decision baseline is unavailable", () => {
  render(<PortfolioScenario snapshot={{ ...snapshot, cash_flow_quality: "PARTIAL" }} report={report()} stale={false} />);

  expect(screen.getByText(/现金流水尚未完成对账/)).toBeVisible();
  expect(screen.getByRole("button", { name: "生成调整情景" })).toBeDisabled();
});

it("keeps a failed evaluation local and does not retry", async () => {
  const send = vi.fn().mockResolvedValue(new Response(null, { status: 409 }));
  vi.stubGlobal("fetch", send);
  render(<PortfolioScenario snapshot={snapshot} report={report()} stale={false} />);

  fireEvent.click(screen.getByRole("button", { name: "生成调整情景" }));

  expect(await screen.findByText(/当前冻结快照尚不满足情景分析条件/)).toBeVisible();
  expect(send).toHaveBeenCalledTimes(1);
});
