import { act, fireEvent, render, screen } from "@testing-library/react";
import type { ComponentProps } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  PortfolioAgentInterpretation,
  portfolioDecisionStorageKey,
} from "../../../app/components/portfolio-agent-interpretation";
import type { components } from "../../../app/lib/generated/schema";
import { portfolioId, snapshotId } from "./fixtures";

type DecisionRun = components["schemas"]["PortfolioDecisionRunResponse"];
type ScenarioRequest = components["schemas"]["PortfolioScenarioRequest"];
type ScenarioResponse = components["schemas"]["PortfolioScenarioResponse"];

const reportId = "0198d500-1000-7000-8000-000000001312";
const runId = "0198d500-1000-7000-8000-000000001412";
const resultHash = "b".repeat(64);
const citationId = "d".repeat(64);

const scenarioRequest: ScenarioRequest = {
  schema_version: "portfolio_scenario_request_v1",
  portfolio_snapshot_id: snapshotId,
  diagnostic_report_id: reportId,
  as_of: "2026-08-23T08:00:00Z",
  constraints: {
    schema_version: "adjustment_scenario_constraints_v1",
    objective: "reduce_security_concentration",
    max_turnover: "0.15",
    minimum_cash: "0.10",
    universe_policy: "current_holdings_only",
    allow_new_positions: false,
  },
};

const scenarioResult = {
  result_hash: resultHash,
  portfolio_snapshot_id: snapshotId,
  diagnostic_report_id: reportId,
  execution_locked: true,
} as ScenarioResponse;

function decisionRun(status: DecisionRun["status"] = "completed"): DecisionRun {
  return {
    schema_version: "portfolio_decision_run_v1",
    run_id: runId,
    portfolio_id: portfolioId,
    portfolio_snapshot_id: snapshotId,
    diagnostic_report_id: reportId,
    decision_time: "2026-08-23T08:01:00Z",
    scenario_request_hash: "a".repeat(64),
    expected_scenario_result_hash: resultHash,
    objective: "reduce_security_concentration",
    status,
    reserved_cost_usd: "0.01",
    settled_cost_usd: status === "completed" ? "0.004" : null,
    input_tokens: status === "completed" ? 80 : null,
    output_tokens: status === "completed" ? 40 : null,
    output: status === "completed" ? {
      status: "completed",
      scenario_result_hash: resultHash,
      error_code: null,
      intent: {
        schema_version: "portfolio_research_intent_v1",
        run_id: runId,
        portfolio_id: portfolioId,
        portfolio_snapshot_id: snapshotId,
        diagnostic_report_id: reportId,
        decision_time: "2026-08-23T08:01:00Z",
        assessment: "explore_adjustment",
        objective: "reduce_security_concentration",
        horizon: "medium_term",
        confidence: "medium",
        rationale: [{ text: "集中度有所下降，但交易成本仍未知。", citation_ids: [citationId] }],
        invalidation_conditions: [{
          signal: "portfolio.scenario_objective_status",
          operator: "eq",
          threshold: "unchanged",
          unit: "state",
          citation_ids: [citationId],
        }],
        limitations: [{ code: "estimated_cost", text: "缺少可核验的交易成本。" }],
        scenario_result_hash: resultHash,
        citation_ids: [citationId],
        execution_locked: true,
      },
    } : null,
    citations: status === "completed" ? [{
      citation_id: citationId,
      evidence: {
        source: "portfolio_diagnostic",
        object_id: reportId,
        content_hash: "e".repeat(64),
        location: "memory://portfolio-diagnostic",
        available_at: "2026-08-23T08:00:00Z",
      },
    }] : [],
    execution_locked: true,
  };
}

function view(overrides: Partial<ComponentProps<typeof PortfolioAgentInterpretation>> = {}) {
  return <PortfolioAgentInterpretation
    portfolioId={portfolioId}
    snapshotId={snapshotId}
    diagnosticReportId={reportId}
    scenarioRequest={scenarioRequest}
    scenarioResult={scenarioResult}
    stale={false}
    {...overrides}
  />;
}

function memoryStorage(): Storage {
  const values = new Map<string, string>();
  return {
    get length() { return values.size; },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => [...values.keys()][index] ?? null,
    removeItem: (key) => { values.delete(key); },
    setItem: (key, value) => { values.set(key, value); },
  };
}

beforeEach(() => {
  Object.defineProperty(window, "localStorage", { configurable: true, value: memoryStorage() });
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("portfolio Agent interpretation", () => {
  it("discovers a persisted matching run without localStorage and never creates work", async () => {
    const send = vi.fn().mockResolvedValue(Response.json({ data: decisionRun() }));
    vi.stubGlobal("fetch", send);

    render(view());

    expect(await screen.findByText("集中度有所下降，但交易成本仍未知。")).toBeVisible();
    expect(send).toHaveBeenCalledTimes(1);
    const [url, init] = send.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/api/v1/portfolio-decision-runs/latest?");
    expect(init).toMatchObject({ method: "GET" });
    expect(send.mock.calls.some(([, requestInit]) => requestInit?.method === "POST")).toBe(false);
    const saved = JSON.parse(window.localStorage.getItem(portfolioDecisionStorageKey(
      portfolioId, snapshotId, reportId, resultHash,
    ))!);
    expect(saved).toEqual({ idempotencyKey: expect.any(String), runId });
  });

  it("resumes a discovered prepared run with GET only", async () => {
    vi.useFakeTimers();
    const send = vi.fn()
      .mockResolvedValueOnce(Response.json({ data: decisionRun("prepared") }))
      .mockResolvedValueOnce(Response.json({ data: decisionRun() }));
    vi.stubGlobal("fetch", send);

    render(view());

    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(screen.getByRole("button", { name: "正在生成研究解读…" })).toBeDisabled();
    await act(async () => { await vi.advanceTimersByTimeAsync(500); });
    expect(screen.getByText("集中度有所下降，但交易成本仍未知。")).toBeVisible();
    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls.every(([, init]) => init?.method === "GET")).toBe(true);
  });

  it("retries uncertain discovery without creating a duplicate run", async () => {
    const send = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 500 }))
      .mockResolvedValueOnce(new Response(null, { status: 404 }));
    vi.stubGlobal("fetch", send);

    render(view());

    expect(await screen.findByText(/无法确认是否已有保存的研究解读/)).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "继续查看 Agent 解读" }));
    expect(await screen.findByRole("button", { name: "让 Agent 解读" })).toBeEnabled();

    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls.every(([, init]) => init?.method === "GET")).toBe(true);
  });

  it("treats a mismatched discovery response as uncertain and never posts", async () => {
    const mismatched = decisionRun();
    mismatched.expected_scenario_result_hash = "c".repeat(64);
    const send = vi.fn()
      .mockResolvedValueOnce(Response.json({ data: mismatched }))
      .mockResolvedValueOnce(new Response(null, { status: 404 }));
    vi.stubGlobal("fetch", send);

    render(view());

    expect(await screen.findByText(/与当前调整情景不一致/)).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "继续查看 Agent 解读" }));
    expect(await screen.findByRole("button", { name: "让 Agent 解读" })).toBeEnabled();
    expect(send.mock.calls.every(([, init]) => init?.method === "GET")).toBe(true);
  });

  it("creates only after discovery finds no saved run and one explicit click", async () => {
    const send = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(Response.json({ data: decisionRun() }, { status: 202 }));
    vi.stubGlobal("fetch", send);
    render(view());

    expect(await screen.findByRole("button", { name: "让 Agent 解读" })).toBeEnabled();
    expect(send.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(0);
    fireEvent.click(screen.getByRole("button", { name: "让 Agent 解读" }));
    fireEvent.click(screen.getByRole("button", { name: "正在生成研究解读…" }));

    expect(await screen.findByText("集中度有所下降，但交易成本仍未知。")).toBeVisible();
    expect(send).toHaveBeenCalledTimes(2);
    const [url, init] = send.mock.calls[1] as [string, RequestInit];
    expect(url).toBe("/api/v1/portfolio-decision-runs");
    expect(JSON.parse(String(init.body))).toEqual({
      schema_version: "portfolio_decision_create_v1",
      portfolio_snapshot_id: snapshotId,
      diagnostic_report_id: reportId,
      expected_scenario_result_hash: resultHash,
      scenario_request: scenarioRequest,
    });
    const saved = JSON.parse(window.localStorage.getItem(portfolioDecisionStorageKey(
      portfolioId, snapshotId, reportId, resultHash,
    ))!);
    expect(saved).toEqual({ idempotencyKey: expect.any(String), runId });
    expect(JSON.stringify(saved)).not.toContain("集中度");
    expect(screen.getByText("缺少可核验的交易成本。")).toBeVisible();
    expect(screen.getByText(/情景目标状态.*变为.*无变化/)).toBeVisible();
    expect(screen.getByText(/portfolio_diagnostic/)).toBeVisible();
    expect(screen.getByText("研究解读，不是投资建议")).toBeVisible();
    expect(screen.getByText("交易保持锁定")).toBeVisible();
    expect(screen.queryByRole("button", { name: /接受|执行|Shadow/ })).not.toBeInTheDocument();
  });

  it("restores a matching run with GET only and never creates work on render", async () => {
    window.localStorage.setItem(portfolioDecisionStorageKey(portfolioId, snapshotId, reportId, resultHash), JSON.stringify({
      idempotencyKey: "saved-key",
      runId,
    }));
    const send = vi.fn().mockResolvedValue(Response.json({ data: decisionRun() }));
    vi.stubGlobal("fetch", send);

    render(view());

    expect(await screen.findByText("集中度有所下降，但交易成本仍未知。")).toBeVisible();
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0][1]).toMatchObject({ method: "GET" });
    expect(String(send.mock.calls[0][0])).toContain(`/api/v1/portfolio-decision-runs/${runId}?`);
    expect(send.mock.calls.some(([, init]) => init?.method === "POST")).toBe(false);
  });

  it("polls a prepared run with bounded GET and prevents duplicate POST", async () => {
    vi.useFakeTimers();
    const send = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(Response.json({ data: decisionRun("prepared") }, { status: 202 }))
      .mockResolvedValueOnce(Response.json({ data: decisionRun() }));
    vi.stubGlobal("fetch", send);
    render(view());

    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    fireEvent.click(screen.getByRole("button", { name: "让 Agent 解读" }));
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(screen.getByRole("button", { name: "正在生成研究解读…" })).toBeDisabled();
    await act(async () => { await vi.advanceTimersByTimeAsync(500); });

    expect(screen.getByText("集中度有所下降，但交易成本仍未知。")).toBeVisible();
    expect(send.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
    expect(send.mock.calls.filter(([, init]) => init?.method === "GET")).toHaveLength(2);
  });

  it("stops observing after 45 seconds without changing the durable run", async () => {
    vi.useFakeTimers();
    const send = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(Response.json({ data: decisionRun("prepared") }, { status: 202 }))
      .mockReturnValueOnce(new Promise<Response>(() => undefined));
    vi.stubGlobal("fetch", send);
    render(view());

    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    fireEvent.click(screen.getByRole("button", { name: "让 Agent 解读" }));
    await act(async () => { await vi.advanceTimersByTimeAsync(45_000); });

    expect(screen.getByText(/本次查看已暂停；后台运行不受影响/)).toBeVisible();
    expect(screen.getByRole("button", { name: "继续查看 Agent 解读" })).toBeEnabled();
    expect(send.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
    expect(send.mock.calls.filter(([, init]) => init?.method === "GET")).toHaveLength(2);
  });

  it("reuses the same idempotency key after an uncertain POST result", async () => {
    const send = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockRejectedValueOnce(new TypeError("connection closed"))
      .mockResolvedValueOnce(Response.json({ data: decisionRun() }, { status: 202 }));
    vi.stubGlobal("fetch", send);
    render(view());

    await screen.findByRole("button", { name: "让 Agent 解读" });
    fireEvent.click(screen.getByRole("button", { name: "让 Agent 解读" }));
    expect(await screen.findByText(/登记结果尚未确认/)).toBeVisible();
    const firstKey = (send.mock.calls[1][1] as RequestInit).headers as Record<string, string>;
    fireEvent.click(screen.getByRole("button", { name: "让 Agent 解读" }));

    expect(await screen.findByText("集中度有所下降，但交易成本仍未知。")).toBeVisible();
    const secondKey = (send.mock.calls[2][1] as RequestInit).headers as Record<string, string>;
    expect(secondKey["idempotency-key"]).toBe(firstKey["idempotency-key"]);
  });

  it("keeps other scope idempotency state and rejects a mismatched response", async () => {
    const oldKey = portfolioDecisionStorageKey(portfolioId, snapshotId, reportId, "c".repeat(64));
    window.localStorage.setItem(oldKey, JSON.stringify({ idempotencyKey: "old", runId }));
    const mismatched = decisionRun();
    mismatched.expected_scenario_result_hash = "c".repeat(64);
    const send = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 404 }))
      .mockResolvedValueOnce(Response.json({ data: mismatched }, { status: 202 }));
    vi.stubGlobal("fetch", send);
    render(view());

    expect(window.localStorage.getItem(oldKey)).not.toBeNull();
    await screen.findByRole("button", { name: "让 Agent 解读" });
    fireEvent.click(screen.getByRole("button", { name: "让 Agent 解读" }));
    expect(await screen.findByText(/与当前调整情景不一致/)).toBeVisible();
    expect(screen.queryByText("集中度有所下降，但交易成本仍未知。")).not.toBeInTheDocument();
  });

  it("does not expose an action when the scenario is stale", () => {
    const send = vi.fn();
    vi.stubGlobal("fetch", send);
    render(view({ stale: true }));

    expect(screen.queryByRole("button", { name: "让 Agent 解读" })).not.toBeInTheDocument();
    expect(send).not.toHaveBeenCalled();
  });
});
