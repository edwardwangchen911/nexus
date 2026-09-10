import { expect, test, type Page } from "@playwright/test";

import { diagnosticReport } from "../unit/optimize/fixtures";

const portfolioId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701";
const snapshotId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702";
const reportId = "0198d500-1000-7000-8000-000000001312";
const runId = "0198d500-1000-7000-8000-000000001412";
const asOf = "2026-08-23T08:00:00Z";
const resultHash = "b".repeat(64);
const citationId = "d".repeat(64);
const portfolio = {
  portfolio_id: portfolioId, name: "Fosun read-only", kind: "real_read_only",
  coverage_scope: "broker_account", provider: "fosun", latest_snapshot_status: "READY",
  latest_snapshot_as_of: asOf, original_currencies: ["USD"], last_sync_run_status: "READY",
};
const snapshot = {
  portfolio_snapshot_id: snapshotId, portfolio_id: portfolioId, coverage_scope: "broker_account",
  status: "READY", display_readiness: "DISPLAY_READY", decision_readiness: "DECISION_READY",
  cash_flow_quality: "PASS", cash_flow_count: 12, execution_locked: true,
  time_basis: "provider_event", as_of: asOf,
  cash: [{ currency: "USD", cash_balance: "1000", available_cash: "1000", provider_as_of: asOf }],
  positions: [{
    instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703", symbol: "AAPL", exchange: "XNAS",
    currency: "USD", quantity: "100", available_quantity: "100", frozen_quantity: "0",
    avg_cost: "35", diluted_cost: "35", current_price: "40", provider_as_of: asOf, read_only: false,
  }],
  open_orders: [], converted_view: null,
};

test("Agent interpretation requires one click and reload recovers with GET only", async ({ page }) => {
  await installFixtures(page);
  const posts: string[] = [];
  const gets: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (request.method() === "POST") posts.push(path);
    if (request.method() === "GET" && path.startsWith("/api/v1/portfolio-decision-runs/")) gets.push(path);
  });

  await page.goto("/portfolio");
  const scenario = page.getByRole("region", { name: "只读调整情景" });
  await expect(scenario.getByRole("button", { name: "生成调整情景" })).toBeEnabled();
  await expect(scenario.getByRole("button", { name: "让 Agent 解读" })).toHaveCount(0);
  expect(posts).toEqual([]);

  await scenario.getByRole("button", { name: "生成调整情景" }).click();
  await expect(scenario.getByRole("button", { name: "让 Agent 解读" })).toBeEnabled();
  expect(posts).toEqual(["/api/v1/portfolio-scenarios/evaluate"]);
  expect(gets).toEqual(["/api/v1/portfolio-decision-runs/latest"]);

  await scenario.getByRole("button", { name: "让 Agent 解读" }).click();
  await expect(scenario.getByText("集中度有所下降，但交易成本仍未知。")).toBeVisible();
  expect(posts).toEqual([
    "/api/v1/portfolio-scenarios/evaluate",
    "/api/v1/portfolio-decision-runs",
  ]);
  expect(gets).toEqual([
    "/api/v1/portfolio-decision-runs/latest",
    `/api/v1/portfolio-decision-runs/${runId}`,
  ]);
  await expect(scenario.getByText("研究解读，不是投资建议")).toBeVisible();
  await expect(scenario.getByText("缺少可核验的交易成本。")).toBeVisible();
  await expect(page.getByRole("button", { name: /接受|执行|Shadow|下单|启动前向观察/ })).toHaveCount(0);

  await page.reload();
  await expect(scenario.getByRole("button", { name: "生成调整情景" })).toBeEnabled();
  await expect(scenario.getByRole("button", { name: "让 Agent 解读" })).toHaveCount(0);
  expect(posts.filter((path) => path === "/api/v1/portfolio-decision-runs")).toHaveLength(1);

  await scenario.getByRole("button", { name: "生成调整情景" }).click();
  await expect(scenario.getByText("集中度有所下降，但交易成本仍未知。")).toBeVisible();
  expect(posts.filter((path) => path === "/api/v1/portfolio-decision-runs")).toHaveLength(1);
  expect(gets).toEqual([
    "/api/v1/portfolio-decision-runs/latest",
    `/api/v1/portfolio-decision-runs/${runId}`,
    `/api/v1/portfolio-decision-runs/${runId}`,
  ]);
  expect(posts.some((path) => /refresh|orders|fills|shadow/.test(path))).toBe(false);
});

test("an old scenario result hash is rejected instead of rendered", async ({ page }) => {
  await installFixtures(page, "c".repeat(64));
  await page.goto("/portfolio");
  const scenario = page.getByRole("region", { name: "只读调整情景" });

  await scenario.getByRole("button", { name: "生成调整情景" }).click();
  await scenario.getByRole("button", { name: "让 Agent 解读" }).click();

  await expect(scenario.getByText(/与当前调整情景不一致/)).toBeVisible();
  await expect(scenario.getByText("集中度有所下降，但交易成本仍未知。")).toHaveCount(0);
});

async function installFixtures(page: Page, returnedResultHash = resultHash): Promise<void> {
  const base = diagnosticReport("OPTIMIZATION_CANDIDATE");
  const report = { ...base, diagnostic_report_id: reportId, portfolio_snapshot_id: snapshotId, snapshot_as_of: asOf };
  await page.route("**/api/v1/integrations", (route) => route.fulfill(json({ data: [], meta: meta() })));
  await page.route("**/api/v1/portfolios?**", (route) => route.fulfill(json({ data: { items: [portfolio], next_cursor: null }, meta: meta() })));
  await page.route(`**/api/v1/portfolios/${portfolioId}/snapshots/latest`, (route) => route.fulfill(json({ data: snapshot, meta: meta() })));
  await page.route(`**/api/v1/portfolios/${portfolioId}/diagnostics/latest`, (route) => route.fulfill(json({ data: report, meta: meta() })));
  await page.route("**/api/v1/portfolio-scenarios/evaluate", (route) => route.fulfill(json({ data: scenarioResult(), meta: meta() })));
  await page.route("**/api/v1/portfolio-decision-runs/latest?**", (route) => {
    const url = new URL(route.request().url());
    expect(url.searchParams.get("portfolio_snapshot_id")).toBe(snapshotId);
    expect(url.searchParams.get("diagnostic_report_id")).toBe(reportId);
    expect(url.searchParams.get("expected_scenario_result_hash")).toBe(resultHash);
    return route.fulfill(json({
      error: { code: "NOT_FOUND", message: "Not found.", request_id: meta().request_id, details: {} },
    }, 404));
  });
  await page.route("**/api/v1/portfolio-decision-runs", async (route) => {
    expect(route.request().headers()["idempotency-key"]).toBeTruthy();
    const body = route.request().postDataJSON();
    expect(body.expected_scenario_result_hash).toBe(resultHash);
    expect(body.scenario_request).toBeTruthy();
    await route.fulfill(json({ data: decisionRun("prepared"), meta: meta() }, 202));
  });
  await page.route(`**/api/v1/portfolio-decision-runs/${runId}?**`, (route) => {
    const url = new URL(route.request().url());
    expect(url.searchParams.get("portfolio_snapshot_id")).toBe(snapshotId);
    expect(url.searchParams.get("diagnostic_report_id")).toBe(reportId);
    expect(url.searchParams.get("expected_scenario_result_hash")).toBe(resultHash);
    return route.fulfill(json({ data: decisionRun("completed", returnedResultHash), meta: meta() }));
  });
}

function scenarioResult() {
  const known = (value: string) => ({ status: "known", value, reason: null });
  const unknown = { status: "unknown", value: null, reason: "COST_EVIDENCE_INCOMPLETE" };
  return {
    schema_version: "portfolio_scenario_evaluation_v2", request_hash: "a".repeat(64), result_hash: resultHash,
    diagnostic_input_hash: "c".repeat(64), portfolio_snapshot_id: snapshotId, diagnostic_report_id: reportId,
    valuation_time: asOf, valuation_scope: "currency:USD", objective: "reduce_security_concentration",
    objective_status: "improved_not_met", analysis_status: "partial", execution_locked: true,
    baseline: { security_top1: known("0.8"), security_top3: known("0.8"), security_hhi: known("0.64"), cash_weight: known("0.2"), one_way_turnover: known("0"), estimated_cost: unknown },
    candidate: { security_top1: known("0.65"), security_top3: known("0.65"), security_hhi: known("0.4225"), cash_weight: known("0.35"), one_way_turnover: known("0.15"), estimated_cost: unknown },
    positions: [{ instrument_id: snapshot.positions[0].instrument_id, current_weight: "0.8", target_weight: "0.65", action: "reduce", reason_code: "CONCENTRATION_REDUCTION" }],
    cash_current_weight: "0.2", cash_target_weight: "0.35", hard_constraint_reasons: [], unknown_metrics: ["estimated_cost"],
    cost_breakdown: {
      currency: "USD", valuation_time: asOf, basis: "portfolio_value_ratio", evidence_version: null, evidence_hash: null, available_at: null,
      commission: { status: "unknown", value: null, reason: "BROKER_COMMISSION_SCHEDULE_UNAVAILABLE" },
      spread: { status: "unknown", value: null, reason: "BID_ASK_EVIDENCE_UNAVAILABLE" },
      slippage: { status: "unknown", value: null, reason: "SLIPPAGE_MODEL_UNAVAILABLE" },
      regulatory_fees: { status: "unknown", value: null, reason: "REGULATORY_FEE_SCHEDULE_UNAVAILABLE" },
      tax_impact: { status: "unknown", value: null, reason: "TAX_DATA_UNAVAILABLE" },
      total_estimated_cost_ex_tax: unknown,
    },
  };
}

function decisionRun(status: "prepared" | "completed", returnedResultHash = resultHash) {
  return {
    schema_version: "portfolio_decision_run_v1", run_id: runId, portfolio_id: portfolioId,
    portfolio_snapshot_id: snapshotId, diagnostic_report_id: reportId, decision_time: "2026-08-23T08:01:00Z",
    scenario_request_hash: "a".repeat(64), expected_scenario_result_hash: returnedResultHash,
    objective: "reduce_security_concentration", status, reserved_cost_usd: "0.01",
    settled_cost_usd: status === "completed" ? "0.004" : null,
    input_tokens: status === "completed" ? 80 : null, output_tokens: status === "completed" ? 40 : null,
    output: status === "completed" ? {
      status: "completed", scenario_result_hash: returnedResultHash, error_code: null,
      intent: {
        schema_version: "portfolio_research_intent_v1", run_id: runId, portfolio_id: portfolioId,
        portfolio_snapshot_id: snapshotId, diagnostic_report_id: reportId, decision_time: "2026-08-23T08:01:00Z",
        assessment: "explore_adjustment", objective: "reduce_security_concentration", horizon: "medium_term",
        confidence: "medium", rationale: [{ text: "集中度有所下降，但交易成本仍未知。", citation_ids: [citationId] }],
        invalidation_conditions: [{ signal: "portfolio.scenario_objective_status", operator: "eq", threshold: "unchanged", unit: "state", citation_ids: [citationId] }],
        limitations: [{ code: "estimated_cost", text: "缺少可核验的交易成本。" }],
        scenario_result_hash: returnedResultHash, citation_ids: [citationId], execution_locked: true,
      },
    } : null,
    citations: status === "completed" ? [{
      citation_id: citationId,
      evidence: { source: "portfolio_diagnostic", object_id: reportId, content_hash: "e".repeat(64), location: "memory://portfolio-diagnostic", available_at: asOf },
    }] : [],
    execution_locked: true,
  };
}

function json(body: unknown, status = 200) {
  return { status, contentType: "application/json", body: JSON.stringify(body) };
}

function meta() {
  return { request_id: "0198f7a0-1111-7777-8111-111111111111", schema_version: "1", as_of: asOf };
}
