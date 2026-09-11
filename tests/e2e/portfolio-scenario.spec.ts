import { expect, test, type Page } from "@playwright/test";

import { diagnosticReport } from "../unit/optimize/fixtures";

const portfolioId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701";
const snapshotId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702";
const reportId = "0198d500-1000-7000-8000-000000001312";
const asOf = "2026-08-23T08:00:00Z";
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

test("one explicit click evaluates one ephemeral scenario and refresh clears it", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installFixtures(page);
  const posts: string[] = [];
  page.on("request", (request) => {
    if (request.method() === "POST") posts.push(new URL(request.url()).pathname);
  });

  await page.goto("/portfolio");
  const scenario = page.getByRole("region", { name: "只读调整情景" });
  await expect(scenario.getByRole("button", { name: "生成调整情景" })).toBeEnabled();
  expect(posts).toEqual([]);

  await scenario.getByRole("button", { name: "生成调整情景" }).click();
  await expect(scenario.getByRole("heading", { name: "有所改善，尚未达到阈值" })).toBeVisible();
  await expect(scenario.getByText("可用于分析比较")).toBeVisible();
  await expect(scenario.getByText("不可执行")).toBeVisible();
  await expect(scenario.getByRole("table", { name: "当前组合与调整情景比较" })).toContainText("65%");
  expect(posts).toEqual(["/api/v1/portfolio-scenarios/evaluate"]);
  await expect(page.getByRole("button", { name: /接受|下单|启动前向观察/ })).toHaveCount(0);

  await page.getByRole("button", { name: "刷新页面数据" }).click();
  await expect(scenario.getByRole("heading", { name: "有所改善，尚未达到阈值" })).toHaveCount(0);
  await expect(scenario.getByRole("button", { name: "生成调整情景" })).toBeEnabled();
  expect(posts).toEqual(["/api/v1/portfolio-scenarios/evaluate"]);
});

async function installFixtures(page: Page): Promise<void> {
  const base = diagnosticReport("OPTIMIZATION_CANDIDATE");
  const report = { ...base, diagnostic_report_id: reportId, portfolio_snapshot_id: snapshotId, snapshot_as_of: asOf };
  await page.route("**/api/v1/integrations", (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: [], meta: meta() }) }));
  await page.route("**/api/v1/portfolios?**", (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: { items: [portfolio], next_cursor: null }, meta: meta() }) }));
  await page.route(`**/api/v1/portfolios/${portfolioId}/snapshots/latest`, (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: snapshot, meta: meta() }) }));
  await page.route(`**/api/v1/portfolios/${portfolioId}/diagnostics/latest`, (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: report, meta: meta() }) }));
  await page.route("**/api/v1/portfolio-scenarios/evaluate", async (route) => {
    const body = route.request().postDataJSON();
    expect(Object.keys(body).sort()).toEqual(["as_of", "constraints", "diagnostic_report_id", "portfolio_snapshot_id", "schema_version"]);
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: result(), meta: meta() }) });
  });
}

function result() {
  const known = (value: string) => ({ status: "known", value, reason: null });
  const unknown = { status: "unknown", value: null, reason: "COST_EVIDENCE_INCOMPLETE" };
  return {
    schema_version: "portfolio_scenario_evaluation_v2", request_hash: "a".repeat(64),
    result_hash: "b".repeat(64), diagnostic_input_hash: "c".repeat(64),
    portfolio_snapshot_id: snapshotId, diagnostic_report_id: reportId, valuation_time: asOf,
    valuation_scope: "currency:USD", objective: "reduce_security_concentration",
    objective_status: "improved_not_met", analysis_status: "partial",
    analysis_comparable: true, execution_eligible: false, execution_locked: true,
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

function meta() {
  return { request_id: "0198f7a0-1111-7777-8111-111111111111", schema_version: "1", as_of: asOf };
}
