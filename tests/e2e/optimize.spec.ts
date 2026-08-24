import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const portfolioId = "0198d500-1000-7000-8000-000000001301";
const snapshotId = "0198d500-1000-7000-8000-000000001302";
const groupId = "0198d500-1000-7000-8000-000000001303";
const proposalIds = [
  "0198d500-1000-7000-8000-000000001304",
  "0198d500-1000-7000-8000-000000001305",
  "0198d500-1000-7000-8000-000000001306",
];
const instrumentIds = [
  "0198d500-1000-7000-8000-000000001307",
  "0198d500-1000-7000-8000-000000001308",
];
const meta = { request_id: "0198d500-1000-7000-8000-000000001399", schema_version: "1", as_of: "2026-08-24T00:00:00Z" };
const evidence = { source: "nexus_db", object_id: "diagnostic:e2e", available_at: meta.as_of, location: "db://diagnostics/e2e", content_hash: "a".repeat(64) };
const diagnostic = {
  portfolio_snapshot_id: snapshotId,
  snapshot_as_of: meta.as_of,
  trading_day: "2026-08-24",
  policy_version: "real_advisory_policy_v1",
  optimization_need: "OPTIMIZATION_CANDIDATE",
  items: [
    { diagnostic_code: "CONCENTRATION_TOP1", metric_value: "0.20", unit: "ratio", reference_limit: "0.15", headroom: "-0.05", headroom_reason: null, policy_scope: "portfolio", severity: "warning", actionability: "reduce_only", unknown_reason: null, evidence_refs: [evidence] },
    { diagnostic_code: "LIQUIDITY_DAYS_TO_EXIT", metric_value: null, unit: "trading_days", reference_limit: "2", headroom: null, headroom_reason: "ADV20_MISSING", policy_scope: "portfolio", severity: "warning", actionability: "research", unknown_reason: "ADV20_MISSING", evidence_refs: [evidence] },
  ],
};

for (const viewport of [
  { name: "mobile-375", width: 375, height: 812 },
  { name: "mobile-390", width: 390, height: 844 },
  { name: "tablet-768", width: 768, height: 1024 },
  { name: "desktop-1024", width: 1024, height: 768 },
  { name: "desktop-1440", width: 1440, height: 1000 },
  { name: "mobile-landscape", width: 844, height: 390 },
]) {
  test(`${viewport.name} optimization center preserves evidence and proposal context`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await installFixtures(page);
    await page.goto("/overview");

    await expect(page.getByRole("heading", { name: "值得比较优化方案" })).toBeVisible();
    await expect(page.getByRole("region", { name: "冻结组合事实" })).toContainText("real_advisory_policy_v1");
    await page.getByRole("link", { name: "打开组合诊断" }).click();
    await expect(page.getByRole("heading", { name: "组合诊断账簿" })).toBeVisible();
    await expect(page.getByRole("listitem", { name: /CONCENTRATION_TOP1/ })).toContainText("-0.05");
    await page.getByRole("link", { name: "打开已有三方案" }).click();
    await expect(page.getByRole("columnheader", { name: /保持现状|最小调整|策略目标/ })).toHaveCount(3);
    await expect(page.getByText(/STRATEGY_EVIDENCE_INCOMPLETE/)).toBeVisible();
    await page.getByRole("link", { name: "试算最小调整权重" }).click();
    await expect(page.getByRole("heading", { name: "有限权重情景" })).toBeVisible();
    await expect(page.getByRole("textbox", { name: "目标权重 1" })).toHaveValue("0.15");
    await expect(page.getByRole("button", { name: /下单|交易|买入|卖出/ })).toHaveCount(0);

    const serious = (await new AxeBuilder({ page }).analyze()).violations.filter((violation) => violation.impact === "serious" || violation.impact === "critical");
    expect(serious).toEqual([]);
  });
}

async function installFixtures(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: meta.as_of, capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }], meta }) }));
  await page.route("**/api/v1/overview**", async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: { portfolio_id: portfolioId, portfolio_snapshot_id: snapshotId, snapshot_as_of: meta.as_of, trading_day: "2026-08-24", diagnostic_report: diagnostic, proposal_group: { proposal_group_id: groupId, status: "compared", version: 4 }, proposal_admission: { portfolio_snapshot_id: snapshotId, data_snapshot_id: "0198d500-1000-7000-8000-000000001309", cost_policy_id: "0198d500-1000-7000-8000-000000001311" }, proposal_admission_reason: null }, meta }) }));
  await page.route(`**/api/v1/optimization-proposals/${groupId}`, async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: proposalGroup(), meta }) }));
}

function proposalGroup() {
  const variants = ["hold_current", "minimum_change", "strategy_target"] as const;
  return {
    proposal_group_id: groupId, status: "compared", version: 4, baseline_portfolio_snapshot_id: snapshotId,
    portfolio_scope: "real_read_only", diagnostic_input_hash: "b".repeat(64), data_snapshot_id: "0198d500-1000-7000-8000-000000001309", source_policy_id: "0198d500-1000-7000-8000-000000001310", cost_policy_id: "0198d500-1000-7000-8000-000000001311",
    proposals: variants.map((variant, index) => ({
      proposal_id: proposalIds[index], variant, analysis_only: true, strategy_evidence_complete: variant !== "strategy_target", portfolio_scope: "real_read_only",
      positions: instrumentIds.map((instrument_id, positionIndex) => ({ instrument_id, current_weight: positionIndex === 0 ? "0.20" : "0.10", target_weight: positionIndex === 0 && variant !== "hold_current" ? "0.15" : positionIndex === 0 ? "0.20" : "0.10", action: positionIndex === 0 && variant !== "hold_current" ? "reduce" : "maintain", reason_code: positionIndex === 0 ? "CONCENTRATION_TOP1" : "BASELINE_HOLDING" })),
      comparison: comparison(variant === "strategy_target"),
    })),
    shadow_eligibility: { eligible: false, reasons: ["STRATEGY_EVIDENCE_INCOMPLETE", "COST_NOT_PROVEN"] },
  };
}

function comparison(unknownCost: boolean) {
  const known = (value: string) => ({ status: "known", value, reason: null });
  const unknown = (reason: string) => ({ status: "unknown", value: null, reason });
  return { requires_fx: false, non_executable: unknownCost, cost_complete: !unknownCost, tax_impact: "unknown", metrics: { authority: { diagnostic_report_id: "0198d500-1000-7000-8000-000000001312", data_snapshot_id: "0198d500-1000-7000-8000-000000001309", source_policy_id: "0198d500-1000-7000-8000-000000001310", cost_policy_id: "0198d500-1000-7000-8000-000000001311", optimizer_policy_id: "0198d500-1000-7000-8000-000000001313" }, ranking_eligible: !unknownCost, constraint_complete: true, eligibility_complete: true, evidence_complete: !unknownCost, stale: false, unknown_metrics: unknownCost ? ["cost"] : [], dimensions: { hard_violations: 0, top1: known("0.15"), top3: known("0.25"), hhi: known("0.52"), sector_exposure: known("0.25"), currency_exposure: known("0.30"), volatility: unknown("VOLATILITY_POST_PROPOSAL_NOT_PROVEN"), drawdown: unknown("DRAWDOWN_POST_PROPOSAL_NOT_PROVEN"), correlation: unknown("CORRELATION_POST_PROPOSAL_NOT_PROVEN"), liquidity: known("0.90"), turnover: known("0.05"), cost: unknownCost ? unknown("COST_NOT_PROVEN") : known("0.001"), tax_impact: unknown("TAX_DATA_UNAVAILABLE") } } };
}
