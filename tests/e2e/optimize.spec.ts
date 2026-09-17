import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";
import { eligibleProposalGroup } from "../unit/optimize/fixtures";

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
  { name: "mobile-short", width: 390, height: 480 },
  { name: "tablet-768", width: 768, height: 1024 },
  { name: "desktop-1024", width: 1024, height: 768 },
  { name: "desktop-1440", width: 1440, height: 1000 },
  { name: "mobile-landscape", width: 844, height: 390 },
]) {
  test(`${viewport.name} optimization center preserves evidence and proposal context`, async ({ page }) => {
    test.slow();
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await installFixtures(page);
    await page.goto("/overview");

    await expect(page.getByRole("heading", { name: "值得比较优化方案" })).toBeVisible({ timeout: 15_000 });
    await page.getByText(/报告来源与版本/).click();
    await expect(page.getByText("real_advisory_policy_v1")).toBeVisible();
    if (viewport.width < 768) {
      const drawer = page.locator(".app-shell__drawer");
      await drawer.locator(":scope > summary").click();
      await expect(drawer).toHaveAttribute("open", "");
      const mobileWorkbench = page.getByRole("navigation", { name: "移动主导航" }).getByRole("link", { name: "交易工作台" });
      await expect(mobileWorkbench).toBeVisible();
      await mobileWorkbench.focus();
      await expect(mobileWorkbench).toBeFocused();
      if (viewport.name === "mobile-short") {
        const navigationPanel = drawer.locator(":scope > .app-shell__navigation-groups");
        const scrollEvidence = await navigationPanel.evaluate((element) => {
          const panel = element as HTMLElement;
          panel.scrollTop = panel.scrollHeight;
          return {
            clientHeight: panel.clientHeight,
            overflowY: getComputedStyle(panel).overflowY,
            scrollHeight: panel.scrollHeight,
            scrollTop: panel.scrollTop,
          };
        });
        expect(scrollEvidence.overflowY).toBe("auto");
        expect(scrollEvidence.scrollHeight).toBeLessThanOrEqual(scrollEvidence.clientHeight);
        expect(scrollEvidence.scrollTop).toBe(0);
      }
      await drawer.locator(":scope > summary").click();
      await expect(drawer).not.toHaveAttribute("open", "");
    } else {
      await expect(page.getByRole("navigation", { name: "主导航" }).getByRole("link", { name: "交易工作台" })).toBeVisible();
    }
    await expect(page.getByRole("link", { name: "生成一个调整候选" })).toHaveAttribute("href", "/portfolio");
    await page.goto("/optimize");
    await expect(page.getByRole("region", { name: "投资决策证据链，可横向滚动" }).getByRole("listitem").filter({ hasText: "组合诊断" })).toHaveAttribute("aria-current", "step");
    await expect(page.getByRole("heading", { name: "组合诊断账簿" })).toBeVisible();
    await expect(page.getByRole("listitem", { name: /CONCENTRATION_TOP1/ })).toContainText("-5 个百分点");
    if (viewport.name === "mobile-390" || viewport.name === "desktop-1440") {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: `test-results/optimize-${viewport.name}.png`, fullPage: true });
    }
    await page.getByRole("link", { name: "查看历史三方案记录" }).click();
    await expect(page.getByRole("region", { name: "投资决策证据链，可横向滚动" }).getByRole("listitem").filter({ hasText: "组合诊断" })).toHaveAttribute("aria-current", "step");
    const proposalTable = page.getByRole("table", { name: "保持现状、最小调整与策略目标的确定性比较" });
    await expect(proposalTable.getByRole("columnheader", { name: /保持现状|最小调整|策略目标/ })).toHaveCount(3);
    await expect(page.getByRole("heading", { name: "前向影子绩效" })).toBeVisible();
    await expect(page.getByLabel("三方案线型图例")).toContainText("实线");
    await expect(page.getByRole("table", { name: "三方案逐交易日净值" })).toBeVisible();
    await expect(page.getByRole("note")).toContainText("不会创建订单");
    if (viewport.name === "mobile-390" || viewport.name === "desktop-1440") {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: `test-results/proposal-group-${viewport.name}.png`, fullPage: true });
    }
    const sessionNavigator = page.getByRole("group", { name: "选择影子交易日" });
    await sessionNavigator.focus();
    await sessionNavigator.press("Home");
    await expect(page.getByRole("status", { name: "当前影子交易日详情" })).toContainText("2026-08-25");
    await sessionNavigator.press("End");
    await expect(page.getByRole("status", { name: "当前影子交易日详情" })).toContainText("2026-08-31");
    await expect(page.getByRole("button", { name: /下单|提交交易|买入|卖出|审批|验证|晋级|自进化/ })).toHaveCount(0);
    const overflow = await page.evaluate(() => ({
      clientWidth: document.documentElement.clientWidth,
      scrollWidth: document.documentElement.scrollWidth,
      offenders: [...document.querySelectorAll<HTMLElement>("body *")]
        .map((element) => ({
          className: element.className,
          right: Math.round(element.getBoundingClientRect().right),
          tagName: element.tagName,
        }))
        .filter((item) => item.right > document.documentElement.clientWidth + 1)
        .slice(0, 8),
    }));
    expect(overflow.scrollWidth, JSON.stringify(overflow)).toBeLessThanOrEqual(overflow.clientWidth);
    const shadowSerious = (await new AxeBuilder({ page }).analyze()).violations.filter((violation) => violation.impact === "serious" || violation.impact === "critical");
    expect(shadowSerious).toEqual([]);
    await page.getByRole("link", { name: "试算最小调整权重" }).click();
    await expect(page.getByRole("region", { name: "投资决策证据链，可横向滚动" }).getByRole("listitem").filter({ hasText: "情景试算" })).toHaveAttribute("aria-current", "step");
    await expect(page.getByRole("heading", { name: "持仓权重情景试算" })).toBeVisible();
    await expect(page.getByRole("textbox", { name: "AAPL 目标权重（0–1）" })).toHaveValue("0.15");
    await expect(page.getByRole("button", { name: /下单|交易|买入|卖出/ })).toHaveCount(0);

    if (viewport.name === "mobile-390" || viewport.name === "desktop-1440") {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: `test-results/what-if-${viewport.name}.png`, fullPage: true });
    }

    const serious = (await new AxeBuilder({ page }).analyze()).violations.filter((violation) => violation.impact === "serious" || violation.impact === "critical");
    expect(serious).toEqual([]);
  });
}

test("current price purpose blocks a new shadow before any start request", async ({ page }) => {
  const writes: string[] = [];
  page.on("request", (request) => {
    if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url());
  });
  await installFixtures(page);
  await page.route(`**/api/v1/optimization-proposals/${groupId}/shadow`, (route) => route.fulfill({
    status: 404, json: { error: { code: "NOT_FOUND", message: "Proposal Shadow was not started.",
      request_id: groupId, details: { reason: "SHADOW_NOT_STARTED" } } },
  }));
  await page.route(`**/api/v1/optimization-proposals/${groupId}`, (route) => route.fulfill({ json: {
    data: eligibleProposalGroup({ shadow_eligibility: {
      eligible: false, reasons: ["SHADOW_PRICE_AUTHORITY_UNAVAILABLE"],
    } }), meta,
  } }));
  await page.goto(`/optimize/${groupId}`);
  await expect(page.getByText(/当前价格证据尚不能用于新的前向观察/)).toBeVisible();
  await expect(page.getByRole("button", { name: "启动影子验证" })).toHaveCount(0);
  await expect(page.getByText("可进入影子验证")).toHaveCount(0);
  await expect(page.getByRole("table", { name: "保持现状、最小调整与策略目标的确定性比较" })).toBeVisible();
  expect(writes).toEqual([]);
});

test("no diagnostic trigger does not recommend keeping the portfolio", async ({ page }) => {
  await installFixtures(page, { ...diagnostic, optimization_need: "NO_ACTION", items: [] });
  await page.goto("/overview");
  await expect(page.getByRole("heading", { name: "未触发优化条件" })).toBeVisible();
  await expect(page.getByText(/该状态不代表已完成成本收益比较，也不代表保持现状更优/)).toBeVisible();
  await expect(page.getByRole("heading", { name: "保持现状", exact: true })).toHaveCount(0);
});

async function installFixtures(page: Page, report = diagnostic): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: meta.as_of, capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }], meta }) }));
  await page.route("**/api/v1/overview**", async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: { overview_mode: "DIAGNOSTIC", portfolio_id: portfolioId, portfolio_name: null, provider: null, portfolio_snapshot_id: snapshotId, portfolio_snapshot: null, snapshot_as_of: meta.as_of, trading_day: "2026-08-24", diagnostic_report: report, proposal_group: { proposal_group_id: groupId, status: "shadowing", version: 5 }, proposal_admission: { portfolio_snapshot_id: snapshotId, data_snapshot_id: "0198d500-1000-7000-8000-000000001309", cost_policy_id: "0198d500-1000-7000-8000-000000001311" }, proposal_admission_reason: null }, meta }) }));
  await page.route(`**/api/v1/optimization-proposals/${groupId}/shadow`, async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: shadowGroup(), meta }) }));
  await page.route(`**/api/v1/optimization-proposals/${groupId}`, async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: proposalGroup(), meta }) }));
}

function proposalGroup() {
  const variants = ["hold_current", "minimum_change", "strategy_target"] as const;
  return {
    proposal_group_id: groupId, status: "shadowing", version: 5, baseline_portfolio_snapshot_id: snapshotId,
    portfolio_scope: "real_read_only", diagnostic_input_hash: "b".repeat(64), data_snapshot_id: "0198d500-1000-7000-8000-000000001309", source_policy_id: "0198d500-1000-7000-8000-000000001310", cost_policy_id: "0198d500-1000-7000-8000-000000001311",
    proposals: variants.map((variant, index) => ({
      proposal_id: proposalIds[index], variant, analysis_only: true, strategy_evidence_complete: true, portfolio_scope: "real_read_only",
      positions: instrumentIds.map((instrument_id, positionIndex) => ({ instrument_id, display_symbol: positionIndex === 0 ? "AAPL" : null, current_weight: positionIndex === 0 ? "0.20" : "0.10", target_weight: positionIndex === 0 && variant !== "hold_current" ? "0.15" : positionIndex === 0 ? "0.20" : "0.10", action: positionIndex === 0 && variant !== "hold_current" ? "reduce" : "maintain", reason_code: positionIndex === 0 ? "CONCENTRATION_TOP1" : "BASELINE_HOLDING" })),
      comparison: comparison(false),
    })),
    shadow_eligibility: { eligible: true, reasons: [] },
  };
}

function shadowGroup() {
  const sessions = ["2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31"];
  const variants = ["hold_current", "minimum_change", "strategy_target"] as const;
  const known = (value: string) => ({ status: "known", value, reason: null });
  return {
    proposal_group_id: groupId,
    shadow_group_id: "0198d500-1000-7000-8000-000000001390",
    proposal_status: "shadowing",
    version: 5,
    start_session: sessions[0],
    evaluation_end_session: "2026-11-20",
    grace_end_session: "2026-11-30",
    coverage: { completed: 5, due: 5, required: 63, next_expected_session: "2026-09-01", missing_sessions: [] },
    freshness: { status: "current", latest_session: sessions[4], available_at: "2026-08-31T20:30:00Z", last_attempt_at: "2026-08-31T20:31:00Z", database_time: "2026-08-31T20:31:01Z", reason: null },
    sources: [
      { source: "fosun", kind: "price_bars", delivery_mode: "delayed", latest_session: sessions[4], available_at: "2026-08-31T20:30:00Z" },
      { source: "sec", kind: "corporate_actions", delivery_mode: "delayed", latest_session: sessions[4], available_at: "2026-08-31T20:29:00Z" },
    ],
    portfolios: variants.map((variant, variantIndex) => ({
      shadow_portfolio_id: [
        "0198d500-1000-7000-8000-000000001391",
        "0198d500-1000-7000-8000-000000001392",
        "0198d500-1000-7000-8000-000000001393",
      ][variantIndex],
      variant,
      nav: sessions.map((session, index) => ({ session, nav: (100 + index * (variantIndex + 1) * 0.125).toFixed(12) })),
      initial_outcomes: [{
        instrument_id: instrumentIds[0], side: "buy", status: "filled", reason: "filled at target-session open",
        fill: { side: "buy", quantity: "0.100000000000", price: "100.000000000000", notional: "10.000000000000", cost: "0.010000000000" },
      }],
    })),
    checkpoints: [{
      ordinal: 5,
      session: sessions[4],
      status: "complete",
      coverage_count: 5,
      reason: null,
      metrics: variants.map((variant) => ({
        variant,
        metrics: {
          net_cumulative_return: known("0.012000000000"),
          return_delta_vs_hold: known(variant === "hold_current" ? "0.000000000000" : "0.006000000000"),
          max_drawdown: known("-0.021000000000"),
          drawdown_improvement: known("0.004000000000"),
          annualized_volatility: known("0.143000000000"),
          volatility_improvement: known("0.011000000000"),
          worst_sessions: [{ session: "2026-08-27", return_value: "-0.012000000000" }],
          cumulative_turnover: known("0.084000000000"),
          fee_drag_index_points: known("0.014000000000"),
        },
      })),
    }],
    last_failed_attempt: null,
    sealed: false,
    seal_reason: null,
    safety: { creates_orders: false, trading_approval: false },
  };
}

function comparison(unknownCost: boolean) {
  const known = (value: string) => ({ status: "known", value, reason: null });
  const unknown = (reason: string) => ({ status: "unknown", value: null, reason });
  return { requires_fx: false, non_executable: unknownCost, cost_complete: !unknownCost, tax_impact: "unknown", metrics: { authority: { diagnostic_report_id: "0198d500-1000-7000-8000-000000001312", data_snapshot_id: "0198d500-1000-7000-8000-000000001309", source_policy_id: "0198d500-1000-7000-8000-000000001310", cost_policy_id: "0198d500-1000-7000-8000-000000001311", optimizer_policy_id: "0198d500-1000-7000-8000-000000001313" }, ranking_eligible: !unknownCost, constraint_complete: true, eligibility_complete: true, evidence_complete: !unknownCost, stale: false, unknown_metrics: unknownCost ? ["cost"] : [], dimensions: { hard_violations: 0, top1: known("0.15"), top3: known("0.25"), hhi: known("0.52"), sector_exposure: known("0.25"), currency_exposure: known("0.30"), volatility: unknown("VOLATILITY_POST_PROPOSAL_NOT_PROVEN"), drawdown: unknown("DRAWDOWN_POST_PROPOSAL_NOT_PROVEN"), correlation: unknown("CORRELATION_POST_PROPOSAL_NOT_PROVEN"), liquidity: known("0.90"), turnover: known("0.05"), cost: unknownCost ? unknown("COST_NOT_PROVEN") : known("0.001"), tax_impact: unknown("TAX_DATA_UNAVAILABLE") } } };
}
