import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { decisionTime, instrumentId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope, runId } from "../unit/research/fixtures";
import { providerInputs } from "../unit/research/provider-fixtures";
import { portfolio, portfolioId, responseMeta, snapshot } from "../unit/portfolio/fixtures";

for (const viewport of [{ width: 375, height: 812 }, { width: 1440, height: 1000 }]) {
  test(`frozen company and macro sources remain distinct and read-only at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
    const writes: string[] = [];
    page.on("request", (request) => { if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url()); });
    await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {} }));
    const inputs = providerInputs();
    inputs.company_profiles[0].description = '<img src=x onerror="alert(1)"> ' + "完整冻结公司描述，不代表财务判断。".repeat(80);
    inputs.company_profiles[0].company_name = "Synthetic".repeat(30);
    inputs.company_profiles[0].industry = "原始细分行业".repeat(25);
    inputs.macro_observations[0].value = "-0.0000001";
    await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, (route) => route.fulfill({ json: researchAdmissionEnvelope(researchAdmissionData({
      inputs, eligible: false, reason: "prompt_authority_unavailable",
    })) }));
    await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
    const company = page.getByRole("region", { name: "公司背景资料", exact: true });
    const macro = page.getByRole("region", { name: "宏观观察资料", exact: true });
    await expect(company).toBeVisible();
    await expect(macro).toBeVisible();
    await expect(page.getByText("资料已冻结 · 研究待生成")).toBeVisible();
    await expect(page.getByText("FMP / FRED · 本地只读证据")).toBeVisible();
    await expect(company).toContainText("不是已验证的组合风控分类");
    await expect(macro).toContainText("(-0.001, 0)%");
    for (const [region, label] of [[company, "查看公司描述与标识"], [company, "公司证据与来源"], [macro, "DGS10 证据与日历绑定"]] as const) {
      const summary = region.getByText(label, { exact: true });
      await summary.focus();
      await summary.press("Enter");
      await expect(summary.locator("..")).toHaveAttribute("open", "");
    }
    await expect(company.locator("img, script")).toHaveCount(0);
    await expect(company.getByText(inputs.company_profiles[0].description, { exact: true })).toBeVisible();
    await expect(macro).toContainText("2026-08-20");
    await expect(macro).toContainText("2026-08-21");
    await expect(macro).toContainText("2026-08-25 01:30:00 北京时间");
    await expect(macro).toContainText("2026-08-25 02:30:00 北京时间");
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    const violations = (await new AxeBuilder({ page }).include(".decision-card").analyze()).violations;
    expect(violations.filter((item) => ["serious", "critical"].includes(item.impact ?? ""))).toEqual([]);
    await page.getByRole("button", { name: "重新读取", exact: true }).click();
    await expect(company).toBeVisible();
    await expect(page.getByRole("button", { name: /运行冻结研究|买入|卖出|下单/ })).toHaveCount(0);
    expect(writes).toEqual([]);
    await company.scrollIntoViewIfNeeded();
    await page.screenshot({ path: `test-results/provider-company-${viewport.width}.png` });
    await macro.scrollIntoViewIfNeeded();
    await page.screenshot({ path: `test-results/provider-macro-${viewport.width}.png` });
  });
}

test("portfolio separates the exact old report from updated frozen materials", async ({ page }) => {
  const writes: string[] = [];
  const reads: string[] = [];
  page.on("request", (request) => {
    if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url());
    if (request.url().includes("/api/v1/")) reads.push(new URL(request.url()).pathname);
  });
  await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {} }));
  await page.route("**/api/v1/portfolios?**", (route) => route.fulfill({ json: { data: { items: [portfolio], next_cursor: null }, meta: responseMeta() } }));
  await page.route(`**/api/v1/portfolios/${portfolioId}/snapshots/latest`, (route) => route.fulfill({ json: { data: { ...snapshot,
    positions: [{ ...snapshot.positions[0], instrument_id: instrumentId, symbol: "TEST", research: {
      status: "completed", run_id: runId, data_snapshot_id: snapshot.portfolio_snapshot_id,
      decision_time: "2026-08-20T20:00:00Z", read_at: decisionTime,
      quality_status: "partial", uncertainties: [], newer_inputs_available: true,
    } }],
  }, meta: responseMeta() } }));
  await page.route(`**/api/v1/research-runs/${runId}`, (route) => route.fulfill({ json: researchEnvelope(researchData({
    inputs: null, decision_time: "2026-08-20T20:00:00Z", status: "failed", intent: null, quality_status: null,
  })) }));
  await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, (route) => route.fulfill({ json: researchAdmissionEnvelope(researchAdmissionData({
    inputs: providerInputs(), eligible: false, reason: "prompt_authority_unavailable",
  })) }));
  await page.goto("/portfolio");
  const old = page.getByRole("link", { name: "查看 TEST 研究报告" });
  const updated = page.getByRole("link", { name: "查看 TEST 更新资料" });
  await expect(old).toHaveAttribute("href", `/research/${instrumentId}?run_id=${runId}`);
  await expect(updated).toHaveAttribute("href", `/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
  await old.click();
  await expect(page.getByText(/正在回放指定的历史研究/)).toBeVisible();
  await expect(page.getByRole("region", { name: "公司背景资料", exact: true })).toHaveCount(0);
  expect(reads.filter((url) => url.includes("research-admission"))).toEqual([]);
  await page.goBack();
  await page.getByRole("link", { name: "查看 TEST 更新资料" }).click();
  await expect(page.getByRole("region", { name: "公司背景资料", exact: true })).toBeVisible();
  await expect(page.getByRole("region", { name: "宏观观察资料", exact: true })).toBeVisible();
  await expect(page.getByText(/研究资料已冻结，AI 研究尚未生成/)).toBeVisible();
  await expect(page.getByRole("button", { name: /运行冻结研究|买入|卖出|下单/ })).toHaveCount(0);
  expect(writes).toEqual([]);
});
