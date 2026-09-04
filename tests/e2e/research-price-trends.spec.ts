import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { decisionTime, instrumentId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope, runId } from "../unit/research/fixtures";
import { trendInputs } from "../unit/research/trend-fixtures";

// Synthetic source DTO/model terminal state; browser never contacts a provider or model.
for (const viewport of [{ width: 375, height: 812 }, { width: 1440, height: 1000 }]) {
  test(`frozen trends remain separate from failed and exact old research at ${viewport.width}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
    const writes: string[] = [];
    const reads: string[] = [];
    page.on("request", (request) => {
      if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url());
      if (request.url().includes("research-admission")) reads.push(request.url());
    });
    await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {} }));
    const failed = researchEnvelope(researchData({ status: "failed", intent: null, inputs: null,
      quality_status: null, abstention_reason: "model_usage_uncertain" }));
    await page.route(`**/api/v1/instruments/${instrumentId}/research?**`, (route) => route.fulfill({ json: failed }));
    await page.route(`**/api/v1/research-runs/${runId}`, (route) => route.fulfill({ json: failed }));
    await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, (route) => route.fulfill({ json: researchAdmissionEnvelope(researchAdmissionData({
      data_snapshot_id: "0198d600-1000-7000-8000-000000001596", inputs: trendInputs(),
      eligible: false, reason: "prompt_authority_unavailable",
    })) }));
    await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
    const trend = page.getByRole("region", { name: "拆股调整价格趋势", exact: true });
    await expect(trend).toBeVisible();
    await expect(trend).toContainText("123.457 USD");
    await expect(trend).toContainText("<0.001%");
    await expect(trend).toContainText("部分指标资料不足");
    await expect(page.getByText(/当前资料与下方研究使用不同冻结快照/)).toBeVisible();
    await expect(page.getByRole("button", { name: /运行冻结研究|买入|卖出|下单/ })).toHaveCount(0);
    const summary = trend.getByText("趋势证据、身份与缺口", { exact: true });
    await summary.focus();
    await summary.press("Enter");
    await expect(summary.locator("..")).toHaveAttribute("open", "");
    await expect(trend.getByText("2026-07-01", { exact: true })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    const violations = (await new AxeBuilder({ page }).include(".decision-card").analyze()).violations;
    expect(violations.filter((item) => ["serious", "critical"].includes(item.impact ?? ""))).toEqual([]);
    await trend.evaluate((element) => { element.scrollIntoView({ block: "start" }); window.scrollBy(0, -80); });
    await page.screenshot({ path: `test-results/current-price-trends-${viewport.width}.png` });
    await page.getByRole("button", { name: "重新读取", exact: true }).click();
    await expect(trend).toBeVisible();
    const before = reads.length;
    await page.goto(`/research/${instrumentId}?run_id=${runId}`);
    await expect(page.getByText(/正在回放指定的历史研究/)).toBeVisible();
    await expect(page.getByRole("region", { name: "拆股调整价格趋势", exact: true })).toHaveCount(0);
    expect(reads).toHaveLength(before);
    expect(writes).toEqual([]);
  });
}
