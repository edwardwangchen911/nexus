import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { decisionTime, fundObservationInputs, instrumentId, requestId, researchAdmissionData, researchAdmissionEnvelope } from "../unit/research/fixtures";

for (const viewport of [{ width: 390, height: 844 }, { width: 1440, height: 1000 }]) {
  test(`frozen issuer inputs are readable without research or trades at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
    const inputs = fundObservationInputs();
    inputs.issuer_fund_observations[0].holdings[0].name = "Synthetic public holding ".repeat(8);
    inputs.issuer_fund_observations[0].total_holdings = 101;
    inputs.issuer_fund_observations[0].truncated = true;
    const writes: string[] = [];
    page.on("request", (request) => {
      if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url());
    });
    await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {
      error: { code: "NOT_FOUND", message: "No saved research.", request_id: requestId, details: {} },
    } }));
    await page.route("**/api/v1/integrations", (route) => route.fulfill({ json: {
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: decisionTime,
        capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }],
      meta: { request_id: requestId, schema_version: "1", as_of: decisionTime },
    } }));
    await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, (route) => route.fulfill({ json: researchAdmissionEnvelope(researchAdmissionData({
      inputs, eligible: false, reason: "prompt_authority_unavailable",
    })) }));
    await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
    const region = page.getByRole("region", { name: "发行商分类与持仓资料" });
    await expect(region).toBeVisible();
    await expect(page.getByText("资料已冻结 · 研究待生成")).toBeVisible();
    await expect(region).toContainText("显示 3 / 101 项持仓");
    for (const label of ["查看发行商行业分类", "查看发行商持仓", "分类与持仓证据来源"]) {
      const disclosure = region.getByText(label, { exact: true });
      await disclosure.focus();
      await disclosure.press("Enter");
    }
    await expect(region.getByRole("table", { name: "发行商行业分类原始权重" })).toContainText("100%");
    const holdings = region.getByRole("table", { name: "发行商持仓原始权重" });
    await expect(holdings).toContainText("-0.126%");
    await expect(holdings).toContainText("<0.001%");
    if (viewport.width === 1440) {
      await region.getByText("查看发行商持仓", { exact: true }).scrollIntoViewIfNeeded();
      await expect(holdings.getByText("-0.126%", { exact: true })).toBeInViewport();
    }
    const scroll = region.getByRole("region", { name: "发行商持仓表，可横向滚动" });
    await scroll.focus();
    await expect(scroll).toBeFocused();
    if (viewport.width === 390) {
      const before = await scroll.evaluate((element) => element.scrollLeft);
      await scroll.press("ArrowRight");
      await expect.poll(() => scroll.evaluate((element) => element.scrollLeft)).toBeGreaterThan(before);
    }
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    const violations = (await new AxeBuilder({ page }).include("[aria-label='发行商分类与持仓资料']").analyze()).violations;
    expect(violations.filter((item) => ["serious", "critical"].includes(item.impact ?? ""))).toEqual([]);
    await expect(page.getByRole("button", { name: /运行冻结研究|买入|卖出|下单/ })).toHaveCount(0);
    expect(writes).toEqual([]);
    await page.screenshot({ path: `test-results/issuer-inputs-${viewport.width}.png`, fullPage: true });
  });
}
