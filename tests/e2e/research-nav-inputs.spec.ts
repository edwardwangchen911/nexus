import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { decisionTime, instrumentId, navHistoryInputs, requestId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope } from "../unit/research/fixtures";

for (const viewport of [{ width: 390, height: 844 }, { width: 1440, height: 1000 }]) {
  test(`frozen NAV remains read-only and distinct from prices at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
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
      inputs: navHistoryInputs(), eligible: false, reason: "prompt_authority_unavailable",
    })) }));
    await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
    const region = page.getByRole("region", { name: "发行商净值历史资料" });
    await expect(region).toBeVisible();
    await expect(page.getByText("资料已冻结 · 研究待生成")).toBeVisible();
    await expect(region).toContainText("不是市场成交价格或总收益序列");
    for (const label of ["查看净值观察记录", "净值证据与来源"]) {
      const disclosure = region.getByText(label, { exact: true });
      await disclosure.focus();
      await disclosure.press("Enter");
    }
    const table = region.getByRole("table", { name: "发行商净值原始观察" });
    await expect(table).toContainText("12.346");
    await expect(table).toContainText("<0.001");
    await expect(table).toContainText("未知");
    const scroll = region.getByRole("region", { name: "发行商净值表，可横向滚动" });
    await scroll.focus();
    await expect(scroll).toBeFocused();
    if (viewport.width === 390) {
      const before = await scroll.evaluate((element) => element.scrollLeft);
      await scroll.press("ArrowRight");
      await expect.poll(() => scroll.evaluate((element) => element.scrollLeft)).toBeGreaterThan(before);
    }
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    const violations = (await new AxeBuilder({ page }).include("[aria-label='发行商净值历史资料']").analyze()).violations;
    expect(violations.filter((item) => ["serious", "critical"].includes(item.impact ?? ""))).toEqual([]);
    await expect(page.getByRole("button", { name: /运行冻结研究|买入|卖出|下单/ })).toHaveCount(0);
    expect(writes).toEqual([]);
    await page.screenshot({ path: `test-results/nav-inputs-${viewport.width}.png`, fullPage: true });
  });
}

test("new NAV inputs remain visibly separate from a saved failed research run", async ({ page }) => {
  const writes: string[] = [];
  page.on("request", (request) => {
    if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url());
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {} }));
  await page.route(`**/api/v1/instruments/${instrumentId}/research?**`, (route) => route.fulfill({ json: researchEnvelope(researchData({
    status: "failed", intent: null, quality_status: null, abstention_reason: "model_usage_uncertain",
  })) }));
  let different = true;
  await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, (route) => route.fulfill({ json: researchAdmissionEnvelope(researchAdmissionData({
    inputs: navHistoryInputs(), eligible: false, reason: "prompt_authority_unavailable",
    ...(different ? { data_snapshot_id: "0198d600-1000-7000-8000-000000001588" } : {}),
  })) }));
  await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
  const notice = page.getByText(/当前资料与下方研究使用不同冻结快照/);
  await expect(notice).toBeVisible();
  await expect(page.getByRole("region", { name: "发行商净值历史资料" })).toBeVisible();
  await expect(page.getByRole("region", { name: "个股研究证据页" })).toHaveAttribute("data-state", "partial");
  await expect(page.getByRole("button", { name: /运行冻结研究|下单/ })).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  different = false;
  await page.getByRole("button", { name: "重新读取", exact: true }).click();
  await expect(notice).toHaveCount(0);
  expect(writes).toEqual([]);
});
