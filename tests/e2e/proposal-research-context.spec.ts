import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { draftResearchContext, groupId, proposalGroup } from "../unit/optimize/fixtures";

for (const viewport of [{ width: 375, height: 812 }, { width: 1440, height: 900 }]) {
  test(`frozen research stays readable at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
    const context = draftResearchContext();
    const writes: string[] = [];
    page.on("request", (request) => {
      if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url());
    });
    await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {
      error: { code: "NOT_FOUND", message: "fixture unavailable", request_id: groupId, details: {} },
    } }));
    await page.route(`**/api/v1/optimization-proposals/${groupId}`, (route) => route.fulfill({ json: {
      data: proposalGroup({ research_context: context }),
      meta: { request_id: groupId, schema_version: "1", as_of: context.valuation_time },
    } }));
    await page.goto(`/optimize/${groupId}`);
    await page.waitForLoadState("networkidle");
    const region = page.getByRole("region", { name: "冻结研究依据" });
    await expect(region).toBeVisible();
    await expect(region).toContainText("仅辅助阅读，不参与本草稿目标权重计算");
    await expect(region).toContainText("行动语义尚未通过校验");
    await expect(region).toContainText("2026-08-24 08:00:00 北京时间");
    await expect(region).toContainText("2026-08-25 08:10:00 北京时间");
    const disclosure = region.getByText("核对冻结研究版本", { exact: true });
    await disclosure.focus();
    await disclosure.press("Enter");
    await expect(region.getByText(context.output_hash, { exact: true })).toBeVisible();
    await expect(page.getByRole("button", { name: /启动影子验证|下单|提交交易/ })).toHaveCount(0);
    await page.screenshot({ path: `test-results/frozen-research-${viewport.width}.png`, fullPage: true });
    const overflow = await page.evaluate(() => ({
      client: document.documentElement.clientWidth,
      scroll: document.documentElement.scrollWidth,
      containers: [...document.querySelectorAll<HTMLElement>("main, main > *, .proposal-comparison, .proposal-comparison > *, .proposal-research > *")].map((element) => ({
        class: element.className, tag: element.tagName, width: element.getBoundingClientRect().width, right: element.getBoundingClientRect().right,
      })),
      offenders: [...document.querySelectorAll<HTMLElement>("body *")]
        .filter((element) => !element.closest(".decision-path, .table-scroll"))
        .filter((element) => element.getBoundingClientRect().right > document.documentElement.clientWidth + 1)
        .slice(0, 12).map((element) => ({ tag: element.tagName, class: element.className, width: element.getBoundingClientRect().width })),
    }));
    expect(overflow.scroll, JSON.stringify(overflow)).toBeLessThanOrEqual(overflow.client);
    const violations = (await new AxeBuilder({ page }).include(".proposal-research").analyze()).violations;
    expect(violations.filter((violation) => ["serious", "critical"].includes(violation.impact ?? ""))).toEqual([]);
    expect(writes).toEqual([]);
    await page.screenshot({ path: `test-results/frozen-research-${viewport.width}.png`, fullPage: true });
  });
}
