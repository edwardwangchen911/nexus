import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { instrumentId, researchData, researchEnvelope, runId } from "../unit/research/fixtures";
import { concentrationResearch } from "../unit/research/concentration-fixtures";

// Only the HTTP read response is synthetic. The real page, card and evidence drawer run.
for (const viewport of [{ width: 375, height: 812 }, { width: 1440, height: 1000 }]) {
  test(`saved concentration observations remain separated at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
    const writes: string[] = [];
    page.on("request", (request) => { if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url()); });
    await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {} }));
    await page.route(`**/api/v1/research-runs/${runId}`, (route) => route.fulfill({ json: researchEnvelope(concentrationResearch()) }));
    const unknownRunId = "0198d600-1000-7000-8000-000000001596";
    const unknown = concentrationResearch("unknown");
    unknown.run_id = unknownRunId;
    await page.route(`**/api/v1/research-runs/${unknownRunId}`, (route) => route.fulfill({ json: researchEnvelope(unknown) }));
    const oldRunId = "0198d600-1000-7000-8000-000000001598";
    await page.route(`**/api/v1/research-runs/${oldRunId}`, (route) => route.fulfill({ json: researchEnvelope(researchData({ run_id: oldRunId })) }));
    await page.goto(`/research/${instrumentId}?run_id=${runId}`);
    const region = page.getByRole("region", { name: "组合预警核验", exact: true });
    await expect(region.getByRole("region", { name: "单一持仓预警", exact: true })).toContainText("已触发");
    await expect(region.getByRole("region", { name: "单一持仓严重预警", exact: true })).toContainText("未触发");
    await expect(region).toContainText("2026-08-25 00:00:00 北京时间");
    await expect(region).toContainText("不是当前实时状态");
    await expect(page.getByRole("region", { name: "失效条件", exact: true })).toContainText("这些失效条件尚无系统求值结果");
    const opener = region.getByRole("button", { name: "查看核验证据", exact: true }).first();
    await opener.focus();
    await opener.press("Enter");
    const drawer = page.getByRole("dialog");
    await expect(drawer).toContainText("单一持仓预警：已触发");
    await expect(drawer).toContainText("研究观察，不是买卖结论");
    await expect(drawer).toContainText("d".repeat(64));
    await drawer.press("Escape");
    await expect(opener).toBeFocused();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    const violations = (await new AxeBuilder({ page }).include("[aria-label='组合预警核验']").analyze()).violations;
    expect(violations.filter((item) => ["critical", "serious"].includes(item.impact ?? ""))).toEqual([]);
    await page.screenshot({ path: `test-results/concentration-observations-${viewport.width}.png` });
    await page.goto(`/research/${instrumentId}?run_id=${unknownRunId}`);
    await expect(region.getByText("无法核验", { exact: true })).toHaveCount(2);
    await expect(region.getByText("未触发", { exact: true })).toHaveCount(0);
    await page.goto(`/research/${instrumentId}?run_id=${oldRunId}`);
    await expect(page.getByRole("heading", { name: "委员会决策记录" })).toBeVisible();
    await expect(region).toHaveCount(0);
    await expect(page.getByRole("button", { name: /运行冻结研究|下单|买入|卖出/ })).toHaveCount(0);
    expect(writes).toEqual([]);
  });
}
