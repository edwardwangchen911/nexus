import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { draftResearchContext, groupId, overviewData, proposalGroup } from "../unit/optimize/fixtures";

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
    const group = proposalGroup({ research_context: context });
    group.proposals[1].positions[0].display_symbol = "AAPL";
    group.proposals[2].positions = group.proposals[0].positions.map((position) => ({ ...position }));
    await page.route(`**/api/v1/optimization-proposals/${groupId}`, (route) => route.fulfill({ json: {
      data: group,
      meta: { request_id: groupId, schema_version: "1", as_of: context.valuation_time },
    } }));
    const initialRead = page.waitForResponse((response) =>
      new URL(response.url()).pathname === `/api/v1/optimization-proposals/${groupId}`
      && response.request().method() === "GET");
    await page.goto(`/optimize/${groupId}`);
    expect((await initialRead).status()).toBe(200);
    await expect(page.getByRole("region", { name: "三方案比较页面" })).toHaveAttribute("data-state", "ready");
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
    const allocations = page.getByRole("region", { name: /仓位目标与完整性$/ });
    await expect(allocations).toHaveCount(3);
    await expect(page.getByRole("region", { name: "最小调整仓位目标与完整性" })).toContainText("AAPL");
    await expect(page.getByRole("region", { name: "策略目标仓位目标与完整性" })).toContainText("策略目标尚未形成");
    await expect(page.getByText("标的名称未知", { exact: true }).first()).toBeVisible();
    const allocationTable = page.getByRole("region", { name: "最小调整仓位表，可横向滚动" });
    await allocationTable.focus();
    await expect(allocationTable).toBeFocused();
    await expect(page.getByRole("button", { name: /启动影子验证|下单|提交交易/ })).toHaveCount(0);
    await page.screenshot({ path: `test-results/frozen-research-${viewport.width}.png`, fullPage: true });
    const overflow = await page.evaluate(() => ({
      client: document.documentElement.clientWidth,
      scroll: document.documentElement.scrollWidth,
      containers: [...document.querySelectorAll<HTMLElement>("main, main > *, .proposal-comparison, .proposal-comparison > *, .proposal-research > *, .proposal-allocation")].map((element) => ({
        class: element.className, tag: element.tagName, width: element.getBoundingClientRect().width, right: element.getBoundingClientRect().right,
      })),
      offenders: [...document.querySelectorAll<HTMLElement>("body *")]
        .filter((element) => !element.closest(".decision-path, .table-scroll"))
        .filter((element) => element.getBoundingClientRect().right > document.documentElement.clientWidth + 1)
        .slice(0, 12).map((element) => ({ tag: element.tagName, class: element.className, width: element.getBoundingClientRect().width })),
    }));
    expect(overflow.scroll, JSON.stringify(overflow)).toBeLessThanOrEqual(overflow.client);
    const violations = (await new AxeBuilder({ page }).include(".proposal-comparison").analyze()).violations;
    expect(violations.filter((violation) => ["serious", "critical"].includes(violation.impact ?? ""))).toEqual([]);
    expect(writes).toEqual([]);
    await page.screenshot({ path: `test-results/frozen-research-${viewport.width}.png`, fullPage: true });
  });

  test(`no-group diagnosis stays read-only at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
    const requests: Array<{ method: string; url: string }> = [];
    page.on("request", (request) => {
      if (request.url().includes("/api/v1/")) requests.push({ method: request.method(), url: request.url() });
    });
    await page.route("**/api/v1/overview*", (route) => route.fulfill({ json: {
      data: overviewData({ proposal_group: null }),
      meta: { request_id: groupId, schema_version: "1", as_of: "2026-08-24T00:00:00Z" },
    } }));

    const initialRead = page.waitForResponse((response) =>
      new URL(response.url()).pathname === "/api/v1/overview"
      && response.request().method() === "GET");
    await page.goto("/optimize");
    expect((await initialRead).status()).toBe(200);
    await expect(page.getByRole("region", { name: "组合优化页面" })).toHaveAttribute("data-state", "ready");
    const availability = page.getByRole("region", { name: "历史方案状态" });
    await expect(availability).toContainText("当前没有历史三方案记录");
    await expect(availability).toContainText("诊断存在降低仓位条件");
    await expect(availability).toContainText("旧三方案只作为历史记录保留");
    await expect(page.getByText(/比较结果：|推荐/)).toHaveCount(0);
    await expect(page.getByRole("button", { name: /启动影子验证|下单|交易|买入|卖出/ })).toHaveCount(0);

    const readsBeforeRefresh = requests.filter((request) => request.method === "GET" && request.url.includes("/api/v1/overview")).length;
    const requestsBeforeRefresh = requests.length;
    await page.getByRole("button", { name: "刷新诊断" }).click();
    await expect.poll(() => requests.filter((request) => request.method === "GET" && request.url.includes("/api/v1/overview")).length)
      .toBe(readsBeforeRefresh + 1);
    expect(requests.filter((request) => !["GET", "HEAD"].includes(request.method))).toEqual([]);
    expect(requests.slice(requestsBeforeRefresh)).toEqual([
      expect.objectContaining({ method: "GET", url: expect.stringContaining("/api/v1/overview") }),
    ]);

    const width = await page.evaluate(() => ({
      client: document.documentElement.clientWidth,
      scroll: document.documentElement.scrollWidth,
    }));
    expect(width.scroll, JSON.stringify(width)).toBeLessThanOrEqual(width.client);
    const violations = (await new AxeBuilder({ page }).include(".optimize-page").analyze()).violations;
    expect(violations.filter((violation) => ["serious", "critical"].includes(violation.impact ?? ""))).toEqual([]);
    await page.screenshot({ path: `test-results/no-proposal-group-${viewport.width}.png`, fullPage: true });
  });
}
