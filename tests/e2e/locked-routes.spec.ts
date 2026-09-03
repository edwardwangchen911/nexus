import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const routes = [
  { path: "/live-approvals", heading: "实盘逐笔批准", stage: "实盘交易未启用" },
] as const;

for (const viewport of [
  { name: "mobile-390", width: 390, height: 844 },
  { name: "desktop-1440", width: 1440, height: 1000 },
]) {
  for (const routeCase of routes) {
    test(`${viewport.name} ${routeCase.path} remains visibly and operationally locked`, async ({ page }) => {
      const apiWrites: string[] = [];
      page.on("request", (request) => {
        if (new URL(request.url()).pathname.startsWith("/api/") && request.method() !== "GET") {
          apiWrites.push(`${request.method()} ${new URL(request.url()).pathname}`);
        }
      });
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.emulateMedia({ reducedMotion: "reduce" });
      await installShellFixture(page);
      await page.goto(routeCase.path);

      await expect(page.getByRole("heading", { name: routeCase.heading })).toBeVisible();
      await expect(page.getByText("实盘能力 · 逐笔批准", { exact: true })).toBeVisible();
      await expect(page.getByText(routeCase.stage, { exact: true })).toBeVisible();
      await expect(page.locator("form, input, select, textarea")).toHaveCount(0);
      await expect(page.getByRole("button", { name: /下单|提交|批准|买入|卖出|订单|开启实盘/ })).toHaveCount(0);

      await page.keyboard.press("Tab");
      const skipLink = page.getByRole("link", { name: "跳到主要内容" });
      await expect(skipLink).toBeFocused();
      await skipLink.press("Enter");
      await expect(page.locator("#main-content")).toBeFocused();

      expect(apiWrites).toEqual([]);
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
      const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
        (violation) => violation.impact === "serious" || violation.impact === "critical",
      );
      expect(serious).toEqual([]);
      await page.screenshot({ path: `test-results/live-approvals-${viewport.name}.png`, fullPage: true });
    });
  }
}

async function installShellFixture(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: [{
        provider: "fosun",
        mode: "fixture",
        quality: "PASS",
        as_of: "2026-08-27T00:00:00Z",
        capability_expires_at: null,
        contract_hash: null,
        next_action: "仅读研究模式。",
        version: 1,
      }],
      meta: { request_id: "0198f7a0-1111-7777-8111-111111111111", schema_version: "1", as_of: "2026-08-27T00:00:00Z" },
    }),
  }));
}
