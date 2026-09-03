import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page, type Route } from "@playwright/test";

import { envelope, intentId, readyWorkbench } from "../unit/broker-simulation/fixtures";
import { assertReleaseAccessibility, releaseViewports } from "./helpers/accessibility";

for (const viewport of releaseViewports) {
  test(`${viewport.name} broker simulation evidence remains bounded and accessible`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await installBrokerFixtures(page);
    await page.goto("/broker-simulation");

    await expect(page.getByRole("region", { name: "复星模拟盘审核工作台" })).toHaveAttribute("data-state", "ready", { timeout: 15_000 });
    const currentStage = page.getByRole("region", { name: "投资决策证据链，可横向滚动" }).getByRole("listitem").filter({ hasText: "离线审核" });
    await expect(currentStage).toHaveAttribute("aria-current", "step");
    await expect(currentStage).toBeInViewport();
    await expect(page.getByText("实盘关闭").first()).toBeVisible();
    await expect(page.getByText("USD 1,200.00")).toBeVisible();
    await expect(page.getByText("外部连接未激活")).toBeVisible();
    await expect(page.getByRole("button", { name: /派发|修改订单|取消订单/ })).toHaveCount(0);
    const orderRegion = page.getByRole("region", { name: "服务端订单批次，可横向滚动" });
    await orderRegion.focus();
    await expect(orderRegion).toBeFocused();
    await assertReleaseAccessibility(page);

    if (viewport.name === "mobile-390" || viewport.name === "desktop-1440") {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: `test-results/broker-simulation-${viewport.name}.png`, fullPage: true });
    }
  });
}

test("200 percent reflow keeps the workbench inside the viewport", async ({ page }) => {
  // 1440px at 200% browser zoom yields a 720 CSS-pixel layout viewport.
  await page.setViewportSize({ width: 720, height: 500 });
  await installBrokerFixtures(page);
  await page.goto("/broker-simulation");
  await expect(page.getByText("USD 1,200.00")).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
    (violation) => violation.impact === "serious" || violation.impact === "critical",
  );
  expect(serious).toEqual([]);
});

test("local approval submits the sealed hash and never creates dispatch", async ({ page }) => {
  const writes: Array<{ body: unknown; ifMatch?: string; key?: string }> = [];
  await installBrokerFixtures(page, async (route) => {
    const request = route.request();
    writes.push({
      body: request.postDataJSON(),
      ifMatch: request.headers()["if-match"],
      key: request.headers()["idempotency-key"],
    });
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        data: { resource_id: intentId, action: "approve", status: "approved", version: 2, replayed: false, dispatch_created: false },
        meta: envelope().meta,
      }),
    });
  });
  await page.goto("/broker-simulation");
  const approve = page.getByRole("button", { name: "审阅并批准离线批次" });
  await approve.click();
  const dialog = page.getByRole("dialog", { name: "确认批准离线模拟批次" });
  await expect(dialog.getByRole("button", { name: "返回审阅" })).toBeFocused();
  await dialog.getByRole("button", { name: "确认离线批准" }).click();

  await expect(page.getByText("离线批次已批准；未创建派发任务。")).toBeVisible();
  expect(writes).toHaveLength(1);
  expect(writes[0].body).toEqual({ version: 1 });
  expect(writes[0].ifMatch).toBe("a".repeat(64));
  expect(writes[0].key).toMatch(/^broker-approve-/);
  await expect(page.getByRole("button", { name: /派发|修改订单|取消订单/ })).toHaveCount(0);
});

async function installBrokerFixtures(page: Page, actionHandler?: (route: Route) => Promise<void>): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: "2026-08-28T13:00:02Z", capability_expires_at: null, contract_hash: null, next_action: "仅离线证据。", version: 1 }],
      meta: envelope().meta,
    }),
  }));
  await page.route("**/api/v1/broker-simulation", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify(envelope(readyWorkbench)),
  }));
  await page.route(`**/api/v1/broker-simulation/intents/${intentId}/approve`, async (route) => {
    if (actionHandler) await actionHandler(route);
    else await route.fulfill({ status: 405, contentType: "application/json", body: "{}" });
  });
}
