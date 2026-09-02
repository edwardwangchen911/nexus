import { expect, test, type Page } from "@playwright/test";

import { assertReleaseAccessibility, releaseViewports } from "./helpers/accessibility";

const requestId = "0198f7a0-1111-7777-8111-111111111111";
const portfolioId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701";

const portfolio = {
  portfolio_id: portfolioId,
  name: "Fosun read-only",
  kind: "real_read_only",
  coverage_scope: "broker_account",
  provider: "fosun",
  latest_snapshot_status: "READY",
  latest_snapshot_as_of: "2026-08-23T08:00:00Z",
  original_currencies: ["HKD", "USD"],
  last_sync_run_status: "READY",
};

const snapshot = {
  portfolio_snapshot_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702",
  portfolio_id: portfolioId,
  coverage_scope: "broker_account",
  status: "READY",
  display_readiness: "DISPLAY_READY",
  decision_readiness: "DECISION_READY",
  as_of: "2026-08-23T08:00:00Z",
  cash: [
    { currency: "HKD", cash_balance: "8000.00", available_cash: "7000.00", provider_as_of: "2026-08-23T08:00:00Z" },
    { currency: "USD", cash_balance: "1000.00", available_cash: "900.00", provider_as_of: "2026-08-23T08:00:00Z" },
  ],
  positions: [{ instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703", symbol: "00656.HK", exchange: "XHKG", currency: "HKD", quantity: "3000", available_quantity: "2800", frozen_quantity: "200", avg_cost: "6.20", diluted_cost: "5.75", current_price: "6.50", provider_as_of: "2026-08-23T08:00:00Z", read_only: true }],
  open_orders: [],
  converted_view: null,
};

for (const viewport of releaseViewports) {
  test(`${viewport.name} portfolio keeps currencies and evidence boundaries visible`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await installPortfolioFixtures(page);
    await page.goto("/portfolio");

    await expect(page.getByRole("region", { name: "Fosun read-only HKD 组合摘要" })).toContainText("HKD 8,000.00", { timeout: 15_000 });
    await expect(page.getByRole("region", { name: "Fosun read-only USD 组合摘要" })).toContainText("USD 1,000.00");
    await expect(page.getByText("平均成本")).toBeVisible();
    await expect(page.getByText("摊薄成本")).toBeVisible();
    const authority = page.getByRole("region", { name: "数据与决策权威边界" });
    await expect(authority).toContainText("DISPLAY_READY");
    await expect(authority).toContainText("DECISION_READY");
    await expect(authority).toContainText("HUMAN APPROVAL");
    await expect(page.getByText(/香港交易所财报证据尚未准入/)).toBeVisible();
    await expect(page.getByRole("region", { name: "真实刷新仅限本机终端" }).getByText("最近本地刷新状态")).toBeVisible();
    await expect(page.getByRole("button", { name: "刷新页面数据" })).toBeEnabled();
    await expect(page.getByRole("button", { name: "同步复星只读事实" })).toHaveCount(0);
    await expect(page.getByText(/折算总资产/)).toHaveCount(0);
    const assets = page.getByRole("region", { name: "HKD 原币种资产估值" });
    await expect(assets.getByText("HKD 27,500.00", { exact: true })).toBeVisible();
    await expect(page.locator(".portfolio-workspace")).toHaveAttribute("data-single", "true");
    const metric = await assets.locator(".portfolio-valuation__facts dd").first().boundingBox();
    const authorityBox = await authority.boundingBox();
    expect(metric!.y + metric!.height).toBeLessThan(authorityBox!.y);
    if (viewport.name === "desktop-1440") {
      expect(authorityBox!.height).toBeLessThan(100);
      const row = assets.locator(".portfolio-allocation li").first();
      const amount = await row.locator("span").nth(1).boundingBox();
      const percent = await row.locator("strong").boundingBox();
      expect(Math.abs(amount!.y - percent!.y)).toBeLessThan(8);
    }
    if (viewport.width < 768) {
      expect(metric!.y + metric!.height).toBeLessThan(viewport.height);
      expect(await page.evaluate(() => document.documentElement.scrollHeight)).toBeLessThan(3200);
    }
    if (viewport.name === "desktop-1440" || viewport.name === "mobile-390") {
      await page.screenshot({ path: `test-results/asset-first-portfolio-${viewport.name}.png`, fullPage: true });
    }
    if (viewport.width < 768) {
      await page.locator(".app-shell__drawer > summary").click();
      await expect(page.getByRole("navigation", { name: "移动主导航" }).getByRole("link", { name: "组合" })).toHaveAttribute("aria-current", "page");
      await page.locator(".app-shell__drawer > summary").click();
    } else {
      const navigation = page.getByRole("navigation", { name: "主导航" });
      await expect(navigation.getByRole("link", { name: "组合" })).toHaveAttribute("aria-current", "page");
      if (viewport.name === "mobile-landscape") {
        const sidebar = page.locator(".app-shell__sidebar");
        await expect(sidebar).toHaveCSS("overflow-y", "auto");
        const lastLink = navigation.getByRole("link").last();
        await sidebar.locator(".app-shell__boundary").scrollIntoViewIfNeeded();
        await expect(lastLink).toBeVisible();
        const geometry = await page.evaluate(() => {
          const link = document.querySelector<HTMLElement>(".app-shell__sidebar nav:last-of-type a:last-of-type");
          const boundary = document.querySelector<HTMLElement>(".app-shell__boundary");
          if (!link || !boundary) return null;
          const linkRect = link.getBoundingClientRect();
          const boundaryRect = boundary.getBoundingClientRect();
          return { separated: linkRect.bottom <= boundaryRect.top, boundaryBottom: boundaryRect.bottom, viewportHeight: innerHeight };
        });
        expect(geometry).not.toBeNull();
        expect(geometry?.separated).toBe(true);
        expect(geometry?.boundaryBottom).toBeLessThanOrEqual(geometry?.viewportHeight ?? 0);
      }
    }
    await assertReleaseAccessibility(page);
  });
}

test("fail-closed readiness remains fully visible on a 390px viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installPortfolioFixtures(page, { decision_readiness: "RECONCILIATION_REQUIRED" });
  await page.goto("/portfolio");

  const value = page.getByRole("region", { name: "数据与决策权威边界" }).getByText("RECONCILIATION_REQUIRED", { exact: true });
  await expect(value).toBeVisible();
  expect(await value.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
  await expect(page.getByRole("button", { name: /买入|卖出|下单|修改持仓/ })).toHaveCount(0);
});

test("local refresh disclosure is keyboard-visible and never starts a sync", async ({ page }) => {
  const brokerSyncRequests: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/broker-sync-runs")) brokerSyncRequests.push(request.method());
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await installPortfolioFixtures(page);
  await page.goto("/portfolio");

  const disclosure = page.getByRole("region", { name: "真实刷新仅限本机终端" });
  const summary = disclosure.locator("summary");
  const command = disclosure.getByText("uv run --package nexus-engine nexus-fosun-local refresh", { exact: true });
  await expect(command).not.toBeVisible();
  await summary.focus();
  await page.keyboard.press("Enter");
  await expect(command).toBeVisible();
  expect(brokerSyncRequests).toEqual([]);
});

test("page refresh only reloads persisted facts and never admits a broker sync", async ({ page }) => {
  const brokerSyncRequests: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes("/api/v1/broker-sync-runs")) brokerSyncRequests.push(request.method());
  });
  await installPortfolioFixtures(page);
  await page.goto("/portfolio");
  await page.getByRole("button", { name: "刷新页面数据" }).click();

  await expect(page.getByRole("region", { name: "Fosun read-only HKD 组合摘要" })).toContainText("HKD 8,000.00");
  expect(brokerSyncRequests).toEqual([]);
  await expect(page.getByRole("button", { name: /买入|卖出|下单|修改持仓/ })).toHaveCount(0);
});

test("legacy Fosun holdings route permanently converges on the portfolio source view", async ({ page }) => {
  await installPortfolioFixtures(page);
  const response = await page.goto("/holdings/fosun");
  const redirectedFrom = response?.request().redirectedFrom();
  expect((await redirectedFrom?.response())?.status()).toBe(308);
  await expect(page).toHaveURL(/\/portfolio\?source=fosun$/);
  await expect(page.getByRole("region", { name: "组合事实页面" })).toBeVisible();
});

async function installPortfolioFixtures(page: Page, snapshotOverride: Partial<typeof snapshot> = {}): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200, contentType: "application/json",
    body: JSON.stringify({ data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: "2026-08-23T08:00:00Z", capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }], meta: meta() }),
  }));
  await page.route("**/api/v1/portfolios?**", async (route) => route.fulfill({
    status: 200, contentType: "application/json",
    body: JSON.stringify({ data: { items: [portfolio], next_cursor: null }, meta: meta() }),
  }));
  await page.route(`**/api/v1/portfolios/${portfolioId}/snapshots/latest`, async (route) => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify({ data: { ...snapshot, ...snapshotOverride }, meta: meta() }),
  }));
}

function meta() {
  return { request_id: requestId, schema_version: "1", as_of: "2026-08-23T08:00:00Z" };
}
