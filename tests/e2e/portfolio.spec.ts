import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

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
  as_of: "2026-08-23T08:00:00Z",
  cash: [
    { currency: "HKD", cash_balance: "8000.00", available_cash: "7000.00", provider_as_of: "2026-08-23T08:00:00Z" },
    { currency: "USD", cash_balance: "1000.00", available_cash: "900.00", provider_as_of: "2026-08-23T08:00:00Z" },
  ],
  positions: [{ instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703", symbol: "00656.HK", exchange: "XHKG", currency: "HKD", quantity: "3000", available_quantity: "2800", frozen_quantity: "200", avg_cost: "6.20", diluted_cost: "5.75", current_price: "6.50", provider_as_of: "2026-08-23T08:00:00Z", read_only: true }],
  open_orders: [],
  converted_view: null,
};

for (const viewport of [
  { name: "desktop", width: 1440, height: 1000 },
  { name: "mobile-375", width: 375, height: 812 },
  { name: "mobile-390", width: 390, height: 844 },
  { name: "tablet-768", width: 768, height: 1024 },
  { name: "desktop-1024", width: 1024, height: 768 },
  { name: "mobile-landscape", width: 844, height: 390 },
]) {
  test(`${viewport.name} portfolio keeps currencies and evidence boundaries visible`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await installPortfolioFixtures(page);
    await page.goto("/portfolio");

    await expect(page.getByRole("region", { name: "Fosun read-only HKD 组合摘要" })).toContainText("HKD 8,000.00");
    await expect(page.getByRole("region", { name: "Fosun read-only USD 组合摘要" })).toContainText("USD 1,000.00");
    await expect(page.getByText("平均成本")).toBeVisible();
    await expect(page.getByText("摊薄成本")).toBeVisible();
    await expect(page.getByText(/香港交易所财报证据尚未准入/)).toBeVisible();
    await expect(page.getByRole("region", { name: "真实刷新仅限本机终端" })).toContainText(
      "uv run --package nexus-engine nexus-fosun-local refresh",
    );
    await expect(page.getByRole("button", { name: "刷新页面数据" })).toBeEnabled();
    await expect(page.getByRole("button", { name: "同步复星只读事实" })).toHaveCount(0);
    await expect(page.getByText(/折算总资产/)).toHaveCount(0);

    const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(serious).toEqual([]);
  });
}

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

async function installPortfolioFixtures(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200, contentType: "application/json",
    body: JSON.stringify({ data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: "2026-08-23T08:00:00Z", capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }], meta: meta() }),
  }));
  await page.route("**/api/v1/portfolios?**", async (route) => route.fulfill({
    status: 200, contentType: "application/json",
    body: JSON.stringify({ data: { items: [portfolio], next_cursor: null }, meta: meta() }),
  }));
  await page.route(`**/api/v1/portfolios/${portfolioId}/snapshots/latest`, async (route) => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify({ data: snapshot, meta: meta() }),
  }));
}

function meta() {
  return { request_id: requestId, schema_version: "1", as_of: "2026-08-23T08:00:00Z" };
}
