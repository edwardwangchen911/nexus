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
  { name: "mobile", width: 390, height: 844 },
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
    await expect(page.getByRole("button", { name: "同步复星只读事实" })).toBeEnabled();
    await expect(page.getByText(/折算总资产/)).toHaveCount(0);

    const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(serious).toEqual([]);
  });
}

test("manual sync is read-only, idempotent, and visibly asynchronous", async ({ page }) => {
  let syncCalls = 0;
  const keys: Array<string | null> = [];
  await installPortfolioFixtures(page, async (route) => {
    syncCalls += 1;
    keys.push(route.request().headers()["idempotency-key"] ?? null);
    await route.fulfill({
      status: 202,
      contentType: "application/json",
      headers: { location: "/api/v1/broker-sync-runs/018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705" },
      body: JSON.stringify({ data: { broker_sync_run_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705", portfolio_id: portfolioId, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: "2026-08-23T08:01:00Z", completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null }, meta: meta() }),
    });
  });
  await page.goto("/portfolio");
  await page.getByRole("button", { name: "同步复星只读事实" }).click();

  await expect(page.getByText("复星只读同步任务已登记。当前快照不会被静默覆盖。")).toBeVisible();
  expect(syncCalls).toBe(1);
  expect(keys[0]).toBeTruthy();
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

async function installPortfolioFixtures(
  page: Page,
  syncHandler?: Parameters<Page["route"]>[1],
): Promise<void> {
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
  if (syncHandler) await page.route("**/api/v1/broker-sync-runs", syncHandler);
}

function meta() {
  return { request_id: requestId, schema_version: "1", as_of: "2026-08-23T08:00:00Z" };
}
