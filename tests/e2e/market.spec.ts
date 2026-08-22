import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const instrumentId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781";
const requestId = "0198f7a0-1111-7777-8111-111111111111";

const item = {
  instrument_id: instrumentId,
  symbol: "AAPL",
  price: "227.160000000000",
  source: "alpaca",
  feed: "iex",
  delivery_mode: "real_time",
  as_of: "2026-08-21T20:00:00Z",
  quality: "PASS",
  quality_reasons: [],
  watchlisted: false,
};

const selectedSeries = {
  instrument_id: instrumentId,
  source: "alpaca",
  feed: "sip",
  delivery_mode: "delayed",
  adjustment: "raw",
  interval: "1Day",
  quality: "PASS",
  quality_reasons: [],
  bars: [
    {
      session: "2026-08-20",
      open: "224.000000000000",
      high: "229.000000000000",
      low: "223.500000000000",
      close: "228.100000000000",
      volume: "52100000.000000000000",
      available_at: "2026-08-21T00:01:00Z",
      observed_at: "2026-08-20T20:00:00Z",
      quality_flags: [],
    },
    {
      session: "2026-08-21",
      open: "228.200000000000",
      high: "229.400000000000",
      low: "225.900000000000",
      close: "227.160000000000",
      volume: "46300000.000000000000",
      available_at: "2026-08-22T00:01:00Z",
      observed_at: "2026-08-21T20:00:00Z",
      quality_flags: [],
    },
  ],
};

for (const viewport of [
  { name: "desktop", width: 1440, height: 1000 },
  { name: "mobile", width: 390, height: 844 },
]) {
  test(`${viewport.name} market view preserves source evidence and keyboard access`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await installMarketFixtures(page);
    await page.goto("/market");

    await expect(page.getByRole("region", { name: "AAPL 报价证据" })).toContainText("ALPACA");
    await expect(page.getByRole("region", { name: "AAPL 报价证据" })).toContainText("实时");
    await expect(page.getByRole("region", { name: "AAPL OHLCV 证据" })).toContainText("ALPACA");
    await expect(page.getByRole("region", { name: "AAPL OHLCV 证据" })).toContainText("延迟");
    await expect(page.getByRole("region", { name: "AAPL 价格图表" })).toBeVisible();
    await expect(page.locator(".price-chart__canvas canvas").first()).toBeVisible();
    await expect(page.getByRole("table", { name: "AAPL OHLCV 数据" })).toContainText("227.160000000000");

    const previous = page.getByRole("button", { name: "上一根 K 线" });
    await previous.focus();
    await expect(previous).toBeFocused();
    await previous.press("Enter");
    await expect(page.getByLabel("当前 K 线详情")).toContainText("2026-08-20");

    await page.getByRole("button", { name: "将 AAPL 加入观察列表" }).click();
    await expect(page.getByText("AAPL 已加入观察列表。", { exact: true })).toBeVisible();

    const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(serious).toEqual([]);
  });
}

async function installMarketFixtures(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        data: [{
          provider: "fosun",
          mode: "fixture",
          quality: "PASS",
          as_of: "2026-08-22T00:00:00Z",
          capability_expires_at: null,
          contract_hash: null,
          next_action: "仅读研究模式。",
          version: 1,
        }],
        meta: { request_id: requestId, schema_version: "1", as_of: "2026-08-22T00:00:00Z" },
      }),
    });
  });
  await page.route("**/api/v1/market?**", async (route) => {
    const selected = new URL(route.request().url()).searchParams.get("selected_instrument_id");
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        data: {
          items: [item],
          next_cursor: null,
          selected_series: selected === instrumentId ? selectedSeries : null,
        },
        meta: { request_id: requestId, schema_version: "1", as_of: "2026-08-22T00:00:00Z" },
      }),
    });
  });
  await page.route(`**/api/v1/watchlist/${instrumentId}`, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        data: { instrument_id: instrumentId, watchlisted: route.request().method() === "PUT" },
        meta: { request_id: requestId, schema_version: "1", as_of: "2026-08-22T00:00:00Z" },
      }),
    });
  });
}
