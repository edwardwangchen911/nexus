import { expect, test, type Page } from "@playwright/test";

import { assertReleaseAccessibility, releaseViewports } from "./helpers/accessibility";

const instrumentId = "0198fb00-1111-7777-8111-111111111111";

for (const viewport of releaseViewports) {
  test(`${viewport.name} release accessibility gates stay closed`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await installShellFixture(page);
    await page.route("**/api/v1/overview**", async (route) => route.fulfill({
      status: 404, contentType: "application/json", body: JSON.stringify({ error: { code: "NOT_FOUND" } }),
    }));
    await page.goto("/overview");

    const skipLink = page.getByRole("link", { name: "跳到主要内容" });
    await page.keyboard.press("Tab");
    await expect(skipLink).toBeFocused();
    await skipLink.press("Enter");
    await expect(page.locator("#main-content")).toBeFocused();
    await assertReleaseAccessibility(page);

    if (viewport.name === "mobile-390" || viewport.name === "desktop-1440") {
      await page.screenshot({ path: `test-results/accessibility-overview-${viewport.name}.png`, fullPage: true });
    }
  });
}

test("client-side route changes focus the main landmark with a visible outline", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await installShellFixture(page);
  await page.route("**/api/v1/overview**", async (route) => route.fulfill({ status: 404, contentType: "application/json", body: "{}" }));
  await page.route("**/api/v1/market?**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ data: { items: [], next_cursor: null, selected_series: null, intraday_monitor: null }, meta: meta() }),
  }));
  await page.goto("/overview");
  await expect(page.getByRole("heading", { name: "尚无可用诊断" })).toBeVisible();

  await page.getByRole("link", { name: "市场", exact: true }).click();

  await expect(page).toHaveURL(/\/market$/);
  const main = page.locator("#main-content");
  await expect(main).toBeFocused();
  const focusStyle = await main.evaluate((element) => {
    const style = getComputedStyle(element);
    return { style: style.outlineStyle, width: Number.parseFloat(style.outlineWidth) };
  });
  expect(focusStyle.style).toBe("solid");
  expect(focusStyle.width).toBeGreaterThanOrEqual(3);
});

test("market sticky focus and local table controls remain keyboard-visible", async ({ page }) => {
  await page.setViewportSize({ width: 1024, height: 768 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  await installShellFixture(page);
  await installMarketFixture(page);
  await page.goto("/market");

  const instrument = page.locator(".market-instrument").first();
  await instrument.focus();
  await page.evaluate(() => scrollTo(0, document.documentElement.scrollHeight));
  const geometry = await instrument.evaluate((element) => {
    const rect = element.getBoundingClientRect();
    const style = getComputedStyle(element);
    return { top: rect.top, bottom: rect.bottom, viewport: innerHeight, outline: style.outlineStyle };
  });
  expect(geometry.top).toBeGreaterThanOrEqual(0);
  expect(geometry.bottom).toBeLessThanOrEqual(geometry.viewport);
  expect(geometry.outline).toBe("solid");

  await page.setViewportSize({ width: 390, height: 844 });
  const tableRegion = page.getByRole("region", { name: "AAPL OHLCV 表格，可横向滚动" });
  await tableRegion.scrollIntoViewIfNeeded();
  const before = await tableRegion.evaluate((element) => element.scrollLeft);
  const right = page.getByRole("button", { name: "向右浏览表格" });
  await right.focus();
  await right.press("Enter");
  await expect.poll(() => tableRegion.evaluate((element) => element.scrollLeft)).toBeGreaterThan(before);
});

test("an authoritative overview error moves focus to the error summary", async ({ page }) => {
  await installShellFixture(page);
  await page.route("**/api/v1/overview**", async (route) => route.fulfill({
    status: 502, contentType: "application/json", body: JSON.stringify({ error: { code: "UPSTREAM_ERROR" } }),
  }));
  await page.goto("/overview");

  const summary = page.locator(".data-state[data-state='error']");
  await expect(summary).toHaveAttribute("role", "alert");
  await expect(summary).toBeFocused();
  await expect(summary.getByRole("heading", { name: "概览暂时不可用" })).toBeVisible();
});

test("a 200 percent equivalent CSS viewport reflows without two-dimensional page scrolling", async ({ page }) => {
  // 1440px at 200% browser zoom yields a 720 CSS-pixel layout viewport.
  await page.setViewportSize({ width: 720, height: 500 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  await installShellFixture(page);
  await page.route("**/api/v1/overview**", async (route) => route.fulfill({
    status: 404, contentType: "application/json", body: JSON.stringify({ error: { code: "NOT_FOUND" } }),
  }));
  await page.goto("/overview");

  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  await expect(page.getByRole("link", { name: "研究美股证据" })).toBeVisible();
});

async function installShellFixture(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: "2026-08-27T00:00:00Z", capability_expires_at: null, contract_hash: null, next_action: "仅读研究模式。", version: 1 }],
      meta: meta(),
    }),
  }));
}

async function installMarketFixture(page: Page): Promise<void> {
  await page.route("**/api/v1/market?**", async (route) => {
    const selected = new URL(route.request().url()).searchParams.get("selected_instrument_id");
    const item = { instrument_id: instrumentId, symbol: "AAPL", price: "227.160000000000", source: "alpaca", feed: "iex", delivery_mode: "delayed", as_of: "2026-08-26T20:00:00Z", quality: "PASS", quality_reasons: [], watchlisted: false };
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        data: {
          items: [item],
          next_cursor: null,
          selected_series: selected === instrumentId ? {
            instrument_id: instrumentId, source: "alpaca", feed: "iex", delivery_mode: "delayed", adjustment: "raw", interval: "1Day", quality: "PASS", quality_reasons: [],
            bars: [
              { session: "2026-08-25", open: "224.000000000000", high: "228.000000000000", low: "223.000000000000", close: "226.000000000000", volume: "50000000.000000000000", available_at: "2026-08-26T00:01:00Z", observed_at: "2026-08-25T20:00:00Z", quality_flags: [] },
              { session: "2026-08-26", open: "226.000000000000", high: "229.000000000000", low: "225.000000000000", close: "227.160000000000", volume: "51000000.000000000000", available_at: "2026-08-27T00:01:00Z", observed_at: "2026-08-26T20:00:00Z", quality_flags: [] },
            ],
          } : null,
          intraday_monitor: null,
        },
        meta: meta(),
      }),
    });
  });
}

function meta() {
  return { request_id: "0198fb00-1111-7777-8111-111111111199", schema_version: "1", as_of: "2026-08-27T00:00:00Z" };
}
