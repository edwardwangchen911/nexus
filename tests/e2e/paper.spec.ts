import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

import { cancellableOrder, filledOrder, meta, paperPortfolio } from "../unit/paper/fixtures";

for (const viewport of [
  { name: "desktop", width: 1440, height: 1000 },
  { name: "mobile", width: 390, height: 844 },
]) {
  test(`${viewport.name} paper console preserves execution authority and responsive containment`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await installPaperFixtures(page);
    await page.goto("/paper");

    await expect(page.getByRole("heading", { name: "内部模拟盘" })).toBeVisible();
    const currentStage = page.getByRole("region", { name: "投资决策证据链，可横向滚动" }).getByRole("listitem").filter({ hasText: "内部模拟" });
    await expect(currentStage).toHaveAttribute("aria-current", "step");
    await expect(currentStage).toBeInViewport();
    await expect(page.getByRole("region", { name: "模拟账户边界" })).toContainText("不是实盘");
    await expect(page.getByLabel("模拟账户绩效事实")).toContainText("USD 8,500.00");
    await expect(page.getByRole("table", { name: "内部模拟持仓" })).toContainText("alpaca / iex");
    const activeOrder = page.locator("article.paper-order").filter({ hasText: cancellableOrder.order_id });
    await expect(activeOrder.getByRole("table", { name: "AAPL 模拟成交事实" })).toContainText("10 基点");
    await expect(page.getByRole("button", { name: "取消 AAPL 模拟订单" })).toBeVisible();
    await expect(page.getByRole("button", { name: /创建订单|下单|实盘买入|实盘卖出/ })).toHaveCount(0);

    const eventSequence = await activeOrder.getByRole("list", { name: "AAPL 订单事件时间线" })
      .getByRole("listitem").evaluateAll((items) => items.map((item) => item.getAttribute("data-event-seq")));
    expect(eventSequence).toEqual(["1", "2", "3", "4"]);

    const positionScroll = page.getByRole("region", { name: "内部模拟持仓表，可横向滚动" });
    await positionScroll.focus();
    await expect(positionScroll).toBeFocused();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);

    const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(serious).toEqual([]);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: `test-results/paper-${viewport.name}.png`, fullPage: true });
  });
}

test("paper cancellation keeps focus, event version, and idempotency explicit", async ({ page }) => {
  let cancellationBody: unknown = null;
  let cancellationKey: string | undefined;
  let portfolioReads = 0;
  await installPaperFixtures(page, {
    cancelHandler: async (route) => {
      cancellationBody = route.request().postDataJSON();
      cancellationKey = route.request().headers()["idempotency-key"];
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          data: {
            order_id: cancellableOrder.order_id,
            status: "cancelled",
            filled_quantity: "2.000000000000",
            remaining_quantity: "3.000000000000",
            result_event_seq: 5,
            replayed: false,
          },
          meta: meta(),
        }),
      });
    },
    onPortfolioRead: () => { portfolioReads += 1; },
  });
  await page.goto("/paper");

  const cancel = page.getByRole("button", { name: "取消 AAPL 模拟订单" });
  await cancel.click();
  const dialog = page.getByRole("dialog", { name: "确认取消 AAPL 模拟订单" });
  await expect(dialog.getByRole("button", { name: "返回订单" })).toBeFocused();
  await expect(dialog).toContainText("事件版本 4");
  await dialog.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(cancel).toBeFocused();

  await cancel.click();
  await page.getByRole("button", { name: "确认取消模拟订单" }).click();
  await expect(page.getByText("模拟订单已取消；已重新读取服务端事实。")).toBeVisible();
  expect(cancellationBody).toEqual({ expected_event_seq: 4 });
  expect(cancellationKey).toMatch(/^paper-cancel-[0-9a-f-]{36}$/);
  expect(portfolioReads).toBeGreaterThanOrEqual(2);
});

for (const [dataState, visibleLabel] of [
  ["PARTIAL", "部分归因证据不完整"] as const,
  ["STALE", "显示最后已知快照"] as const,
]) {
  test(`${dataState} paper facts remain visible but cancellation fails closed`, async ({ page }) => {
    await installPaperFixtures(page, { portfolio: { ...paperPortfolio, data_state: dataState } });
    await page.goto("/paper");

    await expect(page.getByText(new RegExp(visibleLabel))).toBeVisible();
    await expect(page.getByRole("table", { name: "内部模拟持仓" })).toContainText("AAPL");
    await expect(page.getByRole("button", { name: "取消 AAPL 模拟订单" })).toHaveCount(0);
  });
}

test("paper console does not retain facts after an authoritative load error", async ({ page }) => {
  await installPaperFixtures(page, { portfolioStatus: 502 });
  await page.goto("/paper");

  await expect(page.getByRole("region", { name: "内部模拟盘控制台" })).toHaveAttribute("data-state", "error");
  await expect(page.getByRole("region", { name: "模拟账户边界" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "取消 AAPL 模拟订单" })).toHaveCount(0);
});

test("paper order pagination appends one bounded signed-cursor page", async ({ page }) => {
  const cursors: Array<string | null> = [];
  await installPaperFixtures(page, {
    ordersHandler: async (route) => {
      const cursor = new URL(route.request().url()).searchParams.get("cursor");
      cursors.push(cursor);
      const data = cursor === null
        ? { items: [cancellableOrder], next_cursor: "signed-next" }
        : { items: [{ ...filledOrder, order_id: "0198f7a0-1111-7777-8111-111111111129", symbol: "MSFT" }], next_cursor: null };
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data, meta: meta() }) });
    },
  });
  await page.goto("/paper");

  await page.getByRole("button", { name: "加载更多订单" }).click();
  await expect(page.getByRole("heading", { name: /MSFT/ })).toBeVisible();
  expect(cursors).toEqual([null, "signed-next"]);
});

type PaperFixtureOptions = {
  cancelHandler?: Parameters<Page["route"]>[1];
  onPortfolioRead?: () => void;
  ordersHandler?: Parameters<Page["route"]>[1];
  portfolio?: typeof paperPortfolio;
  portfolioStatus?: number;
};

async function installPaperFixtures(
  page: Page,
  options: PaperFixtureOptions = {},
): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: [{
        provider: "fosun",
        mode: "fixture",
        quality: "PASS",
        as_of: "2026-08-26T20:00:00Z",
        capability_expires_at: null,
        contract_hash: null,
        next_action: "仅读研究模式。",
        version: 1,
      }],
      meta: meta(),
    }),
  }));
  await page.route("**/api/v1/paper/portfolio", async (route) => {
    options.onPortfolioRead?.();
    const status = options.portfolioStatus ?? 200;
    await route.fulfill({
      status,
      contentType: "application/json",
      body: status === 200
        ? JSON.stringify({ data: options.portfolio ?? paperPortfolio, meta: meta() })
        : JSON.stringify({ error: { code: "UPSTREAM_ERROR", message: "paper fixture unavailable" }, meta: meta() }),
    });
  });
  await page.route("**/api/v1/paper/orders?**", options.ordersHandler ?? (async (route) => route.fulfill({
    status: 200, contentType: "application/json",
    body: JSON.stringify({ data: { items: [cancellableOrder, filledOrder], next_cursor: null }, meta: meta() }),
  })));
  if (options.cancelHandler) await page.route("**/api/v1/paper/orders/*/cancel", options.cancelHandler);
}
