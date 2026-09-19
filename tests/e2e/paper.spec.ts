import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";
import { managementSummary, managementPlan } from "../unit/paper/management-fixtures";

test("current target without an occupying plan links to independent actual account facts", async ({ page }) => {
  await installPaperFixtures(page);
  await page.route("**/api/v1/paper/management/portfolio", (route) => route.fulfill({ json: { data: { ...managementSummary, occupying_plan: null }, meta: meta() } }));
  await page.goto("/paper");
  const target = page.getByRole("region", { name: "当前目标详情" });
  await expect(target).toContainText("当前无资源占用计划");
  await expect(target.getByRole("table")).toContainText("80%");
  await target.getByRole("link", { name: "查看独立实际账户事实" }).click();
  await expect(page).toHaveURL(/#paper-actual-facts$/);
  await expect(page.getByRole("region", { name: "独立实际账户事实" })).toBeVisible();
  await expect(page.getByRole("region", { name: "当前调整计划" })).toHaveCount(0);
});

for (const viewport of [{ width: 1280, height: 1066 }, { width: 390, height: 844 }]) {
  test(`portfolio plan has separate resource facts at ${viewport.width}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await installPaperFixtures(page);
    await page.route("**/api/v1/paper/management/portfolio", (route) => route.fulfill({ json: { data: managementSummary, meta: meta() } }));
    await page.route("**/api/v1/paper/plans/*", (route) => route.fulfill({ json: { data: managementPlan, meta: meta() } }));
    await page.goto("/paper");
    const plan = page.getByRole("region", { name: "当前调整计划" });
    await expect(plan).toContainText("资源仍占用");
    await expect(plan.getByRole("heading", { name: "调整计划 …0010" })).toBeVisible();
    await expect(plan).toContainText("USD 3,500.00");
    await expect(plan.getByRole("button", { name: "核对原操作结果" })).toBeDisabled();
    expect(await plan.evaluate((element) => {
      const check = element.querySelector(".pm-cancel")!;
      const timeline = element.querySelector(".pm-timeline")!;
      return Boolean(check.compareDocumentPosition(timeline) & Node.DOCUMENT_POSITION_FOLLOWING);
    })).toBe(true);
    if (viewport.width < 768) expect((await plan.locator(".pm-cancel").boundingBox())!.y).toBeLessThan((await plan.locator(".pm-timeline").boundingBox())!.y);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    await page.screenshot({ path: `test-results/task12-paper-${viewport.width}.png` });
    await page.evaluate(() => window.scrollTo(0, 0));
    const focused = await plan.boundingBox();
    expect(focused).not.toBeNull();
    await page.screenshot({ path: `test-results/task12-paper-focused-${viewport.width}.png`, fullPage: true, clip: focused! });
  });
}

import { agentPaperCycle, cancellableOrder, filledOrder, meta, paperPortfolio } from "../unit/paper/fixtures";

function evaluationSlots(paired = 0): Array<Record<string, unknown>> {
  return Array.from({ length: 24 }, (_, index) => ({
    phase: index < 4 ? "train" : "oos",
    decision_slot_at: new Date(Date.UTC(2026, 8, 18 + index, 14, 5)).toISOString(),
    status: index < paired ? "paired" : "scheduled",
    incumbent: { status: index < paired ? "completed" : "pending", result_code: index < paired ? "NO_ORDER_REQUIRED" : null },
    challenger: { status: index < paired ? "completed" : "pending", result_code: index < paired ? "NO_ORDER_REQUIRED" : null },
    reason_code: index < paired ? "PAIR_RECORDED" : "REGISTERED_SLOT_SCHEDULED",
  }));
}

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
    const versionEvaluation = page.getByRole("region", { name: "Agent 版本并行评价" });
    await expect(versionEvaluation).toContainText("执行锁定 · 券商写入关闭 · 实盘写入关闭");
    await expect(versionEvaluation.getByRole("button")).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "当日 Agent 模拟周期" })).toBeVisible();
    const agentCycle = page.getByRole("region", { name: "当日 Agent 模拟周期" });
    await expect(agentCycle).toContainText("决策报价 · Schwab Level One · 实时");
    await expect(agentCycle).toContainText("执行分钟线 · Schwab pricehistory · 实时性未由响应声明");
    await expect(agentCycle.getByRole("button")).toHaveCount(0);
    await expect(page.getByRole("region", { name: "Agent 模拟候选，可横向滚动" })).toContainText("AAPL");
    await expect(page.getByLabel("模拟账户绩效事实")).toContainText("USD 8,500.00");
    const comparison = page.getByRole("table", { name: "连续模拟四方对照" });
    await expect(comparison).toContainText("保持不动（现金）");
    await expect(comparison.getByText("缺少同窗前向事实")).toHaveCount(2);
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
    const comparisonScroll = page.getByRole("region", { name: "连续模拟四方对照表，可横向滚动" });
    await comparisonScroll.focus();
    await expect(comparisonScroll).toBeFocused();
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
  await page.route("**/api/v1/paper/cycles?**", async (route) => route.fulfill({
    status: 200, contentType: "application/json",
    body: JSON.stringify({ data: { items: [agentPaperCycle], summary: {
      decision_session: "2026-08-26", status: "completed", cycle_count: 1, completed_count: 1,
      latest_result_code: "NO_ORDER_REQUIRED", last_completed_decision_at: "2026-08-26T19:30:00Z",
      next_eligible_at: "2026-08-26T19:35:00Z", next_eligible_reason: "NEXT_SLOT", execution_locked: true,
    }, version_observation: null, version_history: [], version_evaluation: {
      evaluation_id: "71000000-0000-7000-8000-000000000099", status: "observing",
      incumbent_prompt_hash: "a".repeat(64), incumbent_model_hash: "b".repeat(64),
      challenger_prompt_hash: "c".repeat(64), challenger_model_hash: "d".repeat(64),
      train_completed: 2, oos_completed: 0, oos_missing: 20, oos_incomparable: 0,
      slots: evaluationSlots(2),
      incumbent_metrics: null, challenger_metrics: null,
      comparisons: [
        { key: "hold_current", data_state: "READY", reason_code: "CASH_HOLD_CURRENT" },
        { key: "baseline_v1", data_state: "UNKNOWN", reason_code: "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE" },
        { key: "market_benchmark", data_state: "UNKNOWN", reason_code: "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE" },
      ], gates: [], reason_code: "EVALUATION_OBSERVING",
      execution_locked: true, broker_write: false, live_write: false,
    } }, meta: meta() }),
  }));
  await page.route("**/api/v1/paper/orders?**", options.ordersHandler ?? (async (route) => route.fulfill({
    status: 200, contentType: "application/json",
    body: JSON.stringify({ data: { items: [cancellableOrder, filledOrder], next_cursor: null }, meta: meta() }),
  })));
  if (options.cancelHandler) await page.route("**/api/v1/paper/orders/*/cancel", options.cancelHandler);
}
