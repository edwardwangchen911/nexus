import { expect, test, type Page } from "@playwright/test";

import type { components } from "../../app/lib/generated/schema";
import { agentPaperCycle, cancellableOrder, meta, paperPortfolio } from "../unit/paper/fixtures";
import { assertReleaseAccessibility } from "./helpers/accessibility";

type WorkbenchPayload = components["schemas"]["WorkbenchEnvelope"];

test("opens the latest decision from the workbench as an ordered read-only ledger", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  const methods: string[] = [];
  const browserErrors: string[] = [];
  page.on("console", (message) => { if (message.type() === "error") browserErrors.push(message.text()); });
  await installFixtures(page, methods);
  await page.goto("/workspace");

  await page.getByRole("link", { name: "查看完整决策" }).click();
  await expect(page).toHaveURL(new RegExp(`/decisions/${agentPaperCycle.cycle_id}$`));
  await expect(page.getByRole("heading", { name: "买入 · AAPL" })).toBeVisible();
  const headings = await page.getByRole("heading", { level: 2 }).allTextContents();
  expect(headings).toEqual([
    "运行窗口", "市场与研究证据", "Agent 决策", "确定性仓位计算",
    "确定性风控", "订单结论", "执行结果", "后续表现",
  ]);
  await expect(page.getByRole("region", { name: "后续表现" }).getByText("未知", { exact: true })).toBeVisible();

  const rawFacts = page.getByText("查看原始事实", { exact: true }).first();
  await rawFacts.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByText(agentPaperCycle.cycle_id).last()).toBeVisible();
  await expect(page.getByRole("button", { name: /运行|停止|批准|下单|重试/ })).toHaveCount(0);
  expect(methods.length).toBeGreaterThanOrEqual(2);
  expect(methods.every((method) => method === "GET")).toBe(true);
  expect(browserErrors).toEqual([]);
  await assertReleaseAccessibility(page);
  await page.screenshot({ path: "test-results/decision-detail-desktop.png", fullPage: true });
});

test("decision records filter the loaded page and the detail ledger fits mobile", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  await installFixtures(page, []);
  await page.goto("/decisions");

  await expect(page.getByRole("list", { name: "决策记录" }).getByRole("listitem")).toHaveCount(1);
  await page.getByLabel("决策类型").selectOption("buy");
  await expect(page.getByText("当前筛选显示")).toContainText("1 / 1");
  await page.getByRole("link", { name: /查看决策/ }).click();
  await expect(page.getByRole("region", { name: "执行结果" }).getByText("部分成交", { exact: false }).first()).toBeVisible();
  await assertReleaseAccessibility(page);
  await page.screenshot({ path: "test-results/decision-detail-mobile.png", fullPage: true });
});

async function installFixtures(page: Page, methods: string[]): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify({
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: meta().as_of, capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }],
      meta: meta(),
    }),
  }));
  await page.route("**/api/v1/workbench", async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(workbenchPayload()) });
  });
  await page.route("**/api/v1/paper/cycles?limit=20", async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({
      data: { items: [agentPaperCycle], summary: { decision_session: "2026-08-26", status: "completed", cycle_count: 1, completed_count: 0, last_completed_decision_at: null, next_eligible_at: null, next_eligible_reason: null, latest_result_code: agentPaperCycle.result_code, execution_locked: true } },
      meta: meta(),
    }) });
  });
  await page.route(`**/api/v1/paper/cycles/${agentPaperCycle.cycle_id}`, async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: { cycle: agentPaperCycle, orders: [cancellableOrder] }, meta: meta() }) });
  });
}

function workbenchPayload(): WorkbenchPayload {
  return {
    data: {
      mode: "internal_paper", execution_locked: true,
      loop: { status: "online", started_at: "2026-08-26T19:20:00Z", last_seen_at: "2026-08-26T20:00:00Z", stale_after_seconds: 30 },
      session: { decision_session: "2026-08-26", status: "in_progress", cycle_count: 1, completed_count: 0, latest_result_code: agentPaperCycle.result_code, last_completed_decision_at: null, next_eligible_at: null, next_eligible_reason: null, execution_locked: true },
      latest_cycle: agentPaperCycle, active_orders: [cancellableOrder],
      account: { portfolio_id: paperPortfolio.portfolio_id, currency: "USD", initial_capital: paperPortfolio.initial_capital, data_state: "READY", as_of: paperPortfolio.performance!.as_of, nav: paperPortfolio.performance!.nav, available_cash: paperPortfolio.performance!.available_cash, reserved_cash: paperPortfolio.performance!.reserved_cash, market_value: paperPortfolio.performance!.market_value, realized_pnl: paperPortfolio.performance!.realized_pnl, unrealized_pnl: paperPortfolio.performance!.unrealized_pnl },
      attention: [],
    },
    meta: { ...meta(), schema_version: "1" },
  };
}
