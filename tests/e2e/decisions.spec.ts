import { expect, test, type Page } from "@playwright/test";

import type { components } from "../../app/lib/generated/schema";
import { agentPaperCycle, cancellableOrder, meta, paperPortfolio } from "../unit/paper/fixtures";
import { assertReleaseAccessibility } from "./helpers/accessibility";
import { managementDetail, managementPlan } from "../unit/paper/management-fixtures";
import { synthesisUsageDetail } from "../unit/paper/synthesis-usage-fixture";

for (const [status, label] of [
  ["accepted", "历史未知已接受，预算保留"],
  ["not_accepted", "历史未知尚未接受，预算保留"],
  ["invalid", "历史接受证明失效，预算保留"],
] as const) {
  test(`historical synthesis usage ${status} stays failed and refresh is read-only`, async ({ page }) => {
    const methods: string[] = [];
    await installFixtures(page, methods);
    const detail = structuredClone(synthesisUsageDetail);
    detail.synthesis_usage_disposition!.status = status;
    await page.route(`**/api/v1/paper/management/cycles/${agentPaperCycle.cycle_id}`, (route) => {
      methods.push(route.request().method());
      return route.fulfill({ json: { data: detail, meta: meta() } });
    });
    await page.goto(`/decisions/${agentPaperCycle.cycle_id}`);
    const notice = page.getByRole("note", { name: "组合用量历史处置" });
    await expect(notice).toContainText("组合失败／用量未知／未执行");
    await expect(notice).toContainText(label);
    await expect(notice).toContainText("后续周期仍须独立通过准入校验");
    await expect(page.getByRole("note", { name: "目标生效结论" })).toContainText("阻断原因待核对");
    await page.getByRole("button", { name: "刷新记录" }).click();
    await expect(notice).toContainText(label);
    expect(methods.filter((method) => method === "GET").length).toBeGreaterThanOrEqual(2);
    expect(methods.every((method) => method === "GET")).toBe(true);
    await expect(page.getByRole("button", { name: /批准|运行|重试/ })).toHaveCount(0);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  });
}

for (const viewport of [{ width: 1280, height: 910 }, { width: 390, height: 844 }]) {
  test(`portfolio proposal authority fits ${viewport.width} and disclosures work by keyboard`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
    await installFixtures(page, []);
    await page.route(`**/api/v1/paper/management/cycles/${agentPaperCycle.cycle_id}`, (route) => route.fulfill({ json: { data: managementDetail, meta: meta() } }));
    await page.goto(`/decisions/${agentPaperCycle.cycle_id}`);
    const conclusion = page.getByRole("note", { name: "目标生效结论" });
    await expect(conclusion).toContainText("目标 V7 继续生效");
    await expect(conclusion).toContainText("提案 …0008");
    await expect(page.getByRole("table", { name: "确定性目标对照" })).toContainText("标的 …0009");
    const table = page.getByRole("table", { name: "确定性目标对照" });
    expect((await conclusion.boundingBox())!.y).toBeLessThan((await table.boundingBox())!.y);
    await expect(table).toContainText("不更新研究有效期");
    const disclosure = page.getByText("查看政策版本与证据依据", { exact: true });
    await disclosure.focus(); await page.keyboard.press("Enter");
    await expect(page.getByText("提案有效期", { exact: true })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    await assertReleaseAccessibility(page);
    await disclosure.click();
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: `test-results/task12-decisions-${viewport.width}.png` });
    const focused = await page.locator(".portfolio-decision").boundingBox();
    expect(focused).not.toBeNull();
    await page.screenshot({ path: `test-results/task12-decisions-focused-${viewport.width}.png`, fullPage: true, clip: focused! });
  });
}

type WorkbenchPayload = components["schemas"]["WorkbenchEnvelope"];

test("opens the latest decision from the workbench as an ordered read-only ledger", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  const methods: string[] = [];
  const browserErrors: string[] = [];
  const failedResponses: string[] = [];
  page.on("console", (message) => { if (message.type() === "error") browserErrors.push(message.text()); });
  page.on("response", (response) => {
    if (response.status() >= 400) failedResponses.push(`${response.status()} ${new URL(response.url()).pathname}`);
  });
  await installFixtures(page, methods);
  const currentCashResponse = page.waitForResponse((response) => new URL(response.url()).pathname === "/api/v1/paper/actual");
  await page.goto("/workspace");
  expect((await currentCashResponse).status()).toBe(200);
  const currentCash = page.getByRole("region", { name: "当前账本现金（内部模拟）" });
  await expect(currentCash.getByText("USD 3,500", { exact: true })).toBeVisible();
  await expect(currentCash.getByText("USD 500", { exact: true })).toBeVisible();

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
  expect(methods.length).toBeGreaterThanOrEqual(3);
  expect(methods.every((method) => method === "GET")).toBe(true);
  expect(failedResponses).toEqual([]);
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
  await page.route("**/api/v1/paper/actual", async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ json: { data: managementPlan.actual, meta: meta() } });
  });
  await page.route("**/api/v1/paper/management/portfolio", (route) => route.fulfill({ json: { data: { current_target: null, latest_proposal: null, occupying_plan: null, read_as_of: meta().as_of }, meta: meta() } }));
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
  await page.route("**/api/v1/paper/cycles/latest", async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: agentPaperCycle, meta: meta() }) });
  });
  await page.route(`**/api/v1/paper/cycles?decision_session=${agentPaperCycle.decision_session}&limit=20`, async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({
      data: { items: [agentPaperCycle], summary: { decision_session: "2026-08-26", status: "completed", cycle_count: 1, completed_count: 0, last_completed_decision_at: null, next_eligible_at: null, next_eligible_reason: null, latest_result_code: agentPaperCycle.result_code, execution_locked: true }, version_observation: null, version_history: [], version_evaluation: null },
      meta: meta(),
    }) });
  });
  await page.route(`**/api/v1/paper/management/cycles/${agentPaperCycle.cycle_id}`, async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: { cycle: agentPaperCycle, orders: [cancellableOrder], active_target: managementDetail.active_target, plan_summary: { ...managementDetail.blocking_plan, target_version_id: null } }, meta: meta() }) });
  });
}

function workbenchPayload(): WorkbenchPayload {
  return {
    data: {
      mode: "internal_paper", execution_locked: true,
      loop: { status: "online", reason_code: null, started_at: "2026-08-26T19:20:00Z", last_seen_at: "2026-08-26T20:00:00Z", stale_after_seconds: 30 },
      session: { decision_session: "2026-08-26", status: "in_progress", cycle_count: 1, completed_count: 0, latest_result_code: agentPaperCycle.result_code, last_completed_decision_at: null, next_eligible_at: null, next_eligible_reason: null, execution_locked: true },
      latest_cycle: agentPaperCycle, active_orders: [cancellableOrder],
      account: { portfolio_id: paperPortfolio.portfolio_id, currency: "USD", initial_capital: paperPortfolio.initial_capital, data_state: "READY", as_of: paperPortfolio.performance!.as_of, nav: paperPortfolio.performance!.nav, available_cash: paperPortfolio.performance!.available_cash, reserved_cash: paperPortfolio.performance!.reserved_cash, market_value: paperPortfolio.performance!.market_value, realized_pnl: paperPortfolio.performance!.realized_pnl, unrealized_pnl: paperPortfolio.performance!.unrealized_pnl },
      attention: [],
    },
    meta: { ...meta(), schema_version: "1" },
  };
}
