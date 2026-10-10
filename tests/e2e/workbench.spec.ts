import { expect, test, type Page } from "@playwright/test";

import type { components } from "../../app/lib/generated/schema";
import { agentPaperCycle, meta, paperPortfolio } from "../unit/paper/fixtures";
import { assertReleaseAccessibility } from "./helpers/accessibility";
import { managementSummary } from "../unit/paper/management-fixtures";

test("workbench portfolio management is only a summary", async ({ page }) => {
  await installFixtures(page, readyWorkbench(), []);
  await page.route("**/api/v1/paper/management/portfolio", (route) => route.fulfill({ json: { data: managementSummary, meta: meta() } }));
  await page.goto("/workspace");
  const summary = page.getByRole("region", { name: "内部模拟目标与计划摘要" });
  await expect(summary).toContainText("目标 V7");
  await expect(summary.getByRole("table")).toHaveCount(0);
  await expect(summary.getByRole("link", { name: "查看调整计划" })).toBeVisible();
});

type WorkbenchPayload = components["schemas"]["WorkbenchEnvelope"];

const readyWorkbench = (): WorkbenchPayload => ({
  data: {
    mode: "internal_paper",
    execution_locked: true,
    loop: { status: "online", reason_code: null, started_at: "2026-08-26T19:20:00Z", last_seen_at: "2026-08-26T20:00:00Z", stale_after_seconds: 30 },
    session: {
      decision_session: "2026-08-26", status: "completed", cycle_count: 1, completed_count: 1,
      latest_result_code: "NO_ORDER_REQUIRED", last_completed_decision_at: agentPaperCycle.decision_time,
      next_eligible_at: "2026-08-26T20:05:00Z", next_eligible_reason: "NEXT_SLOT", execution_locked: true,
    },
    latest_cycle: {
      ...agentPaperCycle, status: "completed", result_code: "NO_ORDER_REQUIRED", order_ids: [],
      candidates: [{ ...agentPaperCycle.candidates[0], stance: "hold", sizing_action: "maintain" }],
    },
    active_orders: [],
    account: {
      portfolio_id: paperPortfolio.portfolio_id, currency: "USD", initial_capital: paperPortfolio.initial_capital,
      data_state: "READY", as_of: paperPortfolio.performance!.as_of, nav: paperPortfolio.performance!.nav,
      available_cash: paperPortfolio.performance!.available_cash, reserved_cash: paperPortfolio.performance!.reserved_cash,
      market_value: paperPortfolio.performance!.market_value, realized_pnl: paperPortfolio.performance!.realized_pnl,
      unrealized_pnl: paperPortfolio.performance!.unrealized_pnl,
    },
    readiness: { stage: "completed", blocker: "none", reason_code: "NO_ORDER_REQUIRED", previous_cycle_id: null, changes: [] },
    compatibility: { status: "compatible", release_status: "matching", expected_process_version: "agent-paper-loop-v1", expected_release_revision: "a".repeat(40), expected_migration_head: "0064_prompt_release_channel", reported_process_version: "agent-paper-loop-v1+a" + "a".repeat(39), reported_release_revision: "a".repeat(40), reported_migration_head: "0064_prompt_release_channel", database_migration_heads: ["0064_prompt_release_channel"] },
    attention: [],
  },
  meta: { ...meta(), schema_version: "1" as const },
});

for (const viewport of [{ name: "desktop", width: 1440, height: 1000 }, { name: "mobile", width: 390, height: 844 }]) {
  test(`${viewport.name} workbench keeps the decision first and stays read-only`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.emulateMedia({ reducedMotion: "reduce" });
    const methods: string[] = [];
    await installFixtures(page, readyWorkbench(), methods);
    await page.goto("/workspace");

    await expect(page.getByRole("region", { name: "交易工作台页面" })).toHaveAttribute("data-state", "ready", { timeout: 15_000 });
    await expect(page.getByRole("heading", { name: "交易工作台" })).toBeVisible();
    await expect(page.getByText("内部模拟", { exact: true })).toBeVisible();
    await expect(page.getByText("实盘锁定")).toBeVisible();
    const readiness = page.getByRole("region", { name: "运行就绪与阻断" });
    await expect(readiness.getByText("本周期已完成", { exact: true })).toBeVisible();
    await expect(readiness).toContainText("下一五分钟槽");
    await readiness.getByText("查看版本与迁移证据", { exact: true }).click();
    await expect(readiness.getByText("代码预期迁移", { exact: true })).toBeVisible();
    await readiness.getByText("查看版本与迁移证据", { exact: true }).click();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    const decision = page.getByRole("region", { name: "最新决策" });
    const attention = page.getByRole("region", { name: "需要处理" });
    await expect(decision.getByText("AAPL")).toBeVisible();
    await expect(decision.getByRole("link", { name: "查看完整决策" })).toHaveAttribute("href", `/decisions/${agentPaperCycle.cycle_id}`);
    await expect(page.getByRole("button", { name: /运行|停止|批准/ })).toHaveCount(0);

    const decisionBox = await decision.boundingBox();
    const attentionBox = await attention.boundingBox();
    if (viewport.name === "desktop") {
      expect(decisionBox!.x).toBeLessThan(attentionBox!.x);
      expect(decisionBox!.width).toBeGreaterThan(attentionBox!.width);
    } else {
      expect(attentionBox!.y).toBeGreaterThan(decisionBox!.y + decisionBox!.height - 1);
    }

    await page.getByRole("button", { name: "刷新工作台" }).click();
    await expect(page.getByRole("status", { name: "工作台状态" })).toContainText("工作台已更新");
    expect(methods.length).toBeGreaterThanOrEqual(2);
    expect(methods.every((method) => method === "GET")).toBe(true);
    await assertReleaseAccessibility(page);
    await page.screenshot({ path: `test-results/workbench-${viewport.name}.png`, fullPage: true });
  });
}

test("workbench presents offline and unknown results as distinct safe states", async ({ page }) => {
  const payload = readyWorkbench();
  payload.data.loop.status = "offline";
  payload.data.loop.reason_code = "HEARTBEAT_STALE";
  payload.data.latest_cycle = { ...agentPaperCycle, status: "partially_filled", order_ids: [] };
  payload.data.active_orders = [];
  payload.data.readiness = { stage: "recovery", blocker: "result_unknown", reason_code: "ORDER_RESULT_UNKNOWN", previous_cycle_id: null, changes: [] };
  payload.data.attention = [{
    kind: "execution", severity: "critical", title_code: "PAPER_ORDER_RECONCILING", reason_code: "ORDER_RESULT_UNKNOWN",
    safe_action: "verify_original_result", href: "/paper", cycle_id: agentPaperCycle.cycle_id,
  }];
  await installFixtures(page, payload, []);
  await page.goto("/workspace");

  await expect(page.getByText("循环离线")).toBeVisible();
  await expect(page.getByRole("region", { name: "最新决策" }).getByText("结果未知")).toBeVisible();
  await expect(page.getByRole("link", { name: "核对原操作结果" })).toHaveAttribute("href", "/paper");
  await expect(page.getByRole("button", { name: /重试|批准|提交/ })).toHaveCount(0);
});

async function installFixtures(page: Page, payload: WorkbenchPayload, methods: string[]): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify({
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: meta().as_of, capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }],
      meta: meta(),
    }),
  }));
  await page.route("**/api/v1/workbench", async (route) => {
    methods.push(route.request().method());
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(payload) });
  });
}

test("manual start survives refresh as a readonly original-request lookup", async ({ page }) => {
  await installFixtures(page, readyWorkbench(), []);
  const calls: { method: string; key: string | undefined }[] = [];
  const receipt = { request_id: "0198f7a0-1111-7777-8111-111111111111", mode: "research_only", requested_at: "2026-10-09T14:00:00Z", status: "queued", code: "MANUAL_REQUEST_QUEUED", cycle_id: null, execution_locked: true };
  await page.route("**/api/v1/paper/manual-requests", async (route) => {
    const request = route.request();
    calls.push({ method: request.method(), key: request.headers()["idempotency-key"] });
    if (request.method() === "POST") expect(request.postDataJSON()).toEqual({ mode: "research_only" });
    await route.fulfill({ json: { data: receipt, meta: meta() } });
  });
  await page.goto("/workspace");
  await expect(page.getByRole("button", { name: "仅研究", exact: true })).toBeEnabled();
  expect(calls).toHaveLength(0);
  await page.getByRole("button", { name: "仅研究", exact: true }).click();
  await expect(page.getByText("已登记，等待宿主处理", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "研究并内部模拟", exact: true })).toBeDisabled();
  await page.reload();
  await expect(page.getByText("已登记，等待宿主处理", { exact: true })).toBeVisible();
  expect(calls.filter((call) => call.method === "POST")).toHaveLength(1);
  expect(calls.length).toBeGreaterThan(1);
  expect(calls.slice(1).every((call) => call.method === "GET" && call.key === calls[0].key)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
