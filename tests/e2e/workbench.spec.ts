import { expect, test, type Page } from "@playwright/test";

import type { components } from "../../app/lib/generated/schema";
import { agentPaperCycle, meta, paperPortfolio } from "../unit/paper/fixtures";
import { assertReleaseAccessibility } from "./helpers/accessibility";

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
    attention: [{
      kind: "risk", severity: "warning", title_code: "AGENT_PAPER_RISK_REJECTED", reason_code: "RISK_REJECTED",
      safe_action: "view_decision", href: `/decisions/${agentPaperCycle.cycle_id}`, cycle_id: agentPaperCycle.cycle_id,
    }],
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
