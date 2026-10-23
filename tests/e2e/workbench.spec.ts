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
    await expect(readiness).toContainText("下一定时启动区间");
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

for (const viewport of [{ name: "desktop", width: 1440, height: 1000 }, { name: "mobile", width: 390, height: 844 }]) {
  test(`${viewport.name} acknowledged unknown resets locally and a new start uses a new key`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await installFixtures(page, readyWorkbench(), []);
    await page.addInitScript(() => sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "old-unknown", mode: "research_only" })));
    const calls: { method: string; key: string | undefined }[] = [];
    const unknown = { request_id: "0198f7a0-1111-7777-8111-111111111111", mode: "research_only", requested_at: "2026-10-09T14:00:00Z", status: "unknown", code: "MANUAL_RESULT_UNKNOWN", cycle_id: null, execution_locked: true, preparation_disposition: "unknown_acknowledged" };
    await page.route("**/api/v1/paper/manual-requests", async (route) => {
      const request = route.request();
      calls.push({ method: request.method(), key: request.headers()["idempotency-key"] });
      if (request.method() === "GET") await route.fulfill({ json: { data: unknown, meta: meta() } });
      else {
        expect(request.postDataJSON()).toEqual({ mode: "research_only" });
        expect(request.headers()["idempotency-key"]).not.toBe("old-unknown");
        await route.fulfill({ json: { data: { ...unknown, request_id: "0198f7a0-2222-7777-8111-111111111111", status: "queued", code: "MANUAL_REQUEST_QUEUED", preparation_disposition: null }, meta: meta() } });
      }
    });
    await page.goto("/workspace");
    const panel = page.getByRole("region", { name: "开始研究与内部模拟" });
    await expect(panel.getByText("周期结果尚未确认", { exact: true })).toBeVisible();
    await expect(panel).toContainText("历史调用、费用和预算保持原样");
    await page.reload();
    await expect(panel.getByRole("button", { name: "开始新的请求" })).toBeVisible();
    expect(calls.every((call) => call.method === "GET" && call.key === "old-unknown")).toBe(true);
    await panel.screenshot({ path: `test-results/acknowledged-unknown-${viewport.name}.png` });
    await panel.getByRole("button", { name: "开始新的请求" }).click();
    expect(await page.evaluate(() => sessionStorage.getItem("nexus-manual-paper-attempt-v1"))).toBeNull();
    await expect(panel.getByRole("button", { name: "仅研究", exact: true })).toBeEnabled();
    expect(calls.every((call) => call.method === "GET")).toBe(true);
    await panel.getByRole("button", { name: "仅研究", exact: true }).click();
    await expect(panel.getByText("已登记，等待宿主处理", { exact: true })).toBeVisible();
    expect(calls.filter((call) => call.method === "POST")).toHaveLength(1);
  });
}

test("unknown reset requires a confirmed disposition and loses it after failed readback", async ({ page }) => {
  await installFixtures(page, readyWorkbench(), []);
  await page.addInitScript(() => sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "old-unknown", mode: "research_only" })));
  let stage = "unaccepted";
  const methods: string[] = [];
  await page.route("**/api/v1/paper/manual-requests", async (route) => {
    methods.push(route.request().method());
    if (stage === "failed") return route.fulfill({ status: 409, json: { error: { details: { reason_code: "MANUAL_PREPARATION_ACK_BLOCKED" } } } });
    await route.fulfill({ json: { data: { request_id: "0198f7a0-1111-7777-8111-111111111111", mode: "research_only", requested_at: "2026-10-09T14:00:00Z", status: "unknown", code: "MANUAL_RESULT_UNKNOWN", cycle_id: null, execution_locked: true, ...(stage === "accepted" ? { preparation_disposition: "unknown_acknowledged" } : {}) }, meta: meta() } });
  });
  await page.goto("/workspace");
  const panel = page.getByRole("region", { name: "开始研究与内部模拟" });
  await expect(panel.getByText("周期结果尚未确认", { exact: true })).toBeVisible();
  await expect(panel.getByRole("button", { name: "开始新的请求" })).toHaveCount(0);
  stage = "accepted";
  await panel.getByRole("button", { name: "核对原请求" }).click();
  await expect(panel.getByRole("button", { name: "开始新的请求" })).toBeVisible();
  stage = "failed";
  await panel.getByRole("button", { name: "核对原请求" }).click();
  await expect(panel.getByRole("status")).toContainText("提交结果尚未确认");
  await expect(panel.getByRole("button", { name: "开始新的请求" })).toHaveCount(0);
  await expect(panel.getByRole("button", { name: "仅研究", exact: true })).toBeDisabled();
  expect(methods.every((method) => method === "GET")).toBe(true);
});

for (const kind of ["cancelled_before_call", "unknown_preserved", "cycle_reconciled"] as const) {
  test(`current preparation recovery ${kind} stays read-only and revokes stale proof`, async ({ page }) => {
    await installFixtures(page, readyWorkbench(), []);
    await page.addInitScript(() => sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "interrupted", mode: "research_only" })));
    let failed = false;
    const methods: string[] = [];
    const cycle = "0198f7a0-2222-7777-8111-111111111111";
    const cancelled = kind === "cancelled_before_call";
    await page.route("**/api/v1/paper/manual-requests", async (route) => {
      methods.push(route.request().method());
      if (failed) return route.fulfill({ status: 409, json: { error: { details: { reason_code: "MANUAL_PREPARATION_RECOVERY_BLOCKED" } } } });
      await route.fulfill({ json: { data: {
        request_id: "0198f7a0-1111-7777-8111-111111111111", mode: "research_only", requested_at: "2026-10-09T14:00:00Z",
        status: cancelled ? "unavailable" : "unknown", code: cancelled ? "MANUAL_PREPARATION_CANCELLED" : "MANUAL_RESULT_UNKNOWN", cycle_id: null, execution_locked: true,
        preparation_recovery: { disposition: kind, external_effects: cancelled ? "not_started" : kind === "cycle_reconciled" ? "cycle_recorded" : "unknown", cycle_id: kind === "cycle_reconciled" ? cycle : null },
      }, meta: meta() } });
    });
    await page.goto("/workspace");
    const panel = page.getByRole("region", { name: "开始研究与内部模拟" });
    await expect(panel).toContainText(cancelled ? "未开始外部操作" : kind === "unknown_preserved" ? "费用和预算继续保留" : "已找回原周期");
    await expect(panel.getByRole("button", { name: "开始新的请求" })).toHaveCount(cancelled ? 1 : 0);
    if (kind === "cycle_reconciled") await expect(panel.getByRole("link", { name: "查看原周期决策" })).toHaveAttribute("href", `/decisions/${cycle}`);
    await panel.screenshot({ path: `test-results/preparation-recovery-${kind}.png` });
    failed = true;
    await panel.getByRole("button", { name: "核对原请求" }).click();
    await expect(panel.getByRole("status")).toContainText("提交结果尚未确认");
    await expect(panel.getByRole("button", { name: "开始新的请求" })).toHaveCount(0);
    await expect(panel.getByRole("link", { name: "查看原周期决策" })).toHaveCount(0);
    expect(methods.every((method) => method === "GET")).toBe(true);
  });
}

for (const width of [390, 1440]) {
  test(`pure three-stock research readback at width ${width} stays read-only`, async ({ page }) => {
    const { researchReceipt } = await import("../unit/manual-paper/research-fixture");
    await page.setViewportSize({ width, height: 1000 });
    await installFixtures(page, readyWorkbench(), []);
    await page.addInitScript(() => sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "pure-new", mode: "research_only" })));
    const methods: string[] = [];
    await page.route("**/api/v1/paper/manual-requests", async (route) => {
      methods.push(route.request().method());
      await route.fulfill({ json: { data: researchReceipt, meta: meta() } });
    });
    await page.goto("/workspace");
    const panel = page.getByRole("region", { name: "开始研究与内部模拟" });
    await expect(panel.getByText("研究处理已完成", { exact: true })).toBeVisible();
    await expect(panel.getByText("内容受限", { exact: true })).toHaveCount(2);
    await expect(panel).toContainText("远端最新状态未核实");
    await expect(panel).toContainText("正式决策仍须重新核验当时资料与风控");
    const links = panel.getByRole("link", { name: /查看第.*只证券研究/ });
    await expect(links).toHaveCount(3);
    for (const [index, member] of researchReceipt.research.members.entries()) {
      await expect(links.nth(index)).toHaveAttribute("href", `/research/${member.instrument_id}?run_id=${member.run_id}`);
    }
    await page.reload();
    await expect(panel.getByText("研究处理已完成", { exact: true })).toBeVisible();
    expect(methods.length).toBeGreaterThan(1);
    expect(methods.every((method) => method === "GET")).toBe(true);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    await panel.screenshot({ path: `test-results/pure-research-${width}.png` });
  });
}

for (const viewport of [{ name: "desktop", width: 1440, height: 1000 }, { name: "mobile", width: 390, height: 844 }]) {
  test(`${viewport.name} frozen activity keeps sources, modes and original round through refresh failure`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.emulateMedia({ reducedMotion: "reduce" });
    const payload = readyWorkbench();
    const id = "00000000-0000-4000-8000-000000000001";
    const instrument = "00000000-0000-4000-8000-000000000002";
    const run = "00000000-0000-4000-8000-000000000003";
    Object.assign(payload.data, { activity: {
      read_at: "2026-10-23T08:00:00Z", read_status: "complete", scopes: [{
        mode: "research_and_paper", today_registered: 1, today_started: 1, registration_date: "2026-10-23", decision_session: "2026-10-23",
        selection: "today", request_id: id, cycle_id: agentPaperCycle.cycle_id, requested_at: "2026-10-23T07:00:00Z", status: "completed",
        reason_code: "RESEARCH_OUTPUT_NOT_ACTIONABLE", trigger: "manual", historical: false, occupancy: "not_indicated",
        portfolio_status: "deferred", portfolio_reason_code: "RESEARCH_OUTPUT_NOT_ACTIONABLE", href: `/decisions/${agentPaperCycle.cycle_id}`,
        candidates: [{ instrument_id: instrument, symbol: "AAPL", run_id: run, snapshot_id: id, research_as_of: "2026-10-23T07:30:00Z",
          run_status: "completed", job_status: "succeeded", quality: "partial", read_status: "verified", reason_code: "RESEARCH_OUTPUT_NOT_ACTIONABLE",
          href: `/research/${instrument}?run_id=${run}`, has_more: false, limitations: [
            { source: "system", kind: "reason", text: "研究未通过交易准入", impact: null, truncated: false },
            { source: "model", kind: "missing_evidence", text: "历史分母适用范围仍需核对", impact: "估值判断受限", truncated: true },
            { source: "model", kind: "uncertainties", text: "未来需求仍存在不确定性", impact: null, truncated: false },
          ] },
        { instrument_id: id, symbol: "NVDA", run_id: null, snapshot_id: null, research_as_of: null, run_status: null, job_status: "queued", quality: null,
          read_status: "unavailable", reason_code: null, href: null, limitations: [], has_more: false }],
      }, { mode: "research_only", today_registered: 0, today_started: 0, registration_date: "2026-10-23", decision_session: null,
        selection: "none", request_id: null, cycle_id: null, requested_at: null, status: null, reason_code: null, trigger: null,
        historical: false, occupancy: "not_indicated", portfolio_status: "not_applicable", portfolio_reason_code: null, href: null, candidates: [] }],
    } });
    const methods: string[] = [];
    await installFixtures(page, payload, methods);
    await page.goto("/workspace");
    await page.waitForLoadState("networkidle");
    const summary = page.getByRole("region", { name: "本轮运行与研究限制" });
    await expect(summary).toContainText("组合判断暂缓");
    await expect(summary.getByRole("region", { name: "纯研究" })).toContainText("今日未登记");
    await expect(summary.getByRole("article", { name: "AAPL 本轮研究" })).toContainText("研究已完成 · 结论受限");
    await expect(summary.getByRole("article", { name: "NVDA 本轮研究" })).toContainText("等待执行");
    const expand = summary.getByText("展开更多已保存限制", { exact: true });
    await expand.focus();
    await page.keyboard.press("Enter");
    await expect(summary.getByText("未来需求仍存在不确定性", { exact: true })).toBeVisible();
    await expect(summary).toContainText("模型所述限制不等于系统已独立证实");
    await expect(summary.getByRole("link", { name: "查看 AAPL 原研究" })).toHaveAttribute("href", `/research/${instrument}?run_id=${run}`);
    await summary.getByText("查看本轮对象信息", { exact: true }).click();
    await expect(summary.locator("details").filter({ has: page.getByText("查看本轮对象信息", { exact: true }) }).getByText(id, { exact: true })).toBeVisible();
    await assertReleaseAccessibility(page);
    await summary.screenshot({ path: `test-results/workbench-activity-${viewport.name}.png` });
    const text = await summary.textContent();
    await page.route("**/api/v1/workbench", (route) => { methods.push(route.request().method()); return route.fulfill({ status: 503, json: { error: { code: "UNAVAILABLE" } } }); });
    await page.getByRole("button", { name: "刷新工作台" }).click();
    await expect(page.getByRole("region", { name: "交易工作台页面" })).toHaveAttribute("data-state", "stale");
    expect(await summary.textContent()).toBe(text);
    expect(methods.length).toBeGreaterThanOrEqual(2);
    expect(methods.every((method) => method === "GET")).toBe(true);
    await page.route("**/api/v1/workbench", (route) => route.fulfill({ status: 403, json: { error: { code: "FORBIDDEN" } } }));
    await page.getByRole("button", { name: "刷新工作台" }).click();
    await expect(summary).toHaveCount(0);
  });
}
