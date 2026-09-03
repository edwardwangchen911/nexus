import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page, type Route } from "@playwright/test";

import { assertReleaseAccessibility, releaseViewports } from "./helpers/accessibility";
import {
  actionEnvelope,
  detail,
  detailEnvelope,
  experimentId,
  overview,
  overviewEnvelope,
} from "../unit/strategy-evolution/fixtures";

for (const viewport of releaseViewports) {
  test(`${viewport.name} strategy evolution evidence remains bounded and accessible`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await installEvolutionFixtures(page);
    await page.goto("/strategies");

    await expect(page.getByRole("region", { name: "策略进化工作台" })).toHaveAttribute("data-state", "ready");
    await expect(page.getByText("策略进化 · 证据账簿")).toBeVisible();
    await expect(page.getByText("实盘关闭")).toBeVisible();
    await expect(page.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "活动实验" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "证据比较" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "策略前向观察" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "晋级解释" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "冠军历史" })).toBeVisible();
    await expect(page.getByText("21 / 63 个交易日")).toBeVisible();
    await expect(page.getByText("宽限期 5 个交易日")).toBeVisible();
    await expect(page.getByText("已等待 0 个交易日")).toBeVisible();
    await expect(page.getByRole("button", { name: /下单|交易|买入|卖出|券商/ })).toHaveCount(0);

    const comparison = page.getByRole("region", { name: "挑战者与冠军比较表" });
    await comparison.focus();
    await expect(comparison).toBeFocused();
    await assertReleaseAccessibility(page);

    if (viewport.name === "mobile-390" || viewport.name === "desktop-1440") {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: `test-results/strategy-evolution-${viewport.name}.png`, fullPage: true });
    }
  });
}

for (const [state, label] of [
  ["partial", "数据不完整"],
  ["stale", "显示上次数据"],
] as const) {
test(`${state} evidence stays visible but closes every mutation`, async ({ page }) => {
  const value = structuredClone(overview);
  value.status = state;
  value.write_enabled = false;
  value.write_block_reason = state === "partial" ? "EVIDENCE_INCOMPLETE" : "DATA_STALE";
  await installEvolutionFixtures(page, { overviewValue: value });
  await page.goto("/strategies");

  const workbench = page.getByRole("region", { name: "策略进化工作台" });
  await expect(workbench.getByText(label, { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
  for (const button of await page.locator(".evolution-workbench button").all()) {
    if (await button.getAttribute("aria-label") === "刷新策略进化证据") await expect(button).toBeEnabled();
    else await expect(button).toBeDisabled();
  }
});
}

test("pending promotion and rollback boundaries remain explicit", async ({ page }) => {
  const value = structuredClone(overview);
  value.champion.pending_transition = "PROMOTE";
  value.champion.pending_effective_session = "2026-08-28";
  value.registry_events.push({
    sequence: 2,
    event_type: "PROMOTE",
    target_strategy_version: "challenger_liquidity_v1",
    champion_generation: 2,
    occurred_at: "2026-08-27T16:10:00Z",
    effective_session: "2026-08-28",
    status: "pending",
    event_hash: "9".repeat(64),
  });
  value.champion.event_sequence = 2;
  await installEvolutionFixtures(page, { overviewValue: value });
  await page.goto("/strategies");

  await expect(page.getByText("PROMOTE · 2026-08-28")).toBeVisible();
  await expect(page.getByText("PROMOTE · challenger_liquidity_v1")).toBeVisible();

  value.champion.pending_transition = "ROLLBACK";
  value.champion.pending_effective_session = null;
  await page.getByRole("button", { name: "刷新策略进化证据" }).click();
  await expect(page.getByText("ROLLBACK · 等待边界")).toBeVisible();
});

test("pause, resume, veto and rollback submit only fixed commands", async ({ page }) => {
  const actions: Array<{ path: string; body: unknown; etag?: string; key?: string }> = [];
  const value = structuredClone(overview);
  await installEvolutionFixtures(page, {
    overviewValue: value,
    actionHandler: async (route) => {
      const request = route.request();
      const path = new URL(request.url()).pathname;
      const body = request.postDataJSON();
      actions.push({ path, body, etag: request.headers()["if-match"], key: request.headers()["idempotency-key"] });
      let action: Parameters<typeof actionEnvelope>[0] = "generate_family";
      if (path.endsWith("/control")) {
        action = (body as { action: "pause_auto_promotion" | "resume_auto_promotion" }).action;
        value.champion.auto_promotion_paused = action === "pause_auto_promotion";
      } else if (path.endsWith("/veto")) action = "veto_experiment";
      else if (path.endsWith("/rollback")) action = "rollback_registry";
      await route.fulfill({
        status: action === "rollback_registry" ? 202 : 200,
        contentType: "application/json",
        headers: action === "rollback_registry" ? { location: "/api/v1/strategy-evolution" } : {},
        body: JSON.stringify(actionEnvelope(action)),
      });
    },
  });
  await page.goto("/strategies");

  await page.getByRole("button", { name: "暂停自动晋级" }).click();
  await expect(page.getByText("已暂停自动晋级。")).toBeVisible();
  await page.getByRole("button", { name: "恢复自动晋级" }).click();
  await expect(page.getByText("已恢复自动晋级。")).toBeVisible();
  await page.getByRole("button", { name: "否决本次晋级" }).click();
  await expect(page.getByText("已记录人工否决。")).toBeVisible();

  const rollback = page.getByRole("button", { name: "回滚上一个冠军" });
  await rollback.click();
  const dialog = page.getByRole("dialog", { name: "确认回滚策略冠军" });
  const cancel = dialog.getByRole("button", { name: "取消" });
  const confirm = dialog.getByRole("button", { name: "确认回滚" });
  await expect(confirm).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(cancel).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(confirm).toBeFocused();
  await dialog.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(rollback).toBeFocused();
  await rollback.click();
  await page.getByRole("button", { name: "确认回滚" }).click();
  await expect(rollback).toBeFocused();
  await expect(page.getByText("回滚已接受，正在等待确定性边界生效。")).toBeVisible();

  expect(actions.map(({ body }) => body)).toEqual([
    { action: "pause_auto_promotion" },
    { action: "resume_auto_promotion" },
    { reason: "USER_VETO" },
    { reason: "USER_REQUESTED" },
  ]);
  expect(actions.every(({ etag }) => etag === '"g1-v1"')).toBe(true);
  expect(actions.every(({ key }) => /^strategy-evolution-\d+-\d+$/.test(key ?? ""))).toBe(true);
});

test("experiment start accepts no browser-selected parameter", async ({ page }) => {
  const value = structuredClone(overview);
  value.active_family = null;
  value.experiments = [];
  value.experiment_start_enabled = true;
  value.experiment_start_reason = "READY";
  let submitted: unknown;
  await installEvolutionFixtures(page, {
    overviewValue: value,
    actionHandler: async (route) => {
      submitted = route.request().postDataJSON();
      await route.fulfill({ status: 202, contentType: "application/json", headers: { location: "/api/v1/strategy-evolution" }, body: JSON.stringify(actionEnvelope()) });
    },
  });
  await page.goto("/strategies");
  await page.getByRole("button", { name: "启动实验" }).click();

  await expect(page.getByText("实验已入队，等待确定性工作流。")).toBeVisible();
  expect(submitted).toEqual({});
});

test("a 409 refetches authority and focuses the safe conflict summary", async ({ page }) => {
  let actionCalls = 0;
  let reads = 0;
  await installEvolutionFixtures(page, {
    onOverviewRead: () => { reads += 1; },
    actionHandler: async (route) => {
      actionCalls += 1;
      await route.fulfill({ status: 409, contentType: "application/json", body: JSON.stringify({ error: { code: "VERSION_CONFLICT" } }) });
    },
  });
  await page.goto("/strategies");
  await expect.poll(() => reads).toBe(1);
  await page.getByRole("button", { name: "暂停自动晋级" }).click();

  await expect.poll(() => reads).toBe(2);
  const alert = page.getByRole("alert");
  await expect(alert).toContainText("资源已变化");
  await expect(alert).toBeFocused();
  expect(actionCalls).toBe(1);
});

test("an unknown network result replays the exact command with the same idempotency key", async ({ page }) => {
  const requests: Array<{ body: unknown; etag?: string; key?: string }> = [];
  const value = structuredClone(overview);
  await installEvolutionFixtures(page, {
    overviewValue: value,
    actionHandler: async (route) => {
      const request = route.request();
      requests.push({
        body: request.postDataJSON(),
        etag: request.headers()["if-match"],
        key: request.headers()["idempotency-key"],
      });
      if (requests.length === 1) {
        await route.abort("connectionreset");
        return;
      }
      value.champion.auto_promotion_paused = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(actionEnvelope("pause_auto_promotion")),
      });
    },
  });
  await page.goto("/strategies");

  await page.getByRole("button", { name: "暂停自动晋级" }).click();
  await expect(page.getByRole("alert")).toContainText("操作结果未知");
  const retry = page.getByRole("button", { name: "继续确认原操作结果" });
  await expect(retry).toBeVisible();
  await expect(page.getByRole("button", { name: "否决本次晋级" })).toBeDisabled();

  await retry.click();
  await expect(page.getByText("已暂停自动晋级。")).toBeVisible();
  expect(requests).toHaveLength(2);
  expect(requests[1]).toEqual(requests[0]);
});

test("forbidden authority clears all protected strategy evidence", async ({ page }) => {
  await installEvolutionFixtures(page, { overviewStatus: 403 });
  await page.goto("/strategies");

  await expect(page.getByRole("status", { name: "无权访问" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "baseline_v1" })).toHaveCount(0);
  await expect(page.getByText("实盘关闭")).toBeVisible();
});

type FixtureOptions = {
  overviewValue?: typeof overview;
  detailValue?: typeof detail;
  overviewStatus?: number;
  onOverviewRead?: () => void;
  actionHandler?: (route: Route) => Promise<void>;
};

async function installEvolutionFixtures(page: Page, options: FixtureOptions = {}): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: "2026-08-27T16:00:00Z", capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }], meta: overviewEnvelope().meta }),
  }));
  await page.route("**/api/v1/strategy-evolution**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (request.method() === "POST") {
      if (options.actionHandler) await options.actionHandler(route);
      else await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(actionEnvelope()) });
      return;
    }
    if (url.pathname === "/api/v1/strategy-evolution") {
      options.onOverviewRead?.();
      const status = options.overviewStatus ?? 200;
      await route.fulfill({
        status,
        contentType: "application/json",
        headers: { etag: '"g1-v1"' },
        body: status === 200
          ? JSON.stringify({ ...overviewEnvelope(), data: structuredClone(options.overviewValue ?? overview) })
          : JSON.stringify({ error: { code: "FORBIDDEN" } }),
      });
      return;
    }
    if (url.pathname === `/api/v1/strategy-evolution/experiments/${experimentId}`) {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        headers: { etag: '"g1-v1"' },
        body: JSON.stringify({ ...detailEnvelope(), data: structuredClone(options.detailValue ?? detail) }),
      });
      return;
    }
    await route.fulfill({ status: 404, contentType: "application/json", body: "{}" });
  });
}

test("ready page has no serious or critical Axe violations after actions", async ({ page }) => {
  await installEvolutionFixtures(page);
  await page.goto("/strategies");
  const serious = (await new AxeBuilder({ page }).analyze()).violations.filter((item) => item.impact === "serious" || item.impact === "critical");
  expect(serious).toEqual([]);
});
