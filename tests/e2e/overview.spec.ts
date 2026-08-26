import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page, type Route } from "@playwright/test";

import { eligibleProposalGroup, groupId, overviewData, shadowActive } from "../unit/optimize/fixtures";
import {
  decisionTime,
  instrumentId,
  researchAdmissionEnvelope,
  researchEnvelope,
} from "../unit/research/fixtures";

const meta = { request_id: "0198fa00-1111-7777-8111-111111111111", schema_version: "1", as_of: decisionTime };

test("no data progresses through frozen research, diagnosis, three proposals, and shadow observation", async ({ page }) => {
  let researchReady = false;
  let shadowStarted = false;
  let shadowRequest: { body: unknown; idempotencyKey?: string; version?: string } | null = null;
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  await installJourneyFixtures(page, {
    get researchReady() { return researchReady; },
    completeResearch: () => { researchReady = true; },
    get shadowStarted() { return shadowStarted; },
    startShadow: (route) => {
      shadowRequest = {
        body: route.request().postDataJSON(),
        idempotencyKey: route.request().headers()["idempotency-key"],
        version: route.request().headers()["if-match"],
      };
      shadowStarted = true;
    },
  });

  await page.goto("/overview");
  await expect(page.getByRole("heading", { name: "尚无可用诊断" })).toBeVisible();
  await page.getByRole("link", { name: "研究美股证据" }).click();

  await expect(page).toHaveURL(/\/market/);
  await page.getByRole("link", { name: "打开 AAPL 个股研究" }).click();
  await expect(page.getByRole("heading", { name: "没有该时点的研究记录" })).toBeVisible();
  await page.getByRole("button", { name: "运行冻结研究" }).click();
  await expect(page.getByRole("heading", { name: "委员会决策记录" })).toBeVisible();
  await expect(page.getByText("冻结研究已登记；页面只展示可追溯的研究状态，不触发交易。")).toBeVisible();

  await page.getByRole("link", { name: "概览", exact: true }).click();
  await expect(page.getByRole("heading", { name: "值得比较优化方案" })).toBeVisible();
  await page.getByRole("link", { name: "打开组合诊断" }).click();
  await expect(page.getByRole("heading", { name: "组合诊断账簿" })).toBeVisible();
  await page.getByRole("link", { name: "打开已有三方案" }).click();

  const comparison = page.getByRole("table", { name: "保持现状、最小调整与策略目标的确定性比较" });
  await expect(comparison.getByRole("columnheader", { name: /保持现状|最小调整|策略目标/ })).toHaveCount(3);
  await expect(page.getByRole("heading", { name: "尚未启动影子验证" })).toBeVisible();
  await page.getByRole("button", { name: "启动影子验证" }).click();
  await expect(page.getByRole("dialog", { name: "确认启动影子验证" })).toContainText("不会创建券商订单");
  await page.getByRole("button", { name: "确认启动影子验证" }).click();

  await expect(page.getByText("影子验证已启动；不会创建券商订单。")).toBeVisible();
  await expect(page.getByRole("heading", { name: "前向影子绩效" })).toBeVisible();
  expect(shadowRequest).toEqual({
    body: { version: 4 },
    idempotencyKey: expect.stringMatching(/^proposal-shadow-[0-9a-f-]{36}$/),
    version: '"4"',
  });
  await expect(page.getByRole("button", { name: /下单|交易|买入|卖出|实盘|券商订单/ })).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
    (violation) => violation.impact === "serious" || violation.impact === "critical",
  );
  expect(serious).toEqual([]);
});

type JourneyFixture = {
  readonly researchReady: boolean;
  completeResearch: () => void;
  readonly shadowStarted: boolean;
  startShadow: (route: Route) => void;
};

async function installJourneyFixtures(page: Page, state: JourneyFixture): Promise<void> {
  await installShellFixture(page);
  await page.route("**/api/v1/overview**", async (route) => {
    if (!state.researchReady) {
      await route.fulfill({ status: 404, contentType: "application/json", body: JSON.stringify({ error: { code: "NOT_FOUND" } }) });
      return;
    }
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ data: overviewData(), meta }) });
  });
  await page.route("**/api/v1/market?**", async (route) => {
    const selected = new URL(route.request().url()).searchParams.get("selected_instrument_id");
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        data: {
          items: [{
            instrument_id: instrumentId,
            symbol: "AAPL",
            price: "227.160000000000",
            source: "alpaca",
            feed: "iex",
            delivery_mode: "delayed",
            as_of: decisionTime,
            quality: "PASS",
            quality_reasons: [],
            watchlisted: false,
          }],
          next_cursor: null,
          selected_series: selected === instrumentId ? {
            instrument_id: instrumentId,
            source: "alpaca",
            feed: "iex",
            delivery_mode: "delayed",
            adjustment: "raw",
            interval: "1Day",
            quality: "PASS",
            quality_reasons: [],
            bars: [{
              session: "2026-08-24",
              open: "225.000000000000",
              high: "228.000000000000",
              low: "224.000000000000",
              close: "227.160000000000",
              volume: "50000000.000000000000",
              available_at: decisionTime,
              observed_at: "2026-08-24T19:59:00Z",
              quality_flags: [],
            }],
          } : null,
        },
        meta,
      }),
    });
  });
  await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, async (route) => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify(researchAdmissionEnvelope()),
  }));
  await page.route(`**/api/v1/instruments/${instrumentId}/research?**`, async (route) => route.fulfill({
    status: state.researchReady ? 200 : 404,
    contentType: "application/json",
    body: JSON.stringify(state.researchReady ? researchEnvelope() : { error: { code: "NOT_FOUND" } }),
  }));
  await page.route("**/api/v1/research-runs", async (route) => {
    expect(route.request().postDataJSON()).toEqual({
      instrument_id: instrumentId,
      data_snapshot_id: researchAdmissionEnvelope().data.data_snapshot_id,
      decision_time: decisionTime,
    });
    expect(route.request().headers()["idempotency-key"]).toMatch(/^research-[0-9a-f-]{36}$/);
    state.completeResearch();
    await route.fulfill({ status: 202, contentType: "application/json", body: JSON.stringify(researchEnvelope()) });
  });
  await page.route(`**/api/v1/optimization-proposals/${groupId}/shadow`, async (route) => {
    if (route.request().method() === "POST") {
      state.startShadow(route);
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ data: { proposal_group_id: groupId, status: "shadowing", version: 5 }, meta }),
      });
      return;
    }
    await route.fulfill({
      status: state.shadowStarted ? 200 : 404,
      contentType: "application/json",
      body: JSON.stringify(state.shadowStarted ? { data: shadowActive(), meta } : { error: { code: "NOT_FOUND" } }),
    });
  });
  await page.route(`**/api/v1/optimization-proposals/${groupId}`, async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ data: eligibleProposalGroup(), meta }),
  }));
}

async function installShellFixture(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: decisionTime, capability_expires_at: null, contract_hash: null, next_action: "仅读研究模式。", version: 1 }],
      meta,
    }),
  }));
}
