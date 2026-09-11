import { expect, test, type Page } from "@playwright/test";

import { cancellableOrder, meta as paperMeta, paperPortfolio } from "../unit/paper/fixtures";
import { portfolio, portfolioId, responseMeta, snapshot } from "../unit/portfolio/fixtures";
import {
  decisionTime,
  instrumentId,
  researchAdmissionEnvelope,
  researchData,
  researchEnvelope,
} from "../unit/research/fixtures";

const writeAction = /创建方案|创建三方案|启动影子|下单|交易|买入|卖出|实盘/;

test("Agent abstention preserves evidence but cannot create a strategy target", async ({ page }) => {
  await installShellFixture(page);
  const abstained = researchData({
    status: "abstained",
    intent: null,
    quality_status: "partial",
    abstention_reason: "EVIDENCE_INSUFFICIENT",
    invalidation_conditions: [],
  });
  await page.route(`**/api/v1/instruments/${instrumentId}/research?**`, async (route) => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify(researchEnvelope(abstained)),
  }));
  await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, async (route) => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify(researchAdmissionEnvelope()),
  }));

  await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);

  await expect(page.getByText(/EVIDENCE_INSUFFICIENT/)).toBeVisible();
  await expect(page.getByText("尚未形成可供后续分析的研究结论；系统保持观望。")).toBeVisible();
  await expect(page.getByRole("button", { name: /运行冻结研究|重新运行冻结研究/ })).toHaveCount(0);
  await expect(page.getByRole("button", { name: writeAction })).toHaveCount(0);
});

test("source conflict exposes no selected price, chart, proposal, or order action", async ({ page }) => {
  await installShellFixture(page);
  await page.route("**/api/v1/market?**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: {
        items: [{
          instrument_id: instrumentId,
          symbol: "AAPL",
          price: null,
          source: null,
          feed: null,
          delivery_mode: "unknown",
          as_of: decisionTime,
          quality: "FAIL",
          quality_reasons: ["SOURCE_CONFLICT"],
          watchlisted: false,
        }],
        next_cursor: null,
        selected_series: null,
        intraday_monitor: null,
      },
      meta: fixtureMeta(),
    }),
  }));

  await page.goto("/market");

  await expect(page.getByRole("region", { name: "市场监控页面" })).toHaveAttribute("data-state", "partial");
  await expect(page.getByText("无法选定可靠价格")).toBeVisible();
  await expect(page.getByText("SOURCE_CONFLICT")).toBeVisible();
  await expect(page.getByRole("region", { name: "AAPL 价格图表" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: writeAction })).toHaveCount(0);
});

for (const portfolioFailure of [
  { status: "STALE", notice: "显示最后已知快照，不触发业务动作。" },
  { status: "RECONCILIATION_REQUIRED", notice: "双读对账未通过。快照仅供展示，不能用作优化基线。" },
] as const) {
  test(`${portfolioFailure.status} broker snapshot cannot become an optimization or order authority`, async ({ page }) => {
    await installShellFixture(page);
    await installPortfolioFixture(page, portfolioFailure.status);

    await page.goto("/portfolio");

    await expect(page.getByRole("region", { name: "组合事实页面" })).toHaveAttribute(
      "data-state",
      portfolioFailure.status === "STALE" ? "stale" : "partial",
    );
    await expect(page.getByText(portfolioFailure.notice, { exact: true })).toBeVisible();
    await expect(page.getByRole("link", { name: /优化|方案|下单|交易/ })).toHaveCount(0);
    await expect(page.getByRole("button", { name: writeAction })).toHaveCount(0);
    await expect(page.getByText("只读", { exact: true }).first()).toBeVisible();
  });
}

test("partial paper fill permits only versioned cancellation and never order creation", async ({ page }) => {
  const writes: string[] = [];
  page.on("request", (request) => {
    if (request.method() !== "GET") writes.push(`${request.method()} ${new URL(request.url()).pathname}`);
  });
  await installShellFixture(page);
  await page.route("**/api/v1/paper/portfolio", async (route) => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify({ data: paperPortfolio, meta: paperMeta() }),
  }));
  await page.route("**/api/v1/paper/orders?**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ data: { items: [cancellableOrder], next_cursor: null }, meta: paperMeta() }),
  }));

  await page.goto("/paper");

  await expect(page.getByRole("article", { name: "AAPL · 买入" })).toContainText("部分成交");
  await expect(page.getByRole("button", { name: "取消 AAPL 模拟订单" })).toBeVisible();
  await expect(page.getByRole("button", { name: /创建订单|下单|实盘买入|实盘卖出/ })).toHaveCount(0);
  expect(writes).toEqual([]);
});

test("an unacknowledged drawdown lock exposes no recovery, proposal, or order control", async ({ page }) => {
  await installShellFixture(page);
  await page.route("**/api/v1/readiness", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: { status: "ready", components: { database: "ready", migration: "ready", object_store: "ready", worker: "ready" } },
      meta: fixtureMeta(),
    }),
  }));
  await page.route("**/api/v1/risk-audit?**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: {
        summary: {
          data_quality: "PASS",
          drawdown_lock: { state: "locked", event_seq: 21, acknowledge_allowed: false },
          failed_jobs: { count: 0, retryable: [] },
        },
        items: [{
          event_seq: 21,
          occurred_at: "2026-08-27T00:00:00Z",
          kind: "risk",
          actor_kind: "system",
          action: "drawdown.locked",
          schema_version: "1",
          outcome: { status: "locked", reason: "DRAWDOWN_LIMIT" },
          previous_hash: "a".repeat(64),
          event_hash: "b".repeat(64),
        }],
        next_cursor: null,
      },
      meta: fixtureMeta(),
    }),
  }));

  await page.goto("/risk-audit?kind=risk&from=2026-08-20&to=2026-08-27&status=attention");

  await expect(page.getByRole("heading", { name: "已锁定" })).toBeVisible();
  await expect(page.getByText("事件版本 21")).toBeVisible();
  await expect(page.getByRole("button", { name: "确认解除回撤锁" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: writeAction })).toHaveCount(0);
});

async function installPortfolioFixture(page: Page, status: "STALE" | "RECONCILIATION_REQUIRED"): Promise<void> {
  await page.route("**/api/v1/portfolios?**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ data: { items: [{ ...portfolio, latest_snapshot_status: status }], next_cursor: null }, meta: responseMeta() }),
  }));
  await page.route(`**/api/v1/portfolios/${portfolioId}/snapshots/latest`, async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ data: { ...snapshot, status }, meta: responseMeta() }),
  }));
}

async function installShellFixture(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: decisionTime, capability_expires_at: null, contract_hash: null, next_action: "仅读研究模式。", version: 1 }],
      meta: fixtureMeta(),
    }),
  }));
}

function fixtureMeta() {
  return { request_id: "0198fa00-1111-7777-8111-111111111111", schema_version: "1", as_of: decisionTime };
}
