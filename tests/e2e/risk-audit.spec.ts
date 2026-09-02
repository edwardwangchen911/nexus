import AxeBuilder from "@axe-core/playwright";
import { gzipSync } from "node:zlib";
import { expect, test, type Page, type Route } from "@playwright/test";

const requestId = "0198f7a0-1111-7777-8111-111111111111";
const jobId = "0198f7a0-1111-7777-8111-111111111112";
const meta = { request_id: requestId, schema_version: "1", as_of: "2026-08-27T00:00:00Z" };

for (const viewport of [
  { name: "phone-375", width: 375, height: 812 },
  { name: "phone-390", width: 390, height: 844 },
  { name: "tablet-768", width: 768, height: 1024 },
  { name: "desktop-1024", width: 1024, height: 768 },
  { name: "desktop-1440", width: 1440, height: 1000 },
  { name: "phone-landscape", width: 844, height: 390 },
]) {
  test(`${viewport.name} risk-audit console contains facts and controls`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await installRiskAuditFixtures(page);
    await page.goto("/risk-audit?kind=job&from=2026-08-20&to=2026-08-27&status=attention");

    await expect(page.getByRole("heading", { name: "风险与审计", exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "依赖就绪" })).toBeVisible();
    await expect(page.getByRole("region", { name: "风险与审计控制台" })).toHaveAttribute("data-state", "ready");
    await expect(page.getByRole("table", { name: "脱敏审计事件" })).toContainText("job.failed");
    await expect(page.getByRole("button", { name: "重试 daily_pipeline" })).toBeEnabled();
    await expect(page.getByText(jobId, { exact: true })).toHaveCount(0);
    await expect(page.getByRole("button", { name: /重跑全部|创建订单|实盘|券商下单/ })).toHaveCount(0);

    const tableRegion = page.getByRole("region", { name: "脱敏审计事件表，可横向滚动" });
    await tableRegion.focus();
    await expect(tableRegion).toBeFocused();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);

    const controls = await page.locator(".risk-audit-page button:visible, .risk-audit-page select:visible, .risk-audit-page input:visible").evaluateAll((items) => items.map((item) => {
      const rectangle = item.getBoundingClientRect();
      return { height: rectangle.height, width: rectangle.width };
    }));
    expect(controls.length).toBeGreaterThan(0);
    for (const control of controls) {
      expect(control.height).toBeGreaterThanOrEqual(44);
      expect(control.width).toBeGreaterThanOrEqual(44);
    }

    if (viewport.name === "phone-390" || viewport.name === "desktop-1440") {
      const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
        (violation) => violation.impact === "serious" || violation.impact === "critical",
      );
      expect(serious).toEqual([]);
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: `test-results/risk-audit-${viewport.name}.png`, fullPage: true });
    }
  });
}

test("risk-audit filters keep status local and bind server query to kind/date", async ({ page }) => {
  const queries: string[] = [];
  await installRiskAuditFixtures(page, {
    onAuditRead: (url) => queries.push(url.search),
  });
  await page.goto("/risk-audit?kind=job&from=2026-08-20&to=2026-08-27&status=attention");
  await expect(page.getByText("job.failed")).toBeVisible();

  const readsBeforeStatus = queries.length;
  await page.getByRole("combobox", { name: "结果状态" }).selectOption("recovered");
  await expect(page.getByText("job.failed")).toHaveCount(0);
  await expect.poll(() => queries.length).toBe(readsBeforeStatus);
  await expect(page).toHaveURL(/status=recovered/);

  await page.getByRole("combobox", { name: "事件类型" }).selectOption("risk");
  await expect.poll(() => queries.at(-1) ?? "").toContain("kind=risk");
  expect(queries.at(-1)).toContain("from=2026-08-20T00%3A00%3A00Z");
  expect(queries.at(-1)).toContain("to=2026-08-28T00%3A00%3A00Z");
});

test("manual-safe retry preserves focus, version and idempotency", async ({ page }) => {
  let requestBody: unknown;
  let requestVersion: string | undefined;
  let requestKey: string | undefined;
  await installRiskAuditFixtures(page, {
    retryHandler: async (route) => {
      requestBody = route.request().postDataJSON();
      requestVersion = route.request().headers()["if-match"];
      requestKey = route.request().headers()["idempotency-key"];
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({
        data: { job_id: jobId, status: "queued", version: 8, manual_retry_count: 1, replayed: false }, meta,
      }) });
    },
  });
  await page.goto("/risk-audit?kind=job&from=2026-08-20&to=2026-08-27&status=attention");

  const retry = page.getByRole("button", { name: "重试 daily_pipeline" });
  await retry.click();
  const dialog = page.getByRole("dialog", { name: "确认重试 daily_pipeline" });
  await expect(dialog.getByRole("button", { name: "返回审计" })).toBeFocused();
  await dialog.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(retry).toBeFocused();

  await retry.click();
  await page.getByRole("button", { name: "确认安全重试" }).click();
  await expect(page.getByText("Job 已重新排队；已刷新权威事实。")).toBeVisible();
  expect(requestBody).toEqual({});
  expect(requestVersion).toBe('"7"');
  expect(requestKey).toMatch(/^risk-retry-[0-9a-f-]{36}$/);
});

test("unknown retry result can only continue with the same idempotency key", async ({ page }) => {
  const keys: Array<string | undefined> = [];
  let attempts = 0;
  await installRiskAuditFixtures(page, {
    retryHandler: async (route) => {
      attempts += 1;
      keys.push(route.request().headers()["idempotency-key"]);
      if (attempts === 1) { await route.abort("connectionreset"); return; }
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({
        data: { job_id: jobId, status: "queued", version: 8, manual_retry_count: 1, replayed: true }, meta,
      }) });
    },
  });
  await page.goto("/risk-audit?kind=job&from=2026-08-20&to=2026-08-27&status=attention");
  await page.getByRole("button", { name: "重试 daily_pipeline" }).click();
  await page.getByRole("button", { name: "确认安全重试" }).click();
  await expect(page.getByText("恢复结果未知。保持相同幂等键并关闭其他恢复动作。")).toBeVisible();
  await page.getByRole("button", { name: "继续确认原操作结果" }).click();
  await expect(page.getByText("Job 已重新排队；已刷新权威事实。")).toBeVisible();
  expect(attempts).toBe(2);
  expect(new Set(keys).size).toBe(1);
});

test("drawdown acknowledgement and redacted export stay narrowly scoped", async ({ page }) => {
  let acknowledgement: unknown;
  let version: string | undefined;
  await installRiskAuditFixtures(page, {
    acknowledgeHandler: async (route) => {
      acknowledgement = route.request().postDataJSON();
      version = route.request().headers()["if-match"];
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({
        data: { state: "unlocked", event_seq: 13, replayed: false }, meta,
      }) });
    },
  });
  await page.goto("/risk-audit?kind=job&from=2026-08-20&to=2026-08-27&status=attention");

  await page.getByRole("button", { name: "确认解除回撤锁" }).click();
  await expect(page.getByRole("dialog", { name: "确认解除回撤锁" })).toContainText("不会创建或提交订单");
  await page.getByRole("button", { name: "确认解除", exact: true }).click();
  await expect(page.getByText("回撤锁已确认解除；已刷新权威事实。")).toBeVisible();
  expect(acknowledgement).toEqual({ expected_event_seq: 12 });
  expect(version).toBe('"12"');

  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: "导出 31 天内 NDJSON.gz" }).click();
  const download = await downloadPromise;
  expect(download.suggestedFilename()).toBe("nexus-risk-audit.ndjson.gz");
});

test("unready dependencies retain facts but close recovery", async ({ page }) => {
  await installRiskAuditFixtures(page, { readinessStatus: 503 });
  await page.goto("/risk-audit?kind=job&from=2026-08-20&to=2026-08-27&status=attention");
  await expect(page.getByRole("region", { name: "风险与审计控制台" })).toHaveAttribute("data-state", "stale");
  await expect(page.getByText("依赖未就绪；保留最后读取的脱敏事实，恢复动作保持关闭。")).toBeVisible();
  await expect(page.getByRole("button", { name: "重试 daily_pipeline" })).toBeDisabled();
  await expect(page.getByRole("button", { name: "确认解除回撤锁" })).toBeDisabled();
});

test("authoritative audit failure exposes no prior facts", async ({ page }) => {
  await installRiskAuditFixtures(page, { auditStatus: 502 });
  await page.goto("/risk-audit?kind=job&from=2026-08-20&to=2026-08-27&status=attention");
  await expect(page.getByRole("region", { name: "风险与审计控制台" })).toHaveAttribute("data-state", "error");
  await expect(page.getByText("daily_pipeline")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "重试 daily_pipeline" })).toHaveCount(0);
});

type RiskAuditFixtureOptions = {
  readinessStatus?: number;
  auditStatus?: number;
  onAuditRead?: (url: URL) => void;
  retryHandler?: (route: Route) => Promise<void>;
  acknowledgeHandler?: (route: Route) => Promise<void>;
};

async function installRiskAuditFixtures(page: Page, options: RiskAuditFixtureOptions = {}): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ data: [{
      provider: "fosun", mode: "fixture", quality: "PASS", as_of: "2026-08-27T00:00:00Z",
      capability_expires_at: null, contract_hash: null, next_action: "仅读研究模式。", version: 1,
    }], meta }),
  }));
  await page.route("**/api/v1/readiness", async (route) => {
    const status = options.readinessStatus ?? 200;
    await route.fulfill({ status, contentType: "application/json", body: JSON.stringify({
      data: readiness(status === 200 ? "ready" : "unready"), meta,
    }) });
  });
  await page.route("**/api/v1/risk-audit?**", async (route) => {
    options.onAuditRead?.(new URL(route.request().url()));
    const status = options.auditStatus ?? 200;
    await route.fulfill({ status, contentType: "application/json", body: status === 200
      ? JSON.stringify({ data: auditPage(), meta })
      : JSON.stringify({ error: { code: "PROVIDER_UNAVAILABLE", message: "fixture unavailable", request_id: requestId, details: {} } }) });
  });
  await page.route("**/api/v1/risk-audit/export?**", async (route) => route.fulfill({
    status: 200,
    headers: {
      "content-type": "application/x-ndjson",
      "content-encoding": "gzip",
      "content-disposition": 'attachment; filename="nexus-risk-audit.ndjson.gz"',
    },
    body: gzipSync(`${JSON.stringify(auditPage().items[0])}\n`),
  }));
  if (options.retryHandler) await page.route("**/api/v1/jobs/*/retry", options.retryHandler);
  if (options.acknowledgeHandler) await page.route("**/api/v1/risk/drawdown-lock/acknowledge", options.acknowledgeHandler);
}

function readiness(status: "ready" | "unready") {
  return {
    status,
    components: {
      database: status === "ready" ? "ready" : "unavailable",
      migration: "ready", object_store: "ready", worker: "ready",
    },
  };
}

function auditPage() {
  return {
    summary: {
      data_quality: "PASS",
      drawdown_lock: { state: "recovery_eligible", event_seq: 12, acknowledge_allowed: true },
      failed_jobs: { count: 1, retryable: [{
        job_id: jobId, job_type: "daily_pipeline", status: "failed", version: 7,
        manual_retry_count: 0, last_error_code: "PROVIDER_UNAVAILABLE", finished_at: "2026-08-26T23:30:00Z",
      }] },
    },
    items: [{
      event_seq: 9, occurred_at: "2026-08-26T23:30:00Z", kind: "job", actor_kind: "system",
      action: "job.failed", schema_version: "1", outcome: { status: "failed", retryable: true, error_code: "PROVIDER_UNAVAILABLE" },
      previous_hash: "a".repeat(64), event_hash: "b".repeat(64),
    }],
    next_cursor: null,
  };
}
