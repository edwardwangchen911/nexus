import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

import {
  briefEnvelope,
  dataSnapshotId,
  decisionTime,
  instrumentId,
  researchAdmissionEnvelope,
  researchEnvelope,
  sessionDate,
} from "../unit/research/fixtures";

for (const viewport of [
  { name: "mobile-375", width: 375, height: 812 },
  { name: "mobile-390", width: 390, height: 844 },
  { name: "tablet-768", width: 768, height: 1024 },
  { name: "desktop-1024", width: 1024, height: 768 },
  { name: "desktop-1440", width: 1440, height: 1000 },
  { name: "mobile-landscape", width: 844, height: 390 },
]) {
  test(`${viewport.name} brief to evidence journey is traceable and non-transactional`, async ({ page }) => {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await installFixtures(page);
    await page.goto(`/briefs/${sessionDate}`);

    await expect(page.getByRole("heading", { name: "每日 Agent 简报" })).toBeVisible({ timeout: 15_000 });
    await expect(page.getByRole("heading", { name: `${sessionDate} 证据完整度` })).toBeVisible({ timeout: 15_000 });
    const briefDecisionPath = page.getByRole("region", { name: "投资决策证据链，可横向滚动" });
    await expect(briefDecisionPath.getByRole("listitem").filter({ hasText: "研究证据" })).toHaveAttribute("aria-current", "step");
    if (viewport.name === "mobile-390") {
      const pathSize = await briefDecisionPath.evaluate((element) => ({
        clientWidth: element.clientWidth,
        scrollWidth: element.scrollWidth,
      }));
      expect(pathSize.scrollWidth).toBeGreaterThan(pathSize.clientWidth);
      await briefDecisionPath.focus();
      const before = await briefDecisionPath.evaluate((element) => element.scrollLeft);
      await briefDecisionPath.press("ArrowRight");
      await expect.poll(() => briefDecisionPath.evaluate((element) => element.scrollLeft)).toBeGreaterThan(before);
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    }
    if (viewport.name === "mobile-390" || viewport.name === "desktop-1440") {
      await page.screenshot({ path: `test-results/brief-${viewport.name}.png`, fullPage: true });
    }
    await page.getByRole("link", { name: instrumentId }).first().click();

    await expect(page).toHaveURL(new RegExp(`/research/${instrumentId}\\?as_of=`));
    await expect(page.getByRole("heading", { name: "委员会决策记录" })).toBeVisible();
    await expect(page.getByRole("region", { name: "投资决策证据链，可横向滚动" }).getByRole("listitem").filter({ hasText: "研究证据" })).toHaveAttribute("aria-current", "step");
    await expect(page.getByRole("heading", { name: "支持论点" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "反方论点" })).toBeVisible();
    await expect(page.getByText(/空头角色认为增长放缓/)).toBeVisible();

    const opener = page.getByRole("button", { name: "查看证据定位" }).first();
    await opener.focus();
    await expect(opener).toBeFocused();
    await opener.press("Enter");
    const dialog = page.getByRole("dialog", { name: "证据原文与定位" });
    await expect(dialog).toContainText("SEC");
    await expect(dialog).toContainText("0000320193-26-000001");
    await expect(dialog).toContainText("memory://sha256/");
    await expect(dialog).toContainText("2026-08-24 18:30:00 UTC");
    await expect(dialog).toContainText("a".repeat(64));
    await expect(page.getByRole("button", { name: "关闭证据抽屉" })).toBeFocused();
    await dialog.press("Escape");
    await expect(opener).toBeFocused();

    await expect(page.getByRole("button", { name: /下单|交易|买入|卖出/ })).toHaveCount(0);
    await expect(page.getByText(/\d+\s*\/\s*100|置信度/)).toHaveCount(0);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(serious).toEqual([]);
    if (viewport.name === "mobile-390" || viewport.name === "desktop-1440") {
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: `test-results/research-${viewport.name}.png`, fullPage: true });
    }
  });
}

test("empty research uses only the server admission body for the first run", async ({ page }) => {
  let postedBody: unknown = null;
  await installShellFixture(page);
  await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify(researchAdmissionEnvelope()),
  }));
  await page.route(`**/api/v1/instruments/${instrumentId}/research?**`, async (route) => route.fulfill({
    status: 404,
    contentType: "application/json",
    body: JSON.stringify({ error: { code: "NOT_FOUND" } }),
  }));
  await page.route("**/api/v1/research-runs", async (route) => {
    postedBody = route.request().postDataJSON();
    expect(route.request().headers()["idempotency-key"]).toBeTruthy();
    await route.fulfill({ status: 202, contentType: "application/json", body: JSON.stringify(researchEnvelope()) });
  });

  await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
  await page.getByRole("button", { name: "运行冻结研究" }).click();

  await expect(page.getByText(/冻结研究已登记/)).toBeVisible();
  expect(postedBody).toEqual({
    instrument_id: instrumentId,
    data_snapshot_id: dataSnapshotId,
    decision_time: decisionTime,
  });
  await expect(page.getByRole("button", { name: /下单|交易|买入|卖出/ })).toHaveCount(0);
});

async function installFixtures(page: Page): Promise<void> {
  await installShellFixture(page);
  await page.route(`**/api/v1/briefs/${sessionDate}`, async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(briefEnvelope()) }));
  await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(researchAdmissionEnvelope()) }));
  await page.route(`**/api/v1/instruments/${instrumentId}/research?**`, async (route) => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(researchEnvelope()) }));
}

async function installShellFixture(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: "2026-08-25T01:00:00Z", capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }],
      meta: { request_id: "0198d600-1000-7000-8000-000000001598", schema_version: "1", as_of: "2026-08-25T01:00:00Z" },
    }),
  }));
}
