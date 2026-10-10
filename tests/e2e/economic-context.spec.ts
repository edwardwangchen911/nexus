import { readFileSync, writeFileSync } from "node:fs";
import { expect, test } from "@playwright/test";

import { formatDisplayDecimal } from "../../app/lib/display-decimal";

const fixtureDirectory = process.env.NEXUS_E2E_ECONOMIC_LANE_FIXTURE_DIR ?? ".secrets/economic-pair-lifecycle/lane-fixtures";
const nativeCase = process.env.NEXUS_E2E_ECONOMIC_LANE_LIVE_CASE;
if (nativeCase && !["filled", "stage"].includes(nativeCase)) throw new Error("Run with the live isolated economic API fixture harness.");
for (const lane of ["incumbent", "challenger"] as const) {
  test(`live ${nativeCase}/${lane} reads all four pages and exact decision without business writes`, async ({ page }) => {
    test.setTimeout(90_000); // Four cold dev pages, detail, refresh, and router navigation; request deadline remains 10s.
    test.skip(!nativeCase, "Requires the normal isolated live API fixture harness.");
    const envelope = JSON.parse(readFileSync(`${fixtureDirectory}/${nativeCase}-${lane}.json`, "utf8"));
    const fact = envelope.data;
    const query = `?pair_slot_id=${fact.pair.pair_slot_id}&lane=${lane}`;
    const apiCalls: Array<{ path: string; method: string }> = [];
    const errors: string[] = [];
    const consoleErrors: string[] = [];
    page.on("console", (message) => { if (message.type() === "error") consoleErrors.push(message.text().slice(0, 500)); });
    page.on("request", (request) => { const url = new URL(request.url()); if (url.pathname.startsWith("/api/v1/")) apiCalls.push({ path: url.pathname, method: request.method() }); });
    page.on("pageerror", (error) => errors.push(error.message));
    await page.setViewportSize({ width: lane === "incumbent" ? 1440 : 390, height: 1000 });
    for (const view of ["workspace", "decisions", "paper", "portfolio"]) {
      const laneReply = page.waitForRequest((request) => new URL(request.url()).pathname === `/api/v1/paper/economic-pairs/${fact.pair.pair_slot_id}/lanes/${lane}`, { timeout: 12_000 });
      await page.goto(`/${view}${query}`);
      const laneResponse = await (await laneReply).response();
      expect(laneResponse?.status()).toBe(200);
      try {
        await expect(page.getByRole("region", { name: "经济配对只读页面" })).toHaveAttribute("data-state", "ready");
      } catch (error) {
        writeFileSync(`.secrets/economic-pair-lifecycle/live-${nativeCase}-${lane}-failure.json`, JSON.stringify({ view, errors, consoleErrors, apiCalls }), { mode: 0o600 });
        throw error;
      }
      await page.waitForLoadState("networkidle");
      const context = page.getByRole("region", { name: "配对状态" });
      await expect(context).toContainText(`${fact.pair.ordinal} / 24`);
      await expect(context).toContainText(fact.pair.pair_slot_id);
      for (const link of await page.getByRole("navigation", { name: "配对事实页面" }).getByRole("link").all()) {
        expect(await link.getAttribute("href")).toContain(`pair_slot_id=${fact.pair.pair_slot_id}`);
      }
      if (view === "workspace" || view === "portfolio") {
        await expect(page.getByRole("region", { name: "本侧账本与持仓" })).toContainText("USD");
        if (fact.actual.positions.length) {
          await expect(page.getByRole("table", { name: "本侧当前持仓" })).toContainText(formatDisplayDecimal(fact.actual.positions[0].quantity));
        } else {
          await expect(page.getByText("已核验无持仓", { exact: true })).toBeVisible();
          await expect(page.getByRole("table", { name: "本侧当前持仓" })).toHaveCount(0);
        }
      }
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
      await page.screenshot({ path: `.secrets/economic-pair-lifecycle/live-${nativeCase}-${lane}-${view}.png`, fullPage: true });
    }
    const detailReply = page.waitForRequest((request) => new URL(request.url()).pathname === `/api/v1/paper/economic-pairs/${fact.pair.pair_slot_id}/lanes/${lane}`, { timeout: 12_000 });
    await page.goto(`/decisions/${fact.detail.cycle.cycle_id}${query}`);
    expect((await (await detailReply).response())?.status()).toBe(200);
    await expect(page.getByRole("region", { name: "经济配对只读页面" })).toHaveAttribute("data-state", "ready");
    await expect(page.getByRole("heading", { name: "决策详情", exact: true })).toBeVisible();
    await expect(page.getByRole("region", { name: "执行结果" })).toBeVisible();
    await page.screenshot({ path: `.secrets/economic-pair-lifecycle/live-${nativeCase}-${lane}-detail.png`, fullPage: true });
    expect(apiCalls.every((call) => call.method === "GET")).toBe(true);
    expect(apiCalls.filter((call) => !call.path.includes("/economic-pairs/")).map((call) => call.path)).toEqual(expect.arrayContaining(["/api/v1/integrations"]));
    expect(apiCalls.some((call) => ["/api/v1/workbench", "/api/v1/paper/actual", "/api/v1/portfolios", "/api/v1/paper/cycles/latest"].includes(call.path))).toBe(false);
    expect(errors).toEqual([]);
    const readsBeforeRefresh = apiCalls.length;
    const refreshReply = page.waitForRequest((request) => new URL(request.url()).pathname === `/api/v1/paper/economic-pairs/${fact.pair.pair_slot_id}/lanes/${lane}`, { timeout: 12_000 });
    await page.getByRole("button", { name: "刷新配对事实" }).click();
    await expect.poll(() => apiCalls.length).toBeGreaterThan(readsBeforeRefresh);
    expect((await (await refreshReply).response())?.status()).toBe(200);
    await expect(page.getByRole("region", { name: "经济配对只读页面" })).toHaveAttribute("data-state", "ready");
    const paperReply = page.waitForRequest((request) => new URL(request.url()).pathname === `/api/v1/paper/economic-pairs/${fact.pair.pair_slot_id}/lanes/${lane}`, { timeout: 12_000 });
    await page.goto(`/paper${query}`);
    expect((await (await paperReply).response())?.status()).toBe(200);
    await expect(page.getByRole("region", { name: "经济配对只读页面" })).toHaveAttribute("data-state", "ready");
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.getByRole("navigation", { name: "主导航", exact: true }).getByRole("link", { name: "复盘", exact: true }).click();
    await expect(page).toHaveURL(/\/paper#paper-comparison-title$/);
    await expect(page.getByRole("region", { name: "经济配对只读页面" })).toHaveCount(0);
    expect(errors).toEqual([]);
  });
}
