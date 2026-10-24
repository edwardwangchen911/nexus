import fs from "node:fs";
import path from "node:path";
import { expect, test } from "@playwright/test";

const directory = process.env.NEXUS_TEST_MONITOR_BROWSER_DIR;
test.skip(!directory, "requires the owned, persisted monitoring API fixture");

test("normal BFF and page retain independent saved price and SEC monitoring", async ({ page }) => {
  const ready = JSON.parse(fs.readFileSync(path.join(directory!, "ready.json"), "utf8"));
  const writes: string[] = [];
  const errors: string[] = [];
  page.on("request", request => { if (request.url().includes("/api/") && request.method() !== "GET") writes.push(request.method()); });
  page.on("pageerror", error => errors.push(String(error)));
  await page.route("**/*", route => ["127.0.0.1", "localhost"].includes(new URL(route.request().url()).hostname)
    ? route.continue() : route.abort());
  const responsePromise = page.waitForResponse(response => response.url().includes(`/api/v1/research-runs/${ready.run_id}`));
  await page.goto(`/research/${ready.instrument_id}?run_id=${ready.run_id}`);
  await page.waitForLoadState("networkidle");
  const response = await responsePromise;
  expect(response.status()).toBe(200);
  const body = await response.json();
  expect(body.data.monitoring).toEqual(ready.response.data.monitoring);
  expect(body.data.disclosure_monitoring).toEqual(ready.response.data.disclosure_monitoring);
  await expect(page.getByRole("region", { name: "价格监控记录" })).toBeVisible();
  const sec = page.getByRole("region", { name: "SEC 披露监控记录" });
  await expect(sec).toBeVisible();
  await expect(sec).toContainText("交易保持锁定");
  await expect(sec).toContainText("共记录 2 项披露事件");
  await sec.locator("summary").click();
  await expect(sec.getByRole("heading", { name: "同一封存批次" })).toHaveCount(1);
  const expectedNote: Record<string, string> = {
    MONITOR_KIND_EVENTS_UNRESOLVED: "仍有事件尚未解决",
    MONITOR_DAILY_EVENT_REVIEW_UNVERIFIED: "事件处理提案未通过完整复核",
    MONITOR_DAILY_REPLACEMENT_REJECTED: "监控计划接替未完成",
    unknown_usage: "预算继续保留",
  };
  if (expectedNote[ready.reason]) await expect(sec).toContainText(expectedNote[ready.reason]);
  for (const trigger of body.data.disclosure_monitoring.triggers) {
    if (trigger.run_id) await expect(sec.locator(`a[href="/research/${ready.instrument_id}?run_id=${trigger.run_id}"]`).first()).toBeVisible();
  }
  expect(writes).toEqual([]);
  expect(errors).toEqual([]);
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.screenshot({ path: path.join(directory!, "desktop.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: path.join(directory!, "mobile.png"), fullPage: true });
  await sec.screenshot({ path: path.join(directory!, "sec-mobile.png") });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  if (ready.reason === "addressed") {
    await expect(sec).toContainText("事件已复核处理");
    const child = body.data.disclosure_monitoring.triggers[0].run_id;
    const childResponse = page.waitForResponse(response => response.url().includes(`/api/v1/research-runs/${child}`));
    await sec.getByRole("link", { name: "查看本次事件的研究" }).first().click();
    const exact = await childResponse;
    expect(exact.status()).toBe(200);
    const childBody = await exact.json();
    expect(childBody.data.run_id).toBe(child);
    expect(childBody.data.status).toBe("completed");
    await expect(page).toHaveURL(new RegExp(`run_id=${child}`));
    await expect(page.getByRole("region", { name: "个股研究证据页" })).toHaveAttribute("data-state", /^(ready|partial)$/);
  }
  if (!ready.reason) {
    // Explicit browser transport failures after a real normal BFF response.
    const endpoint = `**/api/v1/research-runs/${ready.run_id}`;
    await page.route(endpoint, route => route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ error: { code: "ENGINE_UNAVAILABLE", message: "unavailable" } }) }));
    await page.getByRole("button", { name: "重新读取" }).click();
    await expect(page.getByRole("region", { name: "个股研究证据页" })).toHaveAttribute("data-state", "stale");
    await expect(sec).toBeVisible();
    await page.unroute(endpoint);
    await page.route(endpoint, route => route.fulfill({ status: 403, contentType: "application/json", body: JSON.stringify({ error: { code: "FORBIDDEN", message: "forbidden" } }) }));
    await page.getByRole("button", { name: "重新读取" }).click();
    await expect(page.getByRole("region", { name: "个股研究证据页" })).toHaveAttribute("data-state", "forbidden");
    await expect(sec).toHaveCount(0);
    await expect(page.getByRole("region", { name: "价格监控记录" })).toHaveCount(0);
  }
  expect(writes).toEqual([]);
  fs.writeFileSync(path.join(directory!, "browser-proof.json"), JSON.stringify({ passed: true, writes, errors, status: response.status() }));
});
