import fs from "node:fs";
import path from "node:path";
import { expect, test } from "@playwright/test";

const directory = process.env.NEXUS_TEST_MONITOR_BROWSER_DIR;
test.skip(!directory, "requires the owned normal budget-rejection database");

test("normal budget rejection remains exact through BFF and the research page", async ({ page }) => {
  const ready = JSON.parse(fs.readFileSync(path.join(directory!, "ready.json"), "utf8"));
  const writes: string[] = [];
  const errors: string[] = [];
  page.on("request", request => {
    if (request.url().includes("/api/") && request.method() !== "GET") writes.push(request.method());
  });
  page.on("pageerror", error => errors.push(String(error)));
  await page.route("**/*", route => ["127.0.0.1", "localhost"].includes(new URL(route.request().url()).hostname)
    ? route.continue() : route.abort());
  const pending = page.waitForResponse(response => response.url().includes(`/api/v1/research-runs/${ready.run_id}`));
  await page.goto(`/research/${ready.instrument_id}?run_id=${ready.run_id}`);
  const response = await pending;
  expect(response.status()).toBe(200);
  const body = await response.json();
  expect(body.data.monitoring).toEqual(ready.response.data.monitoring);
  const monitoring = body.data.monitoring;
  expect(monitoring.execution_locked).toBe(true);
  expect(monitoring.trigger_count).toBe(1);
  expect(monitoring.triggers).toHaveLength(1);
  expect(monitoring.triggers[0]).toMatchObject({
    trigger_id: ready.trigger_id,
    status: "blocked",
    blocked_reason: "budget",
    run_id: null,
    usage_status: "not_started",
    notification_status: "queued",
  });
  const price = page.getByRole("region", { name: "价格监控记录" });
  await expect(price).toBeVisible();
  await price.locator("summary").click();
  await expect(price).toContainText("处理受阻");
  await expect(price).toContainText("预算不足或预算仍被占用。");
  await expect(price).toContainText("研究尚未开始");
  await expect(price).toContainText("通知等待处理");
  await expect(price.getByRole("link", { name: "查看本次触发的研究" })).toHaveCount(0);
  await expect(price).toContainText("设备送达状态未知");
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.screenshot({ path: path.join(directory!, "desktop.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: path.join(directory!, "mobile.png"), fullPage: true });
  await price.screenshot({ path: path.join(directory!, "price-mobile.png") });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  expect(writes).toEqual([]);
  expect(errors).toEqual([]);
  fs.writeFileSync(path.join(directory!, "browser-proof.json"), JSON.stringify({ passed: true, writes, errors, status: response.status() }));
});
