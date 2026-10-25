import fs from "node:fs";
import path from "node:path";
import { expect, test } from "@playwright/test";

const directory = process.env.NEXUS_TEST_MONITOR_BROWSER_DIR;
test.skip(!directory, "requires the owned normal daily research API fixture");

test("normal daily adoption reaches BFF and page without manufacturing events", async ({ page }) => {
  const ready = JSON.parse(fs.readFileSync(path.join(directory!, "ready.json"), "utf8"));
  const writes: string[] = [];
  const errors: string[] = [];
  page.on("request", request => {
    if (request.url().includes("/api/") && request.method() !== "GET") writes.push(request.method());
  });
  page.on("pageerror", error => errors.push(String(error)));
  await page.route("**/*", route => ["127.0.0.1", "localhost"].includes(new URL(route.request().url()).hostname)
    ? route.continue() : route.abort());
  const exact = page.waitForResponse(response => response.url().includes(`/api/v1/research-runs/${ready.run_id}`));
  await page.goto(`/research/${ready.instrument_id}?run_id=${ready.run_id}`);
  await page.waitForLoadState("networkidle");
  const response = await exact;
  expect(response.status()).toBe(200);
  const body = await response.json();
  expect(body.data.run_id).toBe(ready.run_id);
  expect(body.data.monitoring).toEqual(ready.response.data.monitoring);
  expect(body.data.disclosure_monitoring).toEqual(ready.response.data.disclosure_monitoring);
  for (const field of ["monitoring", "disclosure_monitoring"] as const) {
    expect(body.data[field].state).toBe("valid");
    expect(body.data[field].plan_id).toBeTruthy();
    expect(body.data[field].trigger_count).toBe(0);
    expect(body.data[field].execution_locked).toBe(true);
  }
  const price = page.getByRole("region", { name: "价格监控记录" });
  const sec = page.getByRole("region", { name: "SEC 披露监控记录" });
  await expect(price).toContainText("计划有效期内");
  await expect(sec).toContainText("计划有效期内");
  await expect(sec).toContainText("交易保持锁定");
  await expect(sec).toContainText("共记录 0 项披露事件");
  await expect(sec).toContainText("尚无已保存的披露检查");
  await expect(sec.getByRole("heading", { name: "披露复核条件" })).toBeVisible();
  await expect(sec.getByRole("heading", { name: "同一封存批次" })).toHaveCount(0);
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.screenshot({ path: path.join(directory!, "desktop.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: path.join(directory!, "mobile.png"), fullPage: true });
  await sec.screenshot({ path: path.join(directory!, "sec-mobile.png") });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  expect(writes).toEqual([]);
  expect(errors).toEqual([]);
  fs.writeFileSync(path.join(directory!, "browser-proof.json"), JSON.stringify({ passed: true, writes, errors, status: response.status() }));
});
