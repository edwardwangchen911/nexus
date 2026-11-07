import fs from "node:fs";
import path from "node:path";
import { expect, test } from "@playwright/test";

const directory = process.env.NEXUS_TEST_MONITOR_BROWSER_DIR;
test.skip(!directory || !fs.existsSync(path.join(directory, "ready.json")), "requires an isolated Sol cycle");

test("reads the frozen Sol portfolio evidence", async ({ page, request }) => {
  const ready = JSON.parse(fs.readFileSync(path.join(directory!, "ready.json"), "utf8"));
  const detail = ready.cycle;
  test.skip(!detail, "requires portfolio facts");
  const id = detail.cycle.cycle_id;
  const writes: string[] = [];
  page.on("request", r => { if (r.url().includes("/api/") && r.method() !== "GET") writes.push(r.method()); });
  await page.route("**/*", route => ["127.0.0.1", "localhost"].includes(new URL(route.request().url()).hostname)
    ? route.continue() : route.abort());
  const api = await request.get(`/api/v1/paper/management/cycles/${id}`);
  expect(api.status()).toBe(200);
  expect((await api.json()).data).toEqual(detail);
  const detailRead = page.waitForResponse(response => response.url().includes(`/api/v1/paper/management/cycles/${id}`));
  await page.goto(`/decisions/${id}`);
  const detailResponse = await detailRead;
  expect(detailResponse.status()).toBe(200);
  expect((await detailResponse.json()).data).toEqual(detail);
  const usage = page.getByRole("note", { name: "组合模型用量" });
  await expect(usage).toContainText("Responses API · gpt-6.1-sol");
  if (detail.synthesis.responses_usage.settled_cost_usd === null) {
    await expect(usage).toContainText("费用未知，预算保留");
    await expect(usage).toContainText("响应回执模型 · 未知");
    await expect(usage).toContainText("失败阶段 · 发送请求 · 超时");
    await expect(page.getByRole("note", { name: "组合用量历史处置" })).toContainText("尚未接受");
  } else {
    await expect(usage).toContainText(`已结算 USD ${detail.synthesis.responses_usage.settled_cost_usd}`);
    await expect(usage).toContainText("响应回执模型 · gpt-6.1-sol");
    await expect(page.locator(".pm-agent strong").filter({hasText: "暂缓"})).toHaveCount(3);
  }
  await page.screenshot({ path: path.join(directory!, "portfolio.png"), fullPage: true });
  expect(writes).toEqual([]);
});

test("reads the same cycle's frozen Sol stock evidence", async ({ page }) => {
  const ready = JSON.parse(fs.readFileSync(path.join(directory!, "ready.json"), "utf8"));
  test.skip(!ready.cycle, "requires portfolio facts");
  const writes: string[] = [];
  page.on("request", r => { if (r.url().includes("/api/") && r.method() !== "GET") writes.push(r.method()); });
  await page.route("**/*", route => ["127.0.0.1", "localhost"].includes(new URL(route.request().url()).hostname)
    ? route.continue() : route.abort());
  for (const stock of ready.stocks) {
    const endpoint = `/api/v1/research-runs/${stock.run_id}`;
    const read = page.waitForResponse(r => r.url().includes(endpoint));
    await page.goto(`/research/${stock.instrument_id}?run_id=${stock.run_id}`);
    const response = await read;
    expect(response.status()).toBe(200);
    const data = (await response.json()).data;
    expect(data.quality_status).toBe(stock.quality_status);
    expect(data.monitoring.coverage_reason).toBe(stock.monitoring.coverage_reason);
    await expect(page.getByRole("region", {name: "个股研究证据页"})).toHaveAttribute("data-state", /^(ready|partial)$/);
    const monitoring = page.getByRole("region", {name: "价格监控记录"});
    await expect(monitoring).toBeVisible();
    if (data.monitoring.coverage_reason === "audit_rejected") {
      await expect(monitoring).toContainText("研究审查未通过");
    }
    const bill = page.getByRole("note", {name: "个股模型用量"});
    await expect(bill).toContainText("Responses API");
    await expect(bill).toContainText(`已结算 USD ${stock.settled_cost_usd}`);
  }
  expect(writes).toEqual([]);
});
