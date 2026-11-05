import fs from "node:fs";
import path from "node:path";
import { expect, test } from "@playwright/test";

const directory = process.env.NEXUS_TEST_MONITOR_BROWSER_DIR;
const fixture = directory && path.join(directory, "budgeted.json");
test.skip(!fixture || !fs.existsSync(fixture), "requires persisted isolated budgeted request");

test("normal lookup and research page preserve budgeted owner and unknown protection", async ({ page }) => {
  const saved = JSON.parse(fs.readFileSync(fixture!, "utf8"));
  const writes: string[] = [];
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(String(error)));
  await page.route("**/*", route => {
    const request = route.request();
    if (!["127.0.0.1", "localhost"].includes(new URL(request.url()).hostname)) return route.abort();
    if (request.url().includes("/api/") && request.method() !== "GET") {
      writes.push(request.method());
      return route.abort();
    }
    return route.continue();
  });
  await page.addInitScript(key => sessionStorage.setItem("nexus-manual-paper-attempt-v1",
    JSON.stringify({ key, mode: "research_only" })), saved.key);
  const lookup = page.waitForResponse(r => new URL(r.url()).pathname === "/api/v1/paper/manual-requests");
  const summary = page.waitForResponse(r => new URL(r.url()).pathname === "/api/v1/workbench");
  await page.goto("/workspace");
  await page.waitForLoadState("networkidle");
  const response = await lookup;
  expect(response.status()).toBe(200);
  expect((await response.json()).data).toEqual(saved.receipt);
  const scopes = (await (await summary).json()).data.activity.scopes;
  expect(scopes.find((scope: { mode: string }) => scope.mode === "research_only")).toEqual(saved.activity_scope);
  await expect(page.getByText("WORKBENCH_ACTIVITY_UNVERIFIED", { exact: false })).toHaveCount(0);
  const heading: Record<string, string> = {
    complete: "研究处理已完成", output_limit: "研究条件未满足，请求已结束", timeout: "研究结果尚未确认",
  };
  await expect(page.getByText(heading[saved.outcome], { exact: true })).toBeVisible();
  await expect(page.getByText(saved.outcome === "timeout" ? "用量未知，保留预算占用" : "用量已确认", { exact: true })).toBeVisible();
  await expect(page.getByText(/独立研究未知影响已明确接受/)).toHaveCount(0);
  if (saved.outcome === "timeout") await expect(page.getByRole("button", { name: "开始新的请求" })).toHaveCount(0);
  const candidate = page.getByRole("article", { name: "AAPL 本轮研究" });
  await expect(candidate).toBeVisible();
  await expect(candidate).not.toContainText("原因不可核验");
  if (saved.outcome === "output_limit") await expect(candidate).toContainText("模型达到输出上限，未返回完整研究结果");
  if (saved.outcome === "timeout") await expect(candidate).toContainText("部分模型调用未返回用量");
  await page.screenshot({ path: path.join(directory!, "workspace.png"), fullPage: true });
  const research = page.waitForResponse(r => new URL(r.url()).pathname === `/api/v1/research-runs/${saved.run_id}`);
  await page.getByRole("link", { name: "查看第 1 只证券研究" }).click();
  await page.waitForLoadState("networkidle");
  const detail = await research;
  expect(detail.status()).toBe(200);
  const body = (await detail.json()).data;
  expect(body.run_id).toBe(saved.run_id);
  expect(body.status).toBe(saved.research.status);
  expect(body.settled_cost_usd).toEqual(saved.research.settled_cost_usd);
  expect(body.abstention_reason).toEqual(saved.research.abstention_reason);
  const panel = page.getByRole("region", { name: "个股研究证据页" });
  await expect(panel).toBeVisible();
  if (saved.outcome === "output_limit") await expect(panel).toContainText("模型达到输出上限，未返回完整研究结果");
  if (saved.outcome === "timeout") await expect(panel).toContainText("部分模型调用未返回用量");
  const refreshed = page.waitForResponse(r => new URL(r.url()).pathname === `/api/v1/research-runs/${saved.run_id}`);
  await page.getByRole("button", { name: "重新读取" }).click();
  expect((await (await refreshed).json()).data.run_id).toBe(saved.run_id);
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.screenshot({ path: path.join(directory!, "research-desktop.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: path.join(directory!, "research-mobile.png"), fullPage: true });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
  expect(writes).toEqual([]);
  expect(errors).toEqual([]);
  fs.writeFileSync(path.join(directory!, "browser-proof.json"), JSON.stringify({ passed: true, writes, errors }));
});
