import fs from "node:fs";
import path from "node:path";
import { expect, test } from "@playwright/test";

const directory = process.env.NEXUS_TEST_MONITOR_BROWSER_DIR;
const fixture = directory && path.join(directory, "single-sol.json");
test.skip(!fixture || !fs.existsSync(fixture), "requires the isolated single-stock request");

test("reads one protected unknown through the real manual lookup and workspace", async ({ page }) => {
  const { request_id, disposition } = JSON.parse(fs.readFileSync(fixture!, "utf8"));
  const methods: string[] = [];
  await page.route("**/*", route => {
    const request = route.request();
    if (!["localhost", "127.0.0.1"].includes(new URL(request.url()).hostname)) return route.abort();
    if (request.url().includes("/api/")) {
      methods.push(request.method());
      if (request.method() !== "GET") return route.abort();
    }
    return route.continue();
  });
  await page.addInitScript(() => sessionStorage.setItem("nexus-manual-paper-attempt-v1",
    JSON.stringify({ key: "one-v4", mode: "research_only" })));
  const lookup = page.waitForResponse(r => new URL(r.url()).pathname === "/api/v1/paper/manual-requests");
  await page.goto("/workspace");
  const response = await lookup;
  expect(response.status()).toBe(200);
  const { data } = await response.json();
  expect(data).toMatchObject({ request_id, status: "unknown", code: "MANUAL_RESULT_UNKNOWN", execution_locked: true, research_contract: "single_sol_research_v1" });
  expect(data.independent_disposition ?? null).toEqual(disposition);
  expect(data.research.members).toHaveLength(1);
  await expect(page.getByText("研究结果尚未确认", { exact: true })).toBeVisible();
  await expect(page.getByText("用量未知，保留预算占用", { exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "查看第 1 只证券研究" })).toBeVisible();
  if (disposition === "unknown_acknowledged") {
    await expect(page.getByText(/独立研究未知影响已明确接受/)).toBeVisible();
  } else {
    await expect(page.getByText(/独立研究未知影响已明确接受/)).toHaveCount(0);
    await expect(page.getByRole("button", { name: "开始新的请求" })).toHaveCount(0);
    if (disposition === "recheck_required") await expect(page.getByText(/独立研究的接受记录需要重新核验/)).toBeVisible();
  }
  await page.screenshot({ path: path.join(directory!, "workspace.png"), fullPage: true });
  expect(methods.every(m => m === "GET")).toBe(true);
});
