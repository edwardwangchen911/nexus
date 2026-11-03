import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "@playwright/test";

const root = process.env.NEXUS_TEST_MONITOR_BROWSER_DIR;
test.skip(!root, "requires the isolated authenticated engine fixture");
test("shows one missing price window alongside the latest unknown observation", async ({ page }) => {
  const ready = JSON.parse(readFileSync(join(root!, "ready.json"), "utf8"));
  const response = page.waitForResponse((r) => r.url().includes(`/api/v1/research-runs/${ready.run_id}`));
  await page.goto(`/research/${ready.instrument_id}?run_id=${ready.run_id}`);
  const received = await response;
  expect(received.status()).toBe(200);
  expect((await received.json()).data.price_check_windows.missing).toEqual([ready.missing]);
  const coverage = page.getByRole("region", { name: "五分钟价格检查覆盖" });
  await expect(coverage).toContainText("应查 3 次，已有记录 2 次");
  await expect(coverage).toContainText("22:05:00");
  await expect(page.getByText("该次检查结果未知")).toBeVisible();
  await page.screenshot({ path: join(root!, "coverage.png"), fullPage: true });
});
