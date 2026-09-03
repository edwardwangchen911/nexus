import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

for (const viewport of [
  { name: "mobile-390", width: 390, height: 844 },
  { name: "desktop-1440", width: 1440, height: 1000 },
]) {
  test(`${viewport.name} integrations exposes safe system evidence without credentials`, async ({ page }) => {
    const apiWrites: string[] = [];
    page.on("request", (request) => {
      if (new URL(request.url()).pathname.startsWith("/api/") && request.method() !== "GET") {
        apiWrites.push(`${request.method()} ${new URL(request.url()).pathname}`);
      }
    });
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await installFixture(page);
    await page.goto("/integrations");

    await expect(page.getByRole("heading", { name: "数据与能力状态" })).toBeVisible({ timeout: 15_000 });
    await expect(page.getByText("系统能力 · 集成状态", { exact: true })).toBeVisible();
    const systemEvidence = page.getByRole("status", { name: "全局系统状态" }).locator("details");
    await expect(systemEvidence).not.toHaveAttribute("open", "");
    await systemEvidence.locator("summary").click();
    await expect(systemEvidence).toHaveAttribute("open", "");
    await systemEvidence.getByRole("button", { name: "刷新状态" }).click();
    await expect(systemEvidence.getByText("状态证据与权限边界")).toBeVisible();
    const card = page.getByRole("region", { name: "fosun 集成（只读）" });
    await expect(card).toContainText("FOSUN");
    await expect(card).toContainText("离线示例");
    await expect(page.getByText(/api_key|account_id|secret|token/i)).toHaveCount(0);
    await expect(page.getByRole("link", { name: "实盘逐笔批准" })).toBeVisible();

    expect(apiWrites).toEqual([]);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(serious).toEqual([]);
    await page.screenshot({ path: `test-results/integrations-${viewport.name}.png`, fullPage: true });
  });
}

async function installFixture(page: Page): Promise<void> {
  await page.route("**/api/v1/integrations", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      data: [{
        provider: "fosun",
        mode: "fixture",
        quality: "PASS",
        as_of: "2026-08-31T20:30:00Z",
        capability_expires_at: null,
        contract_hash: null,
        next_action: "只读研究模式。",
        version: 1,
      }],
      meta: {
        request_id: "0198f7a0-1111-7777-8111-111111111115",
        schema_version: "1",
        as_of: "2026-08-31T20:30:00Z",
      },
    }),
  }));
}
