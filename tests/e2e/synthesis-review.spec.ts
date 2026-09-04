import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { decisionTime, instrumentId, requestId, researchEnvelope, synthesisResearchData } from "../unit/research/fixtures";

for (const viewport of [{ width: 375, height: 812 }, { width: 390, height: 844 }, { width: 1440, height: 1000 }]) {
  test(`saved synthesis reviews remain model assertions and read-only at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: "reduce" });
    const writes: string[] = [];
    page.on("request", (request) => {
      if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url());
    });
    const research = synthesisResearchData();
    const rationale = `合成理由：${"longword".repeat(45)}`;
    research.stage_trace!.stages[0].roles[0].synthesis_assessment!.reviews[0].rationale = rationale;
    await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {} }));
    await page.route("**/api/v1/integrations", (route) => route.fulfill({ json: {
      data: [{ provider: "fosun", mode: "fixture", quality: "PASS", as_of: decisionTime,
        capability_expires_at: null, contract_hash: null, next_action: "只读研究模式。", version: 1 }],
      meta: { request_id: requestId, schema_version: "1", as_of: decisionTime },
    } }));
    await page.route(`**/api/v1/instruments/${instrumentId}/research?**`, (route) => route.fulfill({ json: researchEnvelope(research) }));
    await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
    const toggle = page.getByText("逐项审查（模型自评）", { exact: true });
    await expect(toggle).toBeVisible();
    const details = toggle.locator("..");
    await expect(details).not.toHaveAttribute("open");
    await toggle.focus();
    await toggle.press("Enter");
    await expect(details).toHaveAttribute("open");
    await expect(details).toContainText("不是独立核验结论");
    await expect(details.getByText("模型自评：已解决", { exact: true })).toHaveCount(2);
    await expect(details).toContainText("模型自评的审查充分性：充分");
    await expect(details).toContainText("估值依据仍未验证。");
    await expect(details).toContainText("模型未列出缺失证据（不等于没有缺口）");
    await expect(page.locator(".decision-card__badges")).toContainText("资料不完整");
    await expect(page.getByText(/交易保持锁定/)).toBeVisible();
    await expect(details.getByText(rationale, { exact: true })).toBeVisible();
    const opener = details.getByRole("button", { name: "查看证据定位", exact: true }).first();
    await opener.focus();
    await opener.press("Enter");
    const drawer = page.getByRole("dialog", { name: "研究摘录与证据定位" });
    await expect(drawer).toContainText(rationale);
    await expect(drawer).toContainText("摘录来自研究输出，不是来源原文");
    await drawer.press("Escape");
    await expect(opener).toBeFocused();
    expect(await opener.evaluate((element) => {
      const bounds = element.getBoundingClientRect();
      return element.contains(document.elementFromPoint(bounds.x + bounds.width / 2, bounds.y + bounds.height / 2));
    })).toBe(true);
    const serious = (await new AxeBuilder({ page }).include(".decision-card__reviews").analyze()).violations
      .filter((item) => ["serious", "critical"].includes(item.impact ?? ""));
    expect(serious).toEqual([]);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    await expect(page.getByRole("button", { name: /运行冻结研究|买入|卖出|下单/ })).toHaveCount(0);
    expect(writes).toEqual([]);
    await toggle.focus();
    await toggle.scrollIntoViewIfNeeded();
    await page.evaluate(() => window.scrollBy(0, -80));
    await page.screenshot({ path: `test-results/synthesis-review-${viewport.width}.png` });
    await toggle.press("Space");
    await expect(details).not.toHaveAttribute("open");
  });
}
