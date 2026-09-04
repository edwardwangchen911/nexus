import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { actionInputs } from "../unit/research/action-fixtures";
import { decisionTime, instrumentId, requestId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope, runId } from "../unit/research/fixtures";

for (const width of [375, 1440]) {
  test(`frozen actions are readable and inert at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 960 });
    await page.emulateMedia({ reducedMotion: "reduce" });
    const writes: string[] = [];
    page.on("request", (request) => {
      if (!["GET", "HEAD"].includes(request.method())) writes.push(request.url());
    });
    await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {
      error: { code: "NOT_FOUND", message: "No saved research.", request_id: requestId, details: {} },
    } }));
    await page.route("**/api/v1/integrations", (route) => route.fulfill({ json: {
      data: [], meta: { request_id: requestId, schema_version: "1", as_of: decisionTime },
    } }));
    await page.route(`**/api/v1/instruments/${instrumentId}/research-admission?**`, (route) => route.fulfill({ json: researchAdmissionEnvelope(researchAdmissionData({
      inputs: actionInputs(), eligible: false, reason: "prompt_authority_unavailable",
    })) }));
    await page.goto(`/research/${instrumentId}?as_of=${encodeURIComponent(decisionTime)}`);
    const region = page.getByRole("region", { name: "公司行动观测资料" });
    await expect(region).toBeVisible();
    await expect(page.getByRole("heading", { name: "TEST · 已冻结公司行动资料" })).toBeVisible();
    await expect(page.getByText("资料已冻结 · 研究待生成")).toBeVisible();
    await expect(region).toContainText("来源完整性未知");
    for (const label of ["查看公司行动观察记录", "观察 1 · 第 1 页", "公司行动证据与版本"]) {
      const summary = region.getByText(label, { exact: true });
      await summary.focus();
      await summary.press("Enter");
    }
    const table = region.getByRole("table", { name: "观察 1 的来源字段" });
    await expect(table).toContainText("<0.001");
    await expect(table).toContainText("原件数值超出展示范围");
    await expect(table).toContainText("未知（原件为空）");
    await expect(table.getByRole("row", { name: /^来源数值（rate）/ })).toContainText("0");
    const scroll = region.getByRole("region", { name: "观察 1 的来源字段表，可横向滚动" });
    const valueBox = await table.getByRole("row", { name: /^来源数值（rate）/ }).getByRole("cell").boundingBox();
    const scrollBox = await scroll.boundingBox();
    expect(valueBox!.x + valueBox!.width).toBeLessThanOrEqual(scrollBox!.x + scrollBox!.width + 1);
    await scroll.focus();
    await expect(scroll).toBeFocused();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
    expect((await new AxeBuilder({ page }).include("[aria-label='公司行动观测资料']").analyze()).violations
      .filter((item) => ["serious", "critical"].includes(item.impact ?? ""))).toEqual([]);
    await expect(page.getByRole("button", { name: /运行冻结研究|买入|卖出|下单/ })).toHaveCount(0);
    await page.screenshot({ path: `test-results/action-inputs-${width}.png`, fullPage: true });
    await page.getByRole("button", { name: "重新读取", exact: true }).click();
    await expect(region).toContainText("来源完整性未知");
    expect(writes).toEqual([]);
  });
}

test("exact historical run shows its own action inputs and does not request admission on refresh", async ({ page }) => {
  const requests: string[] = [];
  await page.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {} }));
  await page.route(`**/api/v1/research-runs/${runId}`, (route) => route.fulfill({ json: researchEnvelope(researchData({
    inputs: actionInputs(), status: "failed", intent: null, quality_status: null,
    abstention_reason: "model_usage_uncertain",
  })) }));
  page.on("request", (request) => { if (request.url().includes("/api/v1/")) requests.push(request.method() + " " + request.url()); });
  await page.goto(`/research/${instrumentId}?run_id=${runId}&as_of=2026-09-04T00:00:00Z`);
  const region = page.getByRole("region", { name: "公司行动观测资料" });
  await expect(region).toBeVisible();
  await expect(region).toContainText("2026-08-01 至 2026-08-23");
  await page.getByRole("button", { name: "重新读取", exact: true }).click();
  await expect.poll(() => requests.filter((url) => url.endsWith(`/research-runs/${runId}`)).length).toBe(2);
  expect(requests.some((url) => url.includes("research-admission"))).toBe(false);
  expect(requests.some((url) => !url.startsWith("GET "))).toBe(false);
  await expect(page.getByRole("button", { name: /运行冻结研究/ })).toHaveCount(0);
});
