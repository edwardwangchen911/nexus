import { expect, test } from "@playwright/test";
import { chmod } from "node:fs/promises";
import { join } from "node:path";
import { formatDisplayDecimal } from "../../app/lib/display-decimal";
import type { components } from "../../app/lib/generated/schema";

type OrderPage = components["schemas"]["PaperOrderPageResponse"];
type Actual = components["schemas"]["PaperPlanActualResponse"];
type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];

test("controlled multi-candidate two-fill cycle is readable through BFF and four pages", async ({ page, request }) => {
  const cycleId = process.env.NEXUS_CONTROLLED_CYCLE_ID;
  const expectedOrderIds = process.env.NEXUS_CONTROLLED_ORDER_IDS?.split(",");
  const candidateCount = Number(process.env.NEXUS_CONTROLLED_CANDIDATE_COUNT);
  const deferCount = Number(process.env.NEXUS_CONTROLLED_DEFER_COUNT ?? String(candidateCount - 2));
  const capture = async (name: string) => {
    const directory = process.env.NEXUS_CONTROLLED_SCREENSHOT_DIR;
    if (!directory) return;
    await page.waitForLoadState("networkidle");
    const path = join(directory, `${name}.png`);
    await page.screenshot({ path, fullPage: true });
    await chmod(path, 0o600);
  };
  test.skip(!cycleId || expectedOrderIds?.length !== 2, "isolated controlled PostgreSQL cycle required");
  if (!cycleId || !expectedOrderIds) return;

  const [ordersResponse, actualResponse, detailResponse] = await Promise.all([
    request.get("/api/v1/paper/orders?limit=50"),
    request.get("/api/v1/paper/actual"),
    request.get(`/api/v1/paper/management/cycles/${cycleId}`),
  ]);
  const errorCode = async (response: typeof ordersResponse) =>
    response.ok() ? "" : (await response.json()).error?.code ?? "UNKNOWN_ERROR";
  expect(ordersResponse.status(), await errorCode(ordersResponse)).toBe(200);
  expect(actualResponse.status(), await errorCode(actualResponse)).toBe(200);
  expect(detailResponse.status(), await errorCode(detailResponse)).toBe(200);
  const orders = (await ordersResponse.json()).data as OrderPage;
  const actual = (await actualResponse.json()).data as Actual;
  const detail = (await detailResponse.json()).data as Detail;
  expect(orders.items.map((order) => order.order_id).sort()).toEqual([...expectedOrderIds].sort());
  expect(orders.items.every((order) => order.status === "filled" && order.fills.length > 0)).toBe(true);
  expect(actual.data_state).toBe("READY");
  expect(actual.positions).toHaveLength(2);
  expect(detail.cycle.cycle_id).toBe(cycleId);
  expect(detail.cycle.execution_locked).toBe(true);
  expect(detail.cycle.candidates).toHaveLength(candidateCount);
  expect(detail.synthesis?.choices).toHaveLength(candidateCount);
  expect(detail.synthesis?.choices.filter((choice) => choice.action === "defer")).toHaveLength(deferCount);
  const targetId = detail.active_target?.target_version_id;
  expect(targetId).toBeTruthy();

  const writes: string[] = [];
  page.on("request", (networkRequest) => {
    if (!["GET", "HEAD"].includes(networkRequest.method())) writes.push(networkRequest.method());
  });
  await page.goto("/workspace");
  await expect(page.getByRole("heading", { name: "交易工作台" })).toBeVisible();
  await expect(page.locator(".workbench-candidates li")).toHaveCount(candidateCount);
  await expect(page.getByRole("region", { name: "最新决策" }).getByText("本周期订单已终态")).toBeVisible();
  await expect(page.getByRole("region", { name: "活动订单" }).getByText("本周期订单已终态")).toBeVisible();
  await expect(page.getByRole("region", { name: "活动订单" }).getByRole("link", { name: "查看全部订单" })).toHaveAttribute("href", "/paper");
  const cash = page.getByRole("region", { name: "当前账本现金（内部模拟）" });
  await expect(cash.getByText(`USD ${formatDisplayDecimal(actual.available_cash!, { group: true })}`, { exact: true })).toBeVisible();
  await expect(cash.getByText(`USD ${formatDisplayDecimal(actual.reserved_cash!, { group: true })}`, { exact: true })).toBeVisible();
  await expect(cash).toContainText("读取时点");
  await expect(cash.getByRole("link", { name: "查看当前账本与持仓" })).toHaveAttribute("href", "/paper#paper-current-actual");
  await capture("workspace");

  await page.goto("/decisions");
  await expect(page.getByRole("heading", { name: "决策记录", exact: true })).toBeVisible();
  await expect(page.locator(`a[href="/decisions/${cycleId}"]`)).toBeVisible();
  await capture("decisions");
  await page.goto(`/decisions/${cycleId}`);
  await expect(page.getByRole("heading", { name: "组合决策详情" })).toBeVisible();
  await expect(page.locator("body")).toContainText(targetId!);
  await expect(page.locator(".pm-agent strong")).toHaveCount(candidateCount);
  await expect(page.locator(".pm-agent strong").filter({ hasText: "暂缓" })).toHaveCount(deferCount);
  await capture("decision-detail");

  await page.goto("/paper");
  await expect(page.getByRole("heading", { name: "内部模拟盘" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "当前实际持仓与现金" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "组合经济对照 · 证据与缺口" })).toBeVisible();
  await expect(page.getByText("经济结果尚未评估，不能证明策略有效或样本外通过。", { exact: true })).toBeVisible();
  await expect(page.getByText("经济配对与计划槽位数量：尚未具备权威，无法统计", { exact: true })).toBeVisible();
  for (const orderId of expectedOrderIds) {
    await expect(page.locator("article.paper-order").filter({ hasText: orderId })).toBeVisible();
  }
  await expect(page.getByRole("table", { name: "当前实际模拟持仓" }).getByRole("row")).toHaveCount(3);
  await capture("paper");

  await page.goto("/portfolio");
  await expect(page.getByRole("heading", { name: "组合事实" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "内部模拟目标与计划" })).toBeVisible();
  await capture("portfolio");
  expect(writes).toEqual([]);
});
