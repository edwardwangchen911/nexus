import { expect, test } from "@playwright/test";
import type { components } from "../../app/lib/generated/schema";

type OrderPage = components["schemas"]["PaperOrderPageResponse"];
type Actual = components["schemas"]["PaperPlanActualResponse"];
type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];

test("controlled two-fill cycle is readable through BFF and four pages", async ({ page, request }) => {
  const cycleId = process.env.NEXUS_CONTROLLED_CYCLE_ID;
  const expectedOrderIds = process.env.NEXUS_CONTROLLED_ORDER_IDS?.split(",");
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
  const targetId = detail.active_target?.target_version_id;
  expect(targetId).toBeTruthy();

  const writes: string[] = [];
  page.on("request", (networkRequest) => {
    if (!["GET", "HEAD"].includes(networkRequest.method())) writes.push(networkRequest.method());
  });
  await page.goto("/workspace");
  await expect(page.getByRole("heading", { name: "交易工作台" })).toBeVisible();

  await page.goto("/decisions");
  await expect(page.getByRole("heading", { name: "决策记录", exact: true })).toBeVisible();
  await expect(page.locator(`a[href="/decisions/${cycleId}"]`)).toBeVisible();
  await page.goto(`/decisions/${cycleId}`);
  await expect(page.getByRole("heading", { name: "组合决策详情" })).toBeVisible();
  await expect(page.locator("body")).toContainText(targetId!);

  await page.goto("/paper");
  await expect(page.getByRole("heading", { name: "内部模拟盘" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "当前实际持仓与现金" })).toBeVisible();
  for (const orderId of expectedOrderIds) {
    await expect(page.locator("article.paper-order").filter({ hasText: orderId })).toBeVisible();
  }
  await expect(page.getByRole("table", { name: "当前实际模拟持仓" }).getByRole("row")).toHaveCount(3);

  await page.goto("/portfolio");
  await expect(page.getByRole("heading", { name: "组合事实" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "内部模拟目标与计划" })).toBeVisible();
  expect(writes).toEqual([]);
});
