import { expect, test } from "@playwright/test";

const enabled = process.env.NEXUS_REAL_AGENT_PAPER_ACCEPTANCE === "1";

test("real local Agent paper cycle is the same persisted fact in API and page", async ({ page, request }) => {
  test.skip(!enabled, "manual real-provider acceptance only");

  const response = await request.get("/api/v1/paper/cycles/latest");
  expect(response.status()).toBe(200);
  const envelope = await response.json();
  const cycle = envelope.data;
  expect(cycle).not.toBeNull();
  expect(cycle.execution_locked).toBe(true);
  expect(cycle.cycle_id).toMatch(/^[0-9a-f-]{36}$/);
  expect(cycle.market).toMatchObject({
    source: "alpaca",
    feed: "iex",
    delivery_mode: "real_time",
    window_minutes: 5,
  });

  await page.goto("/paper");
  await expect(page.getByRole("heading", { name: "最新 Agent 模拟周期" })).toBeVisible();
  const region = page.getByRole("region", { name: "最新 Agent 模拟周期" });
  await expect(region).toContainText(cycle.result_code);
  await expect(region).toContainText(cycle.decision_time);
  await expect(page.getByRole("button", { name: /创建订单|下单|实盘买入|实盘卖出/ })).toHaveCount(0);
});
