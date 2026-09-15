import { expect, test } from "@playwright/test";
import type { components } from "../../app/lib/generated/schema";

const enabled = process.env.NEXUS_REAL_AGENT_PAPER_ACCEPTANCE === "1";
type AgentPaperCycle = components["schemas"]["AgentPaperCycleResponse"];

test("two real intraday Agent paper cycles are independent persisted facts in API and page", async ({ page, request }) => {
  test.skip(!enabled, "manual real-provider acceptance only");

  const response = await request.get("/api/v1/paper/cycles?limit=20");
  expect(response.status()).toBe(200);
  const envelope = await response.json();
  const cycles = envelope.data.items as AgentPaperCycle[];
  expect(cycles.length).toBeGreaterThanOrEqual(2);
  const [newerCycle, olderCycle] = cycles;
  if (!newerCycle.decision || !olderCycle.decision) {
    throw new Error("both accepted intraday cycles must persist an Agent decision");
  }
  expect(newerCycle.cycle_id).not.toBe(olderCycle.cycle_id);
  expect(newerCycle.decision.decision_id).toMatch(/^[0-9a-f-]{36}$/);
  expect(olderCycle.decision.decision_id).toMatch(/^[0-9a-f-]{36}$/);
  expect(newerCycle.decision.decision_id).not.toBe(olderCycle.decision.decision_id);
  const newerOutputs = newerCycle.candidates.flatMap((candidate) =>
    candidate.output_id ? [candidate.output_id] : [],
  );
  const olderOutputs = olderCycle.candidates.flatMap((candidate) =>
    candidate.output_id ? [candidate.output_id] : [],
  );
  expect(newerOutputs.length).toBeGreaterThan(0);
  expect(olderOutputs.length).toBeGreaterThan(0);
  expect(newerOutputs.every((outputId) => !olderOutputs.includes(outputId))).toBe(true);
  for (const cycle of [newerCycle, olderCycle]) {
    expect(cycle.execution_locked).toBe(true);
    expect(cycle.cycle_id).toMatch(/^[0-9a-f-]{36}$/);
    expect(cycle.decision_market).toMatchObject({
      source: "schwab",
      feed: "l1",
      delivery_mode: "real_time",
      delayed: false,
    });
    expect(cycle.execution_market).toMatchObject({
      source: "schwab",
      feed: "price_history",
      delivery_mode: "unknown",
      window_minutes: 5,
    });
  }

  await page.goto("/paper");
  await expect(page.getByRole("heading", { name: "当日 Agent 模拟周期" })).toBeVisible();
  const region = page.getByRole("region", { name: "当日 Agent 模拟周期" });
  await expect(region).toContainText(newerCycle.result_code);
  await expect(region.locator(`time[datetime="${newerCycle.decision_time}"]`)).toHaveCount(1);
  await region.locator("select").selectOption(olderCycle.cycle_id);
  await expect(region).toContainText(olderCycle.result_code);
  await expect(region.locator(`time[datetime="${olderCycle.decision_time}"]`)).toHaveCount(1);
  await expect(region).toContainText("决策报价 · Schwab Level One · 实时");
  await expect(region).toContainText("执行分钟线 · Schwab pricehistory · 实时性未由响应声明");
  await expect(page.getByRole("button", { name: /创建订单|下单|实盘买入|实盘卖出/ })).toHaveCount(0);
});
