import { expect, test } from "@playwright/test";
import type { components } from "../../app/lib/generated/schema";

const enabled = process.env.NEXUS_REAL_AGENT_PAPER_ACCEPTANCE === "1";
type AgentPaperCycle = components["schemas"]["AgentPaperCycleResponse"];
type AgentPaperCyclePage = components["schemas"]["AgentPaperCyclePageResponse"];

test("two real intraday Agent paper cycles are independent persisted facts in API and page", async ({ page, request }) => {
  test.skip(!enabled, "manual real-provider acceptance only");

  const response = await request.get("/api/v1/paper/cycles?limit=20");
  expect(response.status()).toBe(200);
  const envelope = await response.json();
  const cyclePage = envelope.data as AgentPaperCyclePage;
  const cycles = cyclePage.items as AgentPaperCycle[];
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
  const evaluation = cyclePage.version_evaluation;
  if (evaluation === null) {
    await expect(page.getByRole("heading", { name: "Agent 版本并行评价" })).toHaveCount(0);
  } else {
    expect(evaluation.execution_locked).toBe(true);
    expect(evaluation.broker_write).toBe(false);
    expect(evaluation.live_write).toBe(false);
    expect(evaluation.train_completed).toBeLessThanOrEqual(4);
    expect(evaluation.oos_completed + evaluation.oos_incomparable + evaluation.oos_missing).toBe(20);
    expect(evaluation.comparisons.map((comparison) => comparison.key)).toEqual([
      "hold_current",
      "baseline_v1",
      "market_benchmark",
    ]);
    const evaluationRegion = page.getByRole("region", { name: "Agent 版本绩效对照，可横向滚动" });
    await expect(page.getByRole("heading", { name: "Agent 版本并行评价" })).toBeVisible();
    await expect(evaluationRegion).toContainText("incumbent");
    await expect(evaluationRegion).toContainText("challenger");
    await expect(page.getByText("执行锁定 · 券商写入关闭 · 实盘写入关闭")).toBeVisible();
  }
  await expect(page.getByRole("button", { name: /创建订单|下单|实盘买入|实盘卖出/ })).toHaveCount(0);
  await expect(page.getByRole("button", { name: /注册评价|推进评价|恢复评价|终结评价|晋升版本|券商写入|实盘写入/ })).toHaveCount(0);
});
