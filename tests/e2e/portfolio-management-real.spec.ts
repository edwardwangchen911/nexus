import { expect, test } from "@playwright/test";
import type { components } from "../../app/lib/generated/schema";

const enabled = process.env.NEXUS_REAL_PORTFOLIO_MANAGEMENT_ACCEPTANCE === "1";
type AgentPaperCyclePage = components["schemas"]["AgentPaperCyclePageResponse"];

test("one real portfolio cycle is readable across API and product pages", async ({ page, request }) => {
  test.skip(!enabled, "manual real-provider portfolio acceptance only");

  const cyclesResponse = await request.get("/api/v1/paper/cycles?limit=20");
  expect(cyclesResponse.status()).toBe(200);
  const cyclePage = (await cyclesResponse.json()).data as AgentPaperCyclePage;
  const cycle = cyclePage.items.find(
    (item) => item.decision?.sizing_policy_version === "portfolio_allocation_v1",
  );
  expect(cycle).toBeDefined();
  if (!cycle) throw new Error("a persisted portfolio cycle is required");
  expect(cycle.execution_locked).toBe(true);
  expect(cycle.candidate_count).toBeGreaterThanOrEqual(1);
  expect(cycle.candidate_count).toBeLessThanOrEqual(5);

  const detailResponse = await request.get(`/api/v1/paper/management/cycles/${cycle.cycle_id}`);
  expect(detailResponse.status()).toBe(200);
  const detail = (await detailResponse.json()).data;
  expect(detail.cycle.cycle_id).toBe(cycle.cycle_id);
  expect(detail.cycle.execution_locked).toBe(true);
  expect(detail.cycle.decision.sizing_policy_version).toBe("portfolio_allocation_v1");

  const summaryResponse = await request.get("/api/v1/paper/management/portfolio");
  expect(summaryResponse.status()).toBe(200);
  const summary = (await summaryResponse.json()).data;
  expect(summary.read_as_of).toBeTruthy();

  await page.goto("/paper");
  await expect(page.getByRole("heading", { name: "内部模拟目标与计划" })).toBeVisible();
  await expect(page.getByText("实盘保持锁定")).toBeVisible();
  await expect(page.getByRole("button", { name: /创建订单|下单|实盘买入|实盘卖出/ })).toHaveCount(0);
});
