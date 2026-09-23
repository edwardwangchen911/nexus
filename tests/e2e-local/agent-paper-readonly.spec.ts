import { expect, test } from "@playwright/test";
import type { components } from "../../app/lib/generated/schema";

const enabled = process.env.NEXUS_LOCAL_PAPER_READONLY === "1";
type Cycle = components["schemas"]["AgentPaperCycleResponse"];
type CyclePage = components["schemas"]["AgentPaperCyclePageResponse"];
type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];
type Summary = components["schemas"]["PaperManagementSummaryResponse"];
type Actual = components["schemas"]["PaperPlanActualResponse"];

test("one persisted portfolio cycle agrees across four read-only pages", async ({ page, request }) => {
  test.skip(!enabled, "explicit local read-only acceptance only");

  const latestResponse = await request.get("/api/v1/paper/cycles/latest");
  expect(latestResponse.status()).toBe(200);
  const latest = (await latestResponse.json()).data as Cycle;
  const cyclesResponse = await request.get(
    `/api/v1/paper/cycles?decision_session=${latest.decision_session}&limit=20`,
  );
  expect(cyclesResponse.status()).toBe(200);
  const cyclePage = (await cyclesResponse.json()).data as CyclePage;
  const cycle = (cyclePage.items as Cycle[]).find(
    (item) => item.status === "completed" && item.decision?.sizing_policy_version === "portfolio_allocation_v1",
  );
  expect(cycle, "a persisted completed portfolio decision is required").toBeDefined();
  if (!cycle) return;
  expect(cycle.execution_locked).toBe(true);

  const detailResponse = await request.get(`/api/v1/paper/management/cycles/${cycle.cycle_id}`);
  const summaryResponse = await request.get("/api/v1/paper/management/portfolio");
  const actualResponse = await request.get("/api/v1/paper/actual");
  expect(detailResponse.status()).toBe(200);
  expect(summaryResponse.status()).toBe(200);
  expect(actualResponse.status()).toBe(200);
  const actual = (await actualResponse.json()).data as Actual;
  expect(actual.data_state).toBe("READY");
  expect(actual.as_of).not.toBeNull();
  const detail = (await detailResponse.json()).data as Detail;
  const summary = (await summaryResponse.json()).data as Summary;
  expect(detail.cycle.cycle_id).toBe(cycle.cycle_id);
  expect(detail.cycle.result_code).toBe(cycle.result_code);
  expect(detail.proposal?.strategy_decision_id).toBe(cycle.decision?.decision_id);
  expect(detail.proposal?.strategy_decision_id).toBe(summary.latest_proposal?.strategy_decision_id);
  const targetId = detail.active_target?.target_version_id;
  expect(targetId).toBeTruthy();
  expect(summary.current_target?.target_version_id).toBe(targetId);

  const writeRequests: string[] = [];
  page.on("request", (networkRequest) => {
    if (!["GET", "HEAD"].includes(networkRequest.method())) writeRequests.push(networkRequest.method());
  });

  await page.goto("/workspace");
  await page.waitForLoadState("networkidle");
  await expect(page.getByRole("heading", { name: "交易工作台" })).toBeVisible();
  await expect(page.locator("body")).toContainText("代码提交、迁移与心跳报告匹配");

  await page.goto("/decisions");
  await page.waitForLoadState("networkidle");
  await expect(page.getByRole("heading", { name: "决策记录", exact: true })).toBeVisible();
  await expect(page.getByLabel("决策日期")).toHaveValue(cycle.decision_session);
  await expect(page.locator(`a[href="/decisions/${cycle.cycle_id}"]`)).toBeVisible();

  await page.goto(`/decisions/${cycle.cycle_id}`);
  await page.waitForLoadState("networkidle");
  await expect(page.getByRole("heading", { name: "组合决策详情" })).toBeVisible();
  await expect(page.getByText("当前生效目标", { exact: true }).first()).toBeVisible();

  await page.goto("/paper");
  await page.waitForLoadState("networkidle");
  await expect(page.getByRole("heading", { name: "内部模拟盘" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "当前实际持仓与现金" })).toBeVisible();
  await expect(page.locator("#paper-current-actual")).toContainText("可用现金");
  await page.getByLabel("决策会话日期").fill(cycle.decision_session);
  await expect(page.getByRole("heading", { name: "所选会话 Agent 模拟周期" })).toBeVisible();
  await page.locator(`select:has(option[value="${cycle.cycle_id}"])`).selectOption(cycle.cycle_id);
  await expect(page.locator("body")).toContainText(cycle.result_code);
  await expect(page.locator("body")).toContainText(targetId!);

  await page.goto("/portfolio");
  await page.waitForLoadState("networkidle");
  await expect(page.getByRole("heading", { name: "组合事实" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "内部模拟目标与计划" })).toBeVisible();

  const readbackResponse = await request.get("/api/v1/paper/management/portfolio");
  expect(readbackResponse.status()).toBe(200);
  const readback = (await readbackResponse.json()).data as Summary;
  expect(readback.current_target?.target_version_id).toBe(targetId);
  expect(readback.latest_proposal?.strategy_decision_id).toBe(detail.proposal?.strategy_decision_id);
  expect(writeRequests).toEqual([]);
});
