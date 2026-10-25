import { expect, test } from "@playwright/test";
import { chmod, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { formatDisplayDecimal } from "../../app/lib/display-decimal";
import type { components } from "../../app/lib/generated/schema";

test("controlled Workspace starts one manual request through the real BFF", async ({ page, request }) => {
  test.skip(process.env.NEXUS_CONTROLLED_MANUAL_SUBMIT !== "1", "isolated manual submission phase required");
  await page.addInitScript(() => Object.defineProperty(crypto, "randomUUID", { value: () => "v10-entry-result" }));
  await page.goto("/workspace");
  const manual = page.getByRole("region", { name: "开始研究与内部模拟" });
  const writes: string[] = [];
  page.on("request", (r) => {
    if (!["GET", "HEAD"].includes(r.method())) writes.push(r.method());
  });
  const recoveredId = process.env.NEXUS_CONTROLLED_RECOVERED_MANUAL_ID;
  let original: unknown;
  if (recoveredId) {
    const old = await request.get("/api/v1/paper/manual-requests", { headers: { "Idempotency-Key": "interrupted" } });
    expect(old.status()).toBe(200);
    original = (await old.json()).data;
    expect(original).toMatchObject({ request_id: recoveredId, status: "unknown", cycle_id: null, preparation_disposition: "unknown_acknowledged", execution_locked: true });
    await page.evaluate(() => sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "interrupted", mode: "research_and_paper" })));
    await page.reload();
    await expect(manual.getByText("周期结果尚未确认", { exact: true })).toBeVisible();
    await expect(manual.getByText(/准备结果未知已明确接受，调度占用已解除/)).toBeVisible();
    await expect(manual.getByRole("button", { name: "研究并内部模拟", exact: true })).toBeDisabled();
    await manual.getByRole("button", { name: "开始新的请求", exact: true }).click();
    expect(await page.evaluate(() => sessionStorage.getItem("nexus-manual-paper-attempt-v1"))).toBeNull();
    expect(writes).toEqual([]);
  }
  const response = page.waitForResponse((r) => r.url().endsWith("/api/v1/paper/manual-requests") && r.request().method() === "POST");
  await manual.getByRole("button", { name: "研究并内部模拟", exact: true }).click();
  const posted = await response;
  expect(posted.status()).toBe(200);
  const data = (await posted.json()).data;
  expect(data.status).toBe("queued");
  expect(data.mode).toBe("research_and_paper");
  expect(data.execution_locked).toBe(true);
  expect(posted.request().headers()["idempotency-key"]).toBe("v10-entry-result");
  await expect(page.getByText(/已登记，等待宿主处理/)).toBeVisible();
  await page.reload();
  await expect(manual.getByText(/已登记，等待宿主处理/)).toBeVisible();
  expect(writes).toEqual(["POST"]);
  if (recoveredId) {
    expect(data.request_id).not.toBe(recoveredId);
    const old = await request.get("/api/v1/paper/manual-requests", { headers: { "Idempotency-Key": "interrupted" } });
    expect(old.status()).toBe(200);
    expect((await old.json()).data).toEqual(original);
  }
  await writeFile(process.env.NEXUS_CONTROLLED_MANUAL_RECEIPT_PATH!, JSON.stringify(data), { mode: 0o600 });
});

type OrderPage = components["schemas"]["PaperOrderPageResponse"];
type Actual = components["schemas"]["PaperPlanActualResponse"];
type Summary = components["schemas"]["PaperManagementSummaryResponse"];
type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];

test("registered economic protocol is read-only through real BFF and Paper", async ({ page, request }) => {
  const hash = process.env.NEXUS_ECONOMIC_PROTOCOL_HASH;
  test.skip(!hash, "isolated persisted economic protocol required");
  const response = await request.get("/api/v1/paper/economic-evidence");
  expect(response.status()).toBe(200);
  const data = (await response.json()).data as components["schemas"]["PortfolioEconomicEvidenceResponse"];
  expect(data.protocol_status).toBe("registered_not_activated");
  expect(data.protocol?.protocol_hash).toBe(hash);
  expect(data.protocol?.slots).toHaveLength(24);
  expect(data.planned_slot_count).toBe(24);
  expect(data.economic_pair_count).toBeNull();
  expect(data.execution_locked).toBe(true);
  const writes: string[] = [];
  page.on("request", (networkRequest) => {
    if (!["GET", "HEAD"].includes(networkRequest.method())) writes.push(networkRequest.method());
  });
  await page.goto("/paper");
  await expect(page.getByText("评价协议已登记，尚未激活。", { exact: true })).toBeVisible();
  await expect(page.getByText("计划槽位：24（训练 4 / 样本外 20）；经济配对数量仍未知。", { exact: true })).toBeVisible();
  await expect(page.getByRole("table", { name: "经济评价计划槽位" }).getByRole("row")).toHaveCount(25);
  await expect(page.getByText("固定策略基线尚未实现", { exact: true })).toBeVisible();
  await expect(page.getByText("共同合法执行窗口尚未验证", { exact: true })).toBeVisible();
  await expect(page.locator("code").filter({ hasText: hash! })).toBeVisible();
  await page.reload();
  await expect(page.getByText("评价协议已登记，尚未激活。", { exact: true })).toBeVisible();
  expect(writes).toEqual([]);
  const second = (await (await request.get("/api/v1/paper/economic-evidence")).json()).data;
  expect(second.protocol).toEqual(data.protocol);
  expect(second.economic_pair_count).toBeNull();
});

test("controlled multi-candidate two-fill cycle is readable through BFF and four pages", async ({ page, request }) => {
  const cycleId = process.env.NEXUS_CONTROLLED_CYCLE_ID;
  const expectedOrderIds = process.env.NEXUS_CONTROLLED_ORDER_IDS?.split(",");
  const firstPartial = process.env.NEXUS_CONTROLLED_FIRST_PARTIAL === "1";
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

  const [ordersResponse, actualResponse, detailResponse, summaryResponse] = await Promise.all([
    request.get("/api/v1/paper/orders?limit=50"),
    request.get("/api/v1/paper/actual"),
    request.get(`/api/v1/paper/management/cycles/${cycleId}`),
    request.get("/api/v1/paper/management/portfolio"),
  ]);
  const errorCode = async (response: Pick<typeof ordersResponse, "ok" | "json">) =>
    response.ok() ? "" : (await response.json()).error?.code ?? "UNKNOWN_ERROR";
  expect(ordersResponse.status(), await errorCode(ordersResponse)).toBe(200);
  expect(actualResponse.status(), await errorCode(actualResponse)).toBe(200);
  expect(detailResponse.status(), await errorCode(detailResponse)).toBe(200);
  expect(summaryResponse.status(), await errorCode(summaryResponse)).toBe(200);
  const summary = (await summaryResponse.json()).data as Summary;
  const orders = (await ordersResponse.json()).data as OrderPage;
  const actual = (await actualResponse.json()).data as Actual;
  const detail = (await detailResponse.json()).data as Detail;
  expect(orders.items.map((order) => order.order_id).sort()).toEqual([...expectedOrderIds].sort());
  expect(orders.items.every((order) => order.fills.length > 0)).toBe(true);
  expect(orders.items.map((order) => order.status).sort()).toEqual(firstPartial ? ["filled", "partially_filled"] : ["filled", "filled"]);
  if (firstPartial) expect(Number(actual.reserved_cash)).toBeGreaterThan(0);
  expect(actual.data_state).toBe("READY");
  expect(actual.positions).toHaveLength(2);
  expect(detail.cycle.cycle_id).toBe(cycleId);
  expect(detail.cycle.execution_locked).toBe(true);
  expect(detail.cycle.candidates).toHaveLength(candidateCount);
  expect(detail.synthesis?.choices).toHaveLength(candidateCount);
  expect(detail.synthesis?.choices.filter((choice) => choice.action === "defer")).toHaveLength(deferCount);
  const targetId = detail.active_target?.target_version_id;
  expect(targetId).toBeTruthy();
  expect(summary.current_target?.target_version_id).toBe(targetId);

  const writes: string[] = [];
  page.on("request", (networkRequest) => {
    if (!["GET", "HEAD"].includes(networkRequest.method())) writes.push(networkRequest.method());
  });
  const workbenchReads: { status: number; code: string | null; response_request_id: string | null }[] = [];
  let workbenchReadsTruncated = false;
  page.on("response", async (response) => {
    if (new URL(response.url()).pathname !== "/api/v1/workbench" || response.request().method() !== "GET") return;
    if (workbenchReads.length >= 8) { workbenchReadsTruncated = true; return; }
    const fact = { status: response.status(), code: null as string | null, response_request_id: null as string | null };
    workbenchReads.push(fact);
    try {
      const body = await response.json();
      const code = body?.error?.code;
      if (typeof code === "string" && /^[A-Z][A-Z0-9_]{0,79}$/.test(code)) fact.code = code;
      const requestId = body?.meta?.request_id;
      if (typeof requestId === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(requestId)) fact.response_request_id = requestId.toLowerCase();
    } catch { fact.code = "READBACK_BODY_UNAVAILABLE"; }
  });
  const manualRequestId = process.env.NEXUS_CONTROLLED_MANUAL_REQUEST_ID;
  await page.goto("/workspace");
  await expect(page.getByRole("heading", { name: "交易工作台" })).toBeVisible();
  if (manualRequestId) {
    await page.evaluate((key) => sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key, mode: "research_and_paper" })), process.env.NEXUS_CONTROLLED_MANUAL_KEY!);
    await page.reload();
    const manual = page.getByRole("region", { name: "开始研究与内部模拟" });
    await expect(manual.getByText("周期已完成", { exact: false })).toBeVisible();
    const receipt = await request.get("/api/v1/paper/manual-requests", { headers: { "Idempotency-Key": process.env.NEXUS_CONTROLLED_MANUAL_KEY! } });
    expect(receipt.status()).toBe(200);
    const data = (await receipt.json()).data;
    expect(data.request_id).toBe(manualRequestId);
    expect(data.cycle_id).toBe(cycleId);
    expect(data.status).toBe("completed");
    await manual.getByRole("button", { name: "开始新的请求", exact: true }).click();
    expect(await page.evaluate(() => sessionStorage.getItem("nexus-manual-paper-attempt-v1"))).toBeNull();
    await expect(manual.getByRole("button", { name: "研究并内部模拟", exact: true })).toBeEnabled();
  }
  try {
    await expect(page.locator(".workbench-candidates li")).toHaveCount(candidateCount);
  } catch (error) {
    if (error instanceof Error) error.message += `\nCONTROLLED_WORKBENCH_READBACK ${JSON.stringify({ reads: workbenchReads, truncated: workbenchReadsTruncated })}`;
    throw error;
  }
  await expect(page.getByRole("region", { name: "最新决策" }).getByText(firstPartial ? "部分成交" : "本周期订单已终态")).toBeVisible();
  await expect(page.getByRole("region", { name: "活动订单" }).getByText(firstPartial ? "partially_filled" : "本周期订单已终态")).toBeVisible();
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

  if (firstPartial) await expect(page.locator("article.paper-order").filter({ hasText: "部分成交" })).toHaveCount(1);

  await page.goto("/portfolio");
  await expect(page.getByRole("heading", { name: "组合事实" })).toBeVisible();
  const management = page.getByRole("region", { name: "内部模拟目标与计划摘要" });
  await expect(management.getByRole("heading", { name: "内部模拟目标与计划" })).toBeVisible();
  await expect(management).toContainText(`目标 V${summary.current_target?.version_seq} · 生效`);
  if (firstPartial) {
    await expect(management).toContainText("当前计划：资源仍占用");
    await expect(management.getByRole("link", { name: "查看调整计划" })).toHaveAttribute("href", `/paper?plan=${summary.occupying_plan?.batch_id}`);
  } else {
    await expect(management.getByRole("link", { name: "查看目标/实际详情" })).toHaveAttribute("href", "/paper");
  }
  await capture("portfolio");
  expect(writes).toEqual([]);
});


test("partial assessment closes without execution through normal BFF and pages", async ({ page, request }) => {
  test.skip(process.env.NEXUS_CONTROLLED_DEFERRED_CLOSEOUT !== "1", "isolated deferred assessment required");
  const id = process.env.NEXUS_CONTROLLED_CYCLE_ID!;
  const endpoint = `/api/v1/paper/management/cycles/${id}`;
  const response = await request.get(endpoint);
  expect(response.status()).toBe(200);
  const original = (await response.json()).data as Detail;
  expect(original.cycle.status).toBe("completed");
  expect(original.cycle.result_code).toBe("PORTFOLIO_ASSESSMENT_DEFERRED");
  expect(original.cycle.execution_locked).toBe(true);
  expect(original.blocker).toBeNull();
  expect(original.proposal).toBeNull();
  expect(original.activation_result).toBeNull();
  expect(original.orders).toEqual([]);
  expect(original.security_research).toHaveLength(3);
  expect(original.security_research.filter((item) => item.quality_status === "partial")).toHaveLength(
    process.env.NEXUS_CONTROLLED_HISTORICAL_ARCHIVE === "1" ? 2 : 3);
  if (process.env.NEXUS_CONTROLLED_HISTORICAL_ARCHIVE === "1") {
    expect(original.historical_source?.execution_allowed).toBe(false);
    expect(Date.parse(original.historical_source!.imported_at)).toBeGreaterThan(
      Date.parse(original.historical_source!.observed_finished_at));
  }
  expect(original.synthesis?.choices.every((choice) => choice.action === "defer")).toBe(true);
  const writes: string[] = [];
  page.on("request", (r) => { if (!["GET", "HEAD"].includes(r.method())) writes.push(r.method()); });
  await page.goto("/workspace");
  await expect(page.getByRole("heading", { name: "交易工作台" })).toBeVisible();
  if (process.env.NEXUS_CONTROLLED_HISTORICAL_ARCHIVE === "1") {
    const current = await request.get("/api/v1/workbench");
    expect(current.status()).toBe(200);
    const currentData = (await current.json()).data;
    const cycles = await request.get("/api/v1/paper/cycles?limit=20");
    expect(cycles.status()).toBe(200);
    const currentCycles = (await cycles.json()).data;
    expect(currentData.latest_cycle).toEqual(currentCycles.items[0] ?? null);
    // Real clock stays intact: a historical import is not today's scheduled cycle.
    if (currentData.latest_cycle === null) {
      await expect(page.getByRole("region", { name: "最新决策" })).toContainText("尚无已保存");
      expect(currentData.session.decision_session).not.toBe(original.cycle.decision_session);
    } else {
      const latest = currentData.latest_cycle;
      expect(latest.decision_session).toBe(currentData.session.decision_session);
      const latestRegion = page.getByRole("region", { name: "最新决策" });
      await expect(latestRegion.getByRole("link", { name: "查看完整决策" }))
        .toHaveAttribute("href", `/decisions/${latest.cycle_id}`);
      await expect(latestRegion.locator("time")).toHaveAttribute("datetime", latest.decision_time);
      if (latest.cycle_id === id) {
        await expect(latestRegion).toContainText("暂缓");
      } else {
        expect(Date.parse(latest.decision_time)).toBeGreaterThan(Date.parse(original.cycle.decision_time));
        await expect(latestRegion.locator(`a[href="/decisions/${id}"]`)).toHaveCount(0);
      }
    }
  } else {
    await expect(page.getByRole("region", { name: "最新决策" })).toContainText("暂缓");
  }
  await page.goto("/decisions");
  await expect(page.locator(`a[href="/decisions/${id}"]`)).toBeVisible();
  await page.goto(`/decisions/${id}`);
  await expect(page.getByRole("heading", { name: "组合决策详情" })).toBeVisible();
  if (process.env.NEXUS_CONTROLLED_HISTORICAL_ARCHIVE === "1") {
    await expect(page.getByRole("note", { name: "历史归档来源" })).toContainText("不可执行");
  }
  await expect(page.locator(".pm-agent strong").filter({ hasText: "暂缓" })).toHaveCount(3);
  await expect(page.getByRole("note", { name: "目标生效结论" }).getByText("已保留研究缺口与组合暂缓结果，本轮非执行收尾。", { exact: true })).toBeVisible();
  await page.reload();
  await expect(page.getByRole("note", { name: "目标生效结论" }).getByText("已保留研究缺口与组合暂缓结果，本轮非执行收尾。", { exact: true })).toBeVisible();
  await page.goto("/paper");
  await expect(page.getByRole("heading", { name: "内部模拟盘" })).toBeVisible();
  await page.goto("/portfolio");
  await expect(page.getByRole("heading", { name: "组合事实" })).toBeVisible();
  expect(writes).toEqual([]);
  const after = await request.get(endpoint);
  expect(after.status()).toBe(200);
  expect((await after.json()).data).toEqual(original);
});
