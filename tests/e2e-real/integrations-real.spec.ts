import { expect, test, type Request, type Response } from "@playwright/test";

const integrationsPath = "/api/v1/integrations";
const fosunTestPath = "/api/v1/integrations/fosun/test";

type IntegrationStatus = {
  provider: "fosun" | "alpaca" | "sec" | "fred" | "openai";
  mode: "disabled" | "fixture" | "read_only";
  quality: "PASS" | "PARTIAL" | "FAIL";
  as_of: string | null;
  capability_expires_at: string | null;
  contract_hash: string | null;
  next_action: string | null;
  version: number;
};

type IntegrationListEnvelope = { data: IntegrationStatus[]; meta: { as_of: string } };
type IntegrationMutationEnvelope = { data: IntegrationStatus; meta: { as_of: string } };

test("proves the real BFF fixture CAS replay and persisted integration state", async ({ page }) => {
  const webOrigin = new URL(test.info().project.use.baseURL as string).origin;
  const observedRequests: Request[] = [];
  page.on("request", (request) => observedRequests.push(request));

  const initialGet = page.waitForResponse((response) => isIntegrationGet(response, webOrigin));
  await page.goto("/integrations");
  const initial = await json<IntegrationListEnvelope>(await initialGet);

  expect(initial.data).toEqual([
    fixtureFosun(0),
    disabledProvider("alpaca"),
    disabledProvider("sec"),
    disabledProvider("fred"),
    disabledProvider("openai"),
  ]);
  await expect(page.getByRole("heading", { name: "数据与能力状态" })).toBeVisible();
  const fosunCard = page.getByLabel("fosun 集成（只读）");
  await expect(fosunCard).toBeVisible();
  await expect(fosunCard.getByText("0", { exact: true })).toBeVisible();

  const mutationResponse = page.waitForResponse((response) => isFosunTestPost(response, webOrigin));
  await page.getByRole("button", { name: "测试非交易连接" }).click();
  const firstMutation = await mutationResponse;
  const firstPayload = await json<IntegrationMutationEnvelope>(firstMutation);
  const firstRequest = firstMutation.request();
  const firstHeaders = await firstRequest.allHeaders();

  expect(new URL(firstRequest.url()).origin).toBe(webOrigin);
  expect(firstRequest.method()).toBe("POST");
  expect(firstHeaders.origin).toBe(webOrigin);
  expect(firstHeaders["if-match"]).toBe('"0"');
  expect(firstHeaders["idempotency-key"]).toMatch(/^.+$/);
  expect(firstRequest.postData()).toBe("{}");
  expect(firstPayload.data).toMatchObject({ provider: "fosun", mode: "fixture", quality: "PASS", version: 1 });
  expect(Date.parse(firstPayload.data.as_of ?? "")).toBeGreaterThan(Date.parse(initial.data[0]!.as_of ?? ""));
  await expect(page.getByText("非交易连接测试已完成。")).toBeVisible();
  await expect(fosunCard.getByText("1", { exact: true })).toBeVisible();

  const replayResponse = page.waitForResponse((response) => isFosunTestPost(response, webOrigin));
  const replay = await page.evaluate(async ({ idempotencyKey }) => {
    const response = await fetch("/api/v1/integrations/fosun/test", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": idempotencyKey,
        "if-match": '"0"',
      },
      body: "{}",
    });
    return { status: response.status, payload: await response.json() };
  }, { idempotencyKey: firstHeaders["idempotency-key"]! });
  const secondRequest = (await replayResponse).request();

  expect(replay.status).toBe(200);
  expect(replay.payload).toMatchObject({ data: { provider: "fosun", mode: "fixture", quality: "PASS", version: 1 } });
  expect(await businessHeaders(secondRequest)).toEqual(await businessHeaders(firstRequest));
  expect(secondRequest.postData()).toBe(firstRequest.postData());

  const persistedGet = page.waitForResponse((response) => isIntegrationGet(response, webOrigin));
  await page.reload();
  const persisted = await json<IntegrationListEnvelope>(await persistedGet);
  const persistedFosun = persisted.data.find((status) => status.provider === "fosun");

  expect(persistedFosun).toMatchObject({ provider: "fosun", mode: "fixture", quality: "PASS", version: 1 });
  expect(Date.parse(persistedFosun?.as_of ?? "")).toBeGreaterThan(Date.parse(initial.data[0]!.as_of ?? ""));
  await expect(page.getByText("非交易连接测试已完成。")).toHaveCount(0);
  await expect(page.getByLabel("fosun 集成（只读）").getByText("1", { exact: true })).toBeVisible();

  for (const version of [10, 11]) {
    await test.step(`v${version} persisted observation survives real GET and refresh`, async () => {
      const instrumentId = "0198d500-1000-7000-8000-000000009001";
      const runId = `0198d500-1000-7000-8000-0000000090${version}`;
      const path = `/api/v1/research-runs/${runId}`;
      const read = () => page.waitForResponse((response) => {
        const url = new URL(response.url());
        return url.origin === webOrigin && url.pathname === path && response.request().method() === "GET";
      });
      const initialRead = read();
      await page.goto(`/research/${instrumentId}?run_id=${runId}`);
      const saved = await json<{ data: Record<string, unknown> }>(await initialRead);
      expect(saved.data).toMatchObject({
        run_id: runId, status: "completed", quality_status: "partial", abstention_reason: null,
        intent: {
          stance: "watch", exposure_action: "none", quality_status: "partial",
          synthesis_assessment: { review_conclusion: "limited" },
        },
      });
      const region = page.getByRole("region", { name: "个股研究证据页" });
      await expect(region).toHaveAttribute("data-state", "partial");
      await region.getByText("逐项审查（模型自评）", { exact: true }).click();
      for (const text of [
        "合成财报只支持历史收入观察。", "合成反方异议：缺少同口径盈利预测。",
        "合成缺口：同口径盈利预测缺失。", "合成受阻结论：无法判断是否增加敞口。",
        "合成不确定性：盈利持续性尚未证实。", "模型自评的审查充分性：有限",
      ]) await expect(region.getByText(text, { exact: true }).first()).toBeVisible();
      await expect(page.getByRole("button", { name: /运行冻结研究|重跑|下单|买入|卖出/ })).toHaveCount(0);
      const refreshedRead = read();
      await page.getByRole("button", { name: "重新读取", exact: true }).click();
      const refreshed = await json<{ data: Record<string, unknown> }>(await refreshedRead);
      expect(refreshed.data).toEqual(saved.data);
      await expect(region).toHaveAttribute("data-state", "partial");
      await expect(region.getByText("合成缺口：同口径盈利预测缺失。", { exact: true }).first()).toBeVisible();
      await expect(page.getByRole("button", { name: /运行冻结研究|重跑/ })).toHaveCount(0);
    });
  }
  expect(observedRequests.filter((request) => (
    new URL(request.url()).pathname.includes("research") && request.method() !== "GET"
  ))).toEqual([]);

  expect(observedRequests.map((request) => new URL(request.url()).origin)).toEqual(
    expect.arrayContaining([webOrigin]),
  );
  expect(observedRequests.every((request) => new URL(request.url()).origin === webOrigin)).toBe(true);
});

function fixtureFosun(version: number): IntegrationStatus {
  return {
    provider: "fosun",
    mode: "fixture",
    quality: "PASS",
    as_of: "2026-08-22T00:00:00Z",
    capability_expires_at: null,
    contract_hash: null,
    next_action: "Read-only admission is required.",
    version,
  };
}

function disabledProvider(provider: Exclude<IntegrationStatus["provider"], "fosun">): IntegrationStatus {
  return {
    provider,
    mode: "disabled",
    quality: "PARTIAL",
    as_of: null,
    capability_expires_at: null,
    contract_hash: null,
    next_action: "Provider is disabled.",
    version: 0,
  };
}

function isIntegrationGet(response: Response, webOrigin: string): boolean {
  const url = new URL(response.url());
  return url.origin === webOrigin && url.pathname === integrationsPath && response.request().method() === "GET";
}

function isFosunTestPost(response: Response, webOrigin: string): boolean {
  const url = new URL(response.url());
  return url.origin === webOrigin && url.pathname === fosunTestPath && response.request().method() === "POST";
}

async function businessHeaders(request: Request): Promise<Record<string, string | undefined>> {
  const headers = await request.allHeaders();
  return {
    "content-type": headers["content-type"],
    "idempotency-key": headers["idempotency-key"],
    "if-match": headers["if-match"],
    origin: headers.origin,
  };
}

async function json<T>(response: Response): Promise<T> {
  expect(response.status()).toBe(200);
  expect(response.headers()["content-type"]).toContain("application/json");
  return response.json() as Promise<T>;
}
