import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useParams: () => ({ proposalGroupId: "0198d500-1000-7000-8000-000000001303" }),
}));

import { OverviewPanel } from "../../../app/(protected)/overview/page";
import OptimizePage, { OptimizePanel } from "../../../app/(protected)/optimize/page";
import ProposalGroupPage, { ProposalGroupPanel } from "../../../app/(protected)/optimize/[proposalGroupId]/page";
import { WhatIfPagePanel } from "../../../app/(protected)/what-if/page";
import type { DataStateKind } from "../../../app/components/data-state";
import { eligibleProposalGroup, groupId, overviewData, proposalGroup, proposalIds, shadowActive, shadowStale } from "./fixtures";
import { portfolio, snapshot } from "../portfolio/fixtures";

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"], ["empty", "暂无数据"], ["ready", "数据已就绪"],
  ["partial", "数据不完整"], ["stale", "显示上次数据"], ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe("optimization pages", () => {
  it.each(states)("renders overview %s as an explicit visible state", (state) => {
    render(<OverviewPanel state={state} overview={["ready", "partial", "stale"].includes(state) ? overviewData() : null} onRefresh={vi.fn()} />);

    const readingLabels = { loading: "正在读取概览", ready: "概览已载入", partial: "概览证据不完整", stale: "读取失败 · 保留旧结果", empty: "暂无概览", error: "概览读取失败", forbidden: "无权读取概览" };
    expect(screen.getByText(readingLabels[state])).toBeVisible();
    expect(screen.getByRole("region", { name: "投资概览页面" })).toHaveAttribute("data-state", state);
  });

  it("answers whether optimization is needed without a score or trade action", () => {
    render(<OverviewPanel state="ready" overview={overviewData()} onRefresh={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "值得比较优化方案" })).toBeVisible();
    expect(screen.getByRole("link", { name: "生成一个调整候选" })).toHaveAttribute("href", "/portfolio");
    expect(screen.getByRole("link", { name: "查看历史三方案记录" })).toHaveAttribute("href", `/optimize/${groupId}`);
    expect(document.body).not.toHaveTextContent(/\d+\s*\/\s*100/);
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
  });

  it("routes an empty overview to the read-only market research journey", () => {
    render(<OverviewPanel state="empty" overview={null} onRefresh={vi.fn()} />);

    expect(screen.getByRole("link", { name: "研究美股证据" })).toHaveAttribute("href", "/market");
    expect(screen.queryByRole("button", { name: /创建方案|下单|交易/ })).not.toBeInTheDocument();
  });

  it("shows persisted Fosun facts and keeps the current state when diagnostics are absent", () => {
    const factsOnlyOverview: NonNullable<Parameters<typeof OverviewPanel>[0]["overview"]> = {
      overview_mode: "PORTFOLIO_FACTS_ONLY",
      portfolio_id: portfolio.portfolio_id,
      portfolio_name: portfolio.name,
      provider: "fosun",
      portfolio_snapshot_id: snapshot.portfolio_snapshot_id,
      portfolio_snapshot: { ...snapshot, status: "PARTIAL" },
      snapshot_as_of: snapshot.as_of!,
      trading_day: null,
      diagnostic_report: null,
      diagnostic_unavailable_reason: "SNAPSHOT_NOT_READY",
      proposal_group: null,
      proposal_admission: null,
      proposal_admission_reason: null,
    };

    render(<OverviewPanel state="ready" overview={factsOnlyOverview} onRefresh={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "今日资产概览" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "尚无诊断，暂不采取动作" })).toBeVisible();
    expect(screen.getByText("快照尚未通过完整核验，暂不能形成正式诊断；未分析不等于没有风险。")).toBeVisible();
    expect(screen.getAllByText("复星（只读）").length).toBeGreaterThan(0);
    expect(screen.getAllByText("需要对账").length).toBeGreaterThan(0);
    const authority = screen.getByRole("region", { name: "数据与决策权威边界" });
    expect(authority).toHaveTextContent("可以查看");
    expect(authority).toHaveTextContent("账户快照核验通过");
    expect(screen.getByTitle(snapshot.display_readiness)).toBeVisible();
    expect(authority).toHaveTextContent("快照核验、研究结论与执行条件分别判断");
    expect(authority).toHaveTextContent("当前不可执行");
    expect(screen.getByRole("region", { name: "HKD 原币种资产估值" })).toHaveTextContent("HKD 27,500.00");
    expect(screen.getByRole("region", { name: "USD 原币种资产估值" })).toHaveTextContent("USD 1,000.00");
    expect(screen.getByRole("link", { name: "查看组合事实" })).toHaveAttribute("href", "/portfolio");
    expect(screen.getByRole("link", { name: "查看持仓行情" })).toHaveAttribute("href", "/market");
    expect(screen.queryByRole("link", { name: /打开组合诊断|查看三方案比较/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /创建方案|下单|交易/ })).not.toBeInTheDocument();
  });

  it("surfaces at most three existing findings without changing report order or authority", () => {
    const overview = overviewData();
    const report = overview.diagnostic_report!;
    const originalOrder = report.items.map((item) => item.diagnostic_code);
    render(<OverviewPanel state="ready" overview={overview} onRefresh={vi.fn()} />);
    const highlights = screen.getByRole("region", { name: "本次诊断重点" });
    const items = within(highlights).getAllByRole("listitem");
    expect(items).toHaveLength(3);
    expect(items[0]).toHaveTextContent("SOURCE_AUTHORITY");
    expect(items[0]).toHaveTextContent("数据来源可靠性");
    expect(items[0]).toHaveTextContent("SOURCE_CONFLICT");
    expect(items[1]).toHaveTextContent("CONCENTRATION_TOP1");
    expect(report.items.map((item) => item.diagnostic_code)).toEqual(originalOrder);
    expect(screen.getByText("概览已载入")).toBeVisible();
    expect(screen.getByRole("link", { name: "查看资产组合" })).toHaveAttribute("href", "/portfolio");
  });

  it("does not expose legacy three-way creation even when admission authority exists", () => {
    render(<OptimizePanel state="ready" overview={overviewData({ proposal_group: null })} onRefresh={vi.fn()} />);

    expect(screen.getByRole("region", { name: "一个确定性候选" })).toHaveTextContent("Agent 负责解释，不生成权重");
    expect(screen.getByRole("link", { name: "生成一个调整候选" })).toHaveAttribute("href", "/portfolio");
    expect(screen.queryByRole("button", { name: "创建三方案比较" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /下单|交易/ })).not.toBeInTheDocument();
  });

  it("routes new decisions to one read-only candidate while preserving legacy groups as history", () => {
    render(<OptimizePanel state="ready" overview={overviewData()} onRefresh={vi.fn()} />);

    expect(screen.getByRole("link", { name: "生成一个调整候选" })).toHaveAttribute("href", "/portfolio");
    expect(screen.getByRole("link", { name: "查看历史三方案记录" })).toHaveAttribute("href", `/optimize/${groupId}`);
    expect(screen.queryByRole("button", { name: "创建三方案比较" })).not.toBeInTheDocument();
  });

  it.each(["NO_ACTION", "RESEARCH_REQUIRED", "BLOCKED"] as const)(
    "%s 无方案组且尚无可确定减仓条目时，不推断保持现状",
    (optimizationNeed) => {
      const overview = overviewData({ proposal_group: null });
      overview.diagnostic_report!.optimization_need = optimizationNeed;
      overview.diagnostic_report!.items = overview.diagnostic_report!.items.map((item) => ({
        ...item,
        actionability: item.actionability === "reduce_only" ? "research" : item.actionability,
      }));
      render(<OptimizePanel state="ready" overview={overview} onRefresh={vi.fn()} />);

      const region = screen.getByRole("region", { name: "历史方案状态" });
      expect(region).toHaveTextContent("当前没有历史三方案记录");
      expect(region).toHaveTextContent("当前诊断未提供可直接用于确定性减仓的条目");
      expect(region).toHaveTextContent("旧三方案只作为历史记录保留");
      expect(region).not.toHaveTextContent(/保持现状更优|建议保持|无需调整|DRAFT_NO_EVIDENCED_REDUCTION/);
    },
  );

  it("仅把 reduce_only 诊断显示为候选条件，不伪造完整方案", () => {
    const overview = overviewData({
      proposal_group: null,
      proposal_admission: null,
      proposal_admission_reason: "OPTIMIZATION_AUTHORITY_MISSING",
    });
    render(<OptimizePanel state="ready" overview={overview} onRefresh={vi.fn()} />);

    const availability = screen.getByRole("region", { name: "历史方案状态" });
    expect(availability).toHaveTextContent("诊断存在降低仓位条件");
    expect(availability).toHaveTextContent("成本未知仍会阻止执行");
    expect(screen.getByRole("region", { name: "一个确定性候选" })).toHaveTextContent("前往组合页");
    expect(screen.queryByRole("table", { name: /三方案/ })).not.toBeInTheDocument();
    expect(screen.queryByText(/比较结果：|推荐/)).not.toBeInTheDocument();
  });

  it("已有方案组时只保留原链接，不显示 no-group 状态", () => {
    render(<OptimizePanel state="ready" overview={overviewData()} onRefresh={vi.fn()} />);

    expect(screen.getByRole("link", { name: "查看历史三方案记录" })).toHaveAttribute("href", `/optimize/${groupId}`);
    expect(screen.queryByRole("region", { name: "历史方案状态" })).not.toBeInTheDocument();
    expect(screen.queryByText("当前没有历史三方案记录")).not.toBeInTheDocument();
  });

  it.each(["OPTIMIZATION_AUTHORITY_MISSING", null] as const)("keeps legacy admission %s out of the new decision path", (reason) => {
    render(<OptimizePanel state="ready" overview={overviewData({ proposal_group: null,
      proposal_admission: null, proposal_admission_reason: reason,
    })} onRefresh={vi.fn()} />);
    const region = screen.getByRole("region", { name: "一个确定性候选" });
    expect(region).toHaveTextContent("前往组合页");
    expect(region).not.toHaveTextContent("三方案准入");
    expect(screen.queryByText("OPTIMIZATION_AUTHORITY_MISSING")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "创建三方案比较" })).not.toBeInTheDocument();
  });

  it("does not equate missing diagnosis with a recommendation to hold", () => {
    render(<OptimizePanel state="empty" overview={null} onRefresh={vi.fn()} />);
    expect(screen.getByText("诊断数据缺失或不可用，暂不能判断是否需要调整；请核查数据后刷新。")).toBeVisible();
    expect(screen.queryByText("数据缺失或过期时保持现状。")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "创建三方案比较" })).not.toBeInTheDocument();
  });

  it("loads diagnostics without posting a legacy proposal task", async () => {
    const writes: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (init?.method && !["GET", "HEAD"].includes(init.method)) writes.push(String(input));
      if (String(input).startsWith("/api/v1/overview")) return Response.json({ data: overviewData({ proposal_group: null }), meta: { request_id: "0198d500-1000-7000-8000-000000001399", schema_version: "1", as_of: "2026-08-24T00:00:00Z" } });
      throw new Error(`unexpected request ${String(input)}`);
    }));

    render(<OptimizePage />);
    expect(await screen.findByRole("link", { name: "生成一个调整候选" })).toHaveAttribute("href", "/portfolio");
    expect(screen.queryByRole("button", { name: "创建三方案比较" })).not.toBeInTheDocument();
    expect(writes).toEqual([]);
  });

  it("keeps group lifecycle limited to shadow validation and rejection", () => {
    render(<ProposalGroupPanel groupState="ready" shadowState="not-started" group={proposalGroup()} shadow={null} shadowRequestId={null} onRefresh={vi.fn()} onStartShadow={vi.fn()} onReject={vi.fn()} />);

    expect(screen.getAllByRole("columnheader", { name: /保持现状|最小调整|策略目标/ })).toHaveLength(3);
    expect(screen.getAllByText("同一基线 · 三种方案").length).toBeGreaterThan(0);
    expect(screen.getByText("组合诊断").closest("li")).toHaveAttribute("aria-current", "step");
    expect(screen.getByRole("link", { name: "试算最小调整权重" })).toHaveAttribute("href", expect.stringContaining(proposalIds.minimum_change));
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
  });

  it.each(["partial", "stale"] as const)("keeps cached %s proposal facts visible but disables every action", (state) => {
    render(<ProposalGroupPanel groupState={state} shadowState="not-started" group={proposalGroup()} shadow={null} shadowRequestId={null} onRefresh={vi.fn()} onStartShadow={vi.fn()} onReject={vi.fn()} />);

    expect(screen.getAllByRole("columnheader", { name: /保持现状|最小调整|策略目标/ })).toHaveLength(3);
    expect(screen.getByText(/当前数据状态不允许发起操作/)).toBeVisible();
    expect(screen.queryByRole("button", { name: /启动影子验证|拒绝方案组/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "试算最小调整权重" })).not.toBeInTheDocument();
  });

  it("keeps latest-version rejection available when only Shadow data is stale", async () => {
    const onReject = vi.fn().mockResolvedValue(undefined);
    const group = proposalGroup({ status: "shadowing", version: 5 });
    render(<ProposalGroupPanel groupState="ready" shadowState="stale" group={group} shadow={shadowStale()} shadowRequestId={null} onRefresh={vi.fn()} onStartShadow={vi.fn()} onReject={onReject} />);

    expect(screen.getByText(/保留最近一次已验证曲线/)).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "拒绝方案组" }));
    await waitFor(() => expect(onReject).toHaveBeenCalledWith(group.proposal_group_id, 5));
    expect(screen.queryByRole("button", { name: "启动影子验证" })).not.toBeInTheDocument();
  });

  it("fetches group and Shadow in parallel and renders independently verified data", async () => {
    setProposalLocation();
    let resolveGroup!: (value: Response) => void;
    let resolveShadow!: (value: Response) => void;
    const fetchMock = vi.fn((input: RequestInfo | URL) => new Promise<Response>((resolve) => {
      if (String(input).endsWith("/shadow")) resolveShadow = resolve;
      else resolveGroup = resolve;
    }));
    vi.stubGlobal("fetch", fetchMock);

    render(<ProposalGroupPage />);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(fetchMock.mock.calls.map(([input]) => String(input))).toEqual([
      `/api/v1/optimization-proposals/${groupId}`,
      `/api/v1/optimization-proposals/${groupId}/shadow`,
    ]);
    resolveGroup(successEnvelope(proposalGroup({ status: "shadowing", version: 5 })));
    resolveShadow(successEnvelope(shadowActive()));

    expect(await screen.findByRole("heading", { name: "前向影子绩效" })).toBeVisible();
    expect(screen.getAllByRole("columnheader", { name: /保持现状|最小调整|策略目标/ }).length).toBeGreaterThanOrEqual(3);
  });

  it("maps Shadow 404 to not-started without treating the proposal group as empty", async () => {
    setProposalLocation();
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) =>
      String(input).endsWith("/shadow")
        ? errorEnvelope(404, "NOT_FOUND", "0198d500-1000-7000-8000-000000001399")
        : successEnvelope(eligibleProposalGroup())));

    render(<ProposalGroupPage />);
    expect(await screen.findByText(/尚未启动影子验证/)).toBeVisible();
    expect(screen.getByRole("button", { name: "启动影子验证" })).toBeVisible();
  });

  it("retains the last verified Shadow curve when refresh fails", async () => {
    setProposalLocation();
    let shadowCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      if (!String(input).endsWith("/shadow")) return successEnvelope(proposalGroup({ status: "shadowing", version: 5 }));
      shadowCalls += 1;
      if (shadowCalls === 1) return successEnvelope(shadowActive());
      throw new Error("network unavailable");
    }));

    render(<ProposalGroupPage />);
    expect(await screen.findByRole("heading", { name: "前向影子绩效" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "重新加载方案" }));
    expect(await screen.findByText(/保留最近一次已验证曲线/)).toBeVisible();
    expect(screen.getByRole("table", { name: "三方案逐交易日净值" })).toBeVisible();
  });

  it("consumes ShadowStartEnvelope then refetches the read projection", async () => {
    setProposalLocation();
    const group = eligibleProposalGroup();
    let shadowGets = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (!url.endsWith("/shadow")) return successEnvelope(group);
      if (init?.method === "POST") return successEnvelope({
        proposal_group_id: groupId,
        shadow_group_id: shadowActive().shadow_group_id,
        status: "shadowing",
        version: 5,
        start_session: shadowActive().start_session,
        evaluation_end_session: shadowActive().evaluation_end_session,
        grace_end_session: shadowActive().grace_end_session,
      });
      shadowGets += 1;
      return shadowGets === 1
        ? errorEnvelope(404, "NOT_FOUND", "0198d500-1000-7000-8000-000000001399")
        : successEnvelope(shadowActive());
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<ProposalGroupPage />);
    fireEvent.click(await screen.findByRole("button", { name: "启动影子验证" }));
    fireEvent.click(screen.getByRole("button", { name: "确认启动影子验证" }));

    expect(await screen.findByRole("heading", { name: "前向影子绩效" })).toBeVisible();
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(post?.[0]).toBe(`/api/v1/optimization-proposals/${groupId}/shadow`);
    expect(shadowGets).toBe(2);
  });

  it("keeps rejection on its distinct lifecycle envelope and endpoint", async () => {
    setProposalLocation();
    const group = proposalGroup({ status: "shadowing", version: 5 });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (init?.method === "POST") {
        return successEnvelope({ proposal_group_id: groupId, status: "rejected", version: 6 });
      }
      return url.endsWith("/shadow") ? successEnvelope(shadowActive()) : successEnvelope(group);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<ProposalGroupPage />);
    fireEvent.click(await screen.findByRole("button", { name: "拒绝方案组" }));

    expect(await screen.findByText(/方案组已拒绝/)).toBeVisible();
    expect(screen.getByText("已拒绝")).toBeVisible();
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(post?.[0]).toBe(`/api/v1/optimization-proposals/${groupId}/reject`);
    expect(new Headers(post?.[1]?.headers).get("if-match")).toBe('"5"');
  });

  it("shows the safe Shadow request id on failure and gives group failure precedence", async () => {
    setProposalLocation();
    const requestId = "0198d500-1000-7000-8000-000000001399";
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) =>
      String(input).endsWith("/shadow")
        ? errorEnvelope(500, "INTERNAL_ERROR", requestId)
        : successEnvelope(proposalGroup())));
    const { unmount } = render(<ProposalGroupPage />);
    expect(await screen.findByText(new RegExp(requestId))).toBeVisible();
    unmount();

    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) =>
      String(input).endsWith("/shadow")
        ? successEnvelope(shadowActive())
        : errorEnvelope(404, "NOT_FOUND", requestId)));
    render(<ProposalGroupPage />);
    expect(await screen.findByText("方案组不存在")).toBeVisible();
    expect(screen.queryByRole("heading", { name: "前向影子绩效" })).not.toBeInTheDocument();
  });

  it("requires a persisted proposal context before rendering what-if inputs", () => {
    const { rerender } = render(<WhatIfPagePanel state="empty" group={null} proposalId={null} onRefresh={vi.fn()} onEvaluate={vi.fn()} />);
    expect(screen.getByText(/请从已有方案进入试算/)).toBeVisible();

    rerender(<WhatIfPagePanel state="ready" group={proposalGroup()} proposalId={proposalIds.minimum_change} onRefresh={vi.fn()} onEvaluate={vi.fn()} />);
    expect(screen.getAllByRole("textbox", { name: /目标权重/ })).toHaveLength(2);
    expect(screen.getAllByText(/仅分析，不保存/).length).toBeGreaterThan(0);
  });

  it.each(["partial", "stale"] as const)("shows cached %s what-if context without enabling evaluation", (state) => {
    render(<WhatIfPagePanel state={state} group={proposalGroup()} proposalId={proposalIds.minimum_change} onRefresh={vi.fn()} onEvaluate={vi.fn()} />);

    expect(screen.getByText(/当前数据状态不允许试算/)).toBeVisible();
    for (const input of screen.getAllByRole("textbox", { name: /目标权重/ })) expect(input).toBeDisabled();
    expect(screen.getByRole("button", { name: "评估情景" })).toBeDisabled();
  });
});

function successEnvelope(data: unknown): Response {
  return Response.json({
    data,
    meta: { request_id: "0198d500-1000-7000-8000-000000001399", schema_version: "1", as_of: "2026-08-24T00:00:00Z" },
  });
}

function errorEnvelope(status: number, code: string, requestId: string): Response {
  return Response.json({ error: { code, message: "safe", request_id: requestId, details: {} } }, { status });
}

function setProposalLocation(): void {
  globalThis.history.replaceState({}, "", `/optimize/${groupId}`);
}
