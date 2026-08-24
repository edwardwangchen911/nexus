import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { OverviewPanel } from "../../../app/(protected)/overview/page";
import OptimizePage, { OptimizePanel } from "../../../app/(protected)/optimize/page";
import { ProposalGroupPanel } from "../../../app/(protected)/optimize/[proposalGroupId]/page";
import { WhatIfPagePanel } from "../../../app/(protected)/what-if/page";
import type { DataStateKind } from "../../../app/components/data-state";
import { groupId, overviewData, proposalGroup, proposalIds } from "./fixtures";

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"], ["empty", "暂无数据"], ["ready", "数据已就绪"],
  ["partial", "数据不完整"], ["stale", "数据已过期"], ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe("optimization pages", () => {
  it.each(states)("renders overview %s as an explicit visible state", (state, label) => {
    render(<OverviewPanel state={state} overview={["ready", "partial", "stale"].includes(state) ? overviewData() : null} onRefresh={vi.fn()} />);

    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "投资概览页面" })).toHaveAttribute("data-state", state);
  });

  it("answers whether optimization is needed without a score or trade action", () => {
    render(<OverviewPanel state="ready" overview={overviewData()} onRefresh={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "值得比较优化方案" })).toBeVisible();
    expect(screen.getByRole("link", { name: "打开组合诊断" })).toHaveAttribute("href", expect.stringContaining("/optimize"));
    expect(screen.getByRole("link", { name: "查看三方案比较" })).toHaveAttribute("href", `/optimize/${groupId}`);
    expect(document.body).not.toHaveTextContent(/\d+\s*\/\s*100/);
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
  });

  it("creates a comparison only from server-supplied admission authority", async () => {
    const onCreate = vi.fn().mockResolvedValue(undefined);
    render(<OptimizePanel state="ready" overview={overviewData({ proposal_group: null })} creating={false} notice={null} onCreate={onCreate} onRefresh={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "创建三方案比较" }));
    await waitFor(() => expect(onCreate).toHaveBeenCalledWith(overviewData().proposal_admission));
    expect(screen.queryByRole("button", { name: /下单|交易/ })).not.toBeInTheDocument();
  });

  it("reuses the proposal idempotency key while an admission result is unknown", async () => {
    const keys: Array<string | null> = [];
    let proposalCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input).startsWith("/api/v1/overview")) return Response.json({ data: overviewData({ proposal_group: null }), meta: { request_id: "0198d500-1000-7000-8000-000000001399", schema_version: "1", as_of: "2026-08-24T00:00:00Z" } });
      if (String(input) === "/api/v1/optimization-proposals") {
        proposalCalls += 1; keys.push(new Headers(init?.headers).get("idempotency-key"));
        if (proposalCalls === 1) return Response.json({ error: { code: "INTERNAL_ERROR", message: "safe", request_id: "0198d500-1000-7000-8000-000000001399", details: {} } }, { status: 500 });
        return Response.json({ data: { job_id: "0198d500-1000-7000-8000-000000001398", status: "QUEUED" }, meta: { request_id: "0198d500-1000-7000-8000-000000001399", schema_version: "1", as_of: "2026-08-24T00:00:00Z" } }, { status: 202 });
      }
      throw new Error(`unexpected request ${String(input)}`);
    }));

    render(<OptimizePage />);
    fireEvent.click(await screen.findByRole("button", { name: "创建三方案比较" }));
    expect(await screen.findByText(/结果尚未确定/)).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "创建三方案比较" }));
    expect(await screen.findByText(/三方案任务已入队/)).toBeVisible();
    expect(proposalCalls).toBe(2);
    expect(keys[0]).toBeTruthy();
    expect(new Set(keys).size).toBe(1);
  });

  it("keeps group lifecycle limited to shadow validation and rejection", () => {
    render(<ProposalGroupPanel state="ready" group={proposalGroup()} onRefresh={vi.fn()} onStartShadow={vi.fn()} onReject={vi.fn()} />);

    expect(screen.getAllByRole("columnheader", { name: /保持现状|最小调整|策略目标/ })).toHaveLength(3);
    expect(screen.getByRole("link", { name: "试算最小调整权重" })).toHaveAttribute("href", expect.stringContaining(proposalIds.minimum_change));
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
  });

  it("requires a persisted proposal context before rendering what-if inputs", () => {
    const { rerender } = render(<WhatIfPagePanel state="empty" group={null} proposalId={null} onRefresh={vi.fn()} onEvaluate={vi.fn()} />);
    expect(screen.getByText(/必须从已持久化的方案进入/)).toBeVisible();

    rerender(<WhatIfPagePanel state="ready" group={proposalGroup()} proposalId={proposalIds.minimum_change} onRefresh={vi.fn()} onEvaluate={vi.fn()} />);
    expect(screen.getAllByRole("textbox", { name: /目标权重/ })).toHaveLength(2);
    expect(screen.getAllByText(/仅分析，不持久化/).length).toBeGreaterThan(0);
  });
});
