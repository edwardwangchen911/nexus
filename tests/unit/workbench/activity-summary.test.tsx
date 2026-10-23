import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ActivitySummary, type WorkbenchActivity, type ActivityScope } from "../../../app/components/workbench/activity-summary";

const requestId = "00000000-0000-4000-8000-000000000001";
const runId = "00000000-0000-4000-8000-000000000002";
const instrumentId = "00000000-0000-4000-8000-000000000003";
export const activityScope = (changes: Partial<ActivityScope> = {}): ActivityScope => ({
  mode: "research_and_paper", today_registered: 0, today_started: 0,
  registration_date: "2026-10-23", decision_session: "2026-10-23", selection: "historical_unresolved",
  request_id: requestId, cycle_id: null, requested_at: "2026-10-22T15:00:00Z", status: "unknown",
  reason_code: "MANUAL_PREPARATION_RESULT_UNKNOWN", trigger: "manual", historical: true,
  occupancy: "unresolved", portfolio_status: null, portfolio_reason_code: null, href: null, candidates: [], ...changes,
});
const activity = (scope = activityScope()): WorkbenchActivity => ({ read_at: "2026-10-23T08:00:00Z", read_status: "complete", scopes: [scope] });
const candidate = () => ({ instrument_id: instrumentId, symbol: "AAPL", run_id: runId, snapshot_id: null,
  research_as_of: "2026-10-22T14:00:00Z", run_status: "completed", job_status: "succeeded", quality: "partial", read_status: "verified" as const,
  reason_code: "RESEARCH_OUTPUT_NOT_ACTIONABLE", href: `/research/${instrumentId}?run_id=${runId}`,
  limitations: [
    { source: "system" as const, kind: "reason", text: "研究未通过交易准入", truncated: false, impact: null },
    { source: "model" as const, kind: "missing_evidence", text: "历史分母适用范围仍需核对", truncated: true, impact: "估值判断受限" },
    { source: "model" as const, kind: "uncertainties", text: "未来需求存在不确定性", truncated: false, impact: null },
  ], has_more: false,
});

describe("frozen round and research limits", () => {
  it("separates today's absence, historical unknown, occupancy and registration time", () => {
    render(<ActivitySummary activity={activity()} />);
    const scope = screen.getByRole("region", { name: "正式研究与内部模拟" });
    expect(scope).toHaveTextContent("今日手动未登记");
    expect(scope).toHaveTextContent("本交易日未启动");
    expect(scope).toHaveTextContent("历史未决");
    expect(scope).toHaveTextContent("结果未知");
    expect(scope).toHaveTextContent("仍有未解除占用");
    expect(scope).toHaveTextContent("2026-10-22");
    expect(scope).toHaveTextContent("详情暂无法定位");
    expect(within(scope).queryByRole("link")).not.toBeInTheDocument();
    expect(within(scope).queryByRole("button")).not.toBeInTheDocument();
    expect(scope).not.toHaveTextContent("今日失败");
  });
  it("keeps preparing without a cycle and independent research-only purpose visible", () => {
    render(<ActivitySummary activity={{ ...activity(), scopes: [
      activityScope({ today_registered: 1, status: "preparing", selection: "active", historical: false }),
      activityScope({ mode: "research_only", today_registered: 1, today_started: 1, status: "completed", selection: "today", historical: false, occupancy: "released", portfolio_status: "not_applicable" }),
    ] }} />);
    expect(screen.getByRole("region", { name: "正式研究与内部模拟" })).toHaveTextContent("正在准备；尚未创建周期");
    expect(screen.getByRole("region", { name: "纯研究" })).toHaveTextContent("组合决策与执行：本模式不适用");
    expect(screen.getByRole("region", { name: "纯研究" })).toHaveTextContent("历史占用已按原合同处置");
  });
  it("separates completed partial research, source labels, bounded excerpts and deferred result", () => {
    render(<ActivitySummary activity={activity(activityScope({ status: "completed", selection: "today", historical: false,
      portfolio_status: "deferred", portfolio_reason_code: "RESEARCH_OUTPUT_NOT_ACTIONABLE", candidates: [candidate()] }))} />);
    const card = screen.getByRole("article", { name: "AAPL 本轮研究" });
    expect(card).toHaveTextContent("研究已完成");
    expect(card).toHaveTextContent("结论受限");
    expect(screen.getByText("组合判断暂缓")).toBeVisible();
    expect(card).toHaveTextContent("系统核验");
    expect(card).toHaveTextContent("模型陈述");
    expect(card).toHaveTextContent("截断摘录");
    expect(card).toHaveTextContent("模型指出的受限结论：估值判断受限");
    expect(screen.getByText("未来需求存在不确定性")).not.toBeVisible();
    fireEvent.click(within(card).getByText("展开更多已保存限制"));
    expect(screen.getByText("未来需求存在不确定性")).toBeVisible();
    expect(card).toHaveTextContent("研究限制／未来不确定性");
    expect(card).not.toHaveTextContent("待补材料");
    expect(within(card).getByRole("link", { name: "查看 AAPL 原研究" })).toHaveAttribute("href", candidate().href);
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
  });
  it("does not expose unverified text or links and retains sibling candidates", () => {
    const bad = { ...candidate(), read_status: "unavailable" as const, limitations: [{ source: "model" as const, kind: "missing_evidence", text: "UNVERIFIED_SECRET", truncated: false, impact: null }], href: "https://example.test/secret" };
    render(<ActivitySummary activity={activity(activityScope({ candidates: [bad, { ...candidate(), instrument_id: requestId, symbol: "NVDA", run_status: "running", quality: null, limitations: [] }] }))} />);
    expect(screen.queryByText("UNVERIFIED_SECRET")).not.toBeInTheDocument();
    expect(screen.getByRole("article", { name: "AAPL 本轮研究" })).toHaveTextContent("摘要暂不可核验");
    expect(screen.getByRole("article", { name: "NVDA 本轮研究" })).toHaveTextContent("研究中");
    expect(screen.queryByRole("link", { name: "查看 AAPL 原研究" })).not.toBeInTheDocument();
  });
  it("distinguishes an incomplete read from research quality and degrades old payloads", () => {
    const { rerender } = render(<ActivitySummary activity={{ ...activity(), read_status: "partial" }} />);
    expect(screen.getByText("本轮读回不完整；已核验部分仍按原时点展示。")).toBeVisible();
    rerender(<ActivitySummary activity={undefined} />);
    expect(screen.getByText("当前服务尚未提供本轮运行摘要。")).toBeVisible();
    rerender(<ActivitySummary activity={{ scopes: [{ candidates: "bad" }] }} />);
    expect(screen.getByText("本轮摘要格式不可核验；未展示未核验正文。")).toBeVisible();
  });
});

it("keeps today's count session distinct from a selected historical cycle and labels manual registration", () => {
  render(<ActivitySummary activity={activity(activityScope({ today_registered: 0, today_started: 1,
    decision_session: "2026-10-23", selected_decision_session: "2026-10-22", selection: "historical_unresolved" }))} />);
  const scope = screen.getByRole("region", { name: "正式研究与内部模拟" });
  expect(scope).toHaveTextContent("今日手动未登记");
  expect(scope).toHaveTextContent("本交易日已启动 1 周期");
  expect(scope).toHaveTextContent("启动计数交易日2026-10-23");
  expect(scope).toHaveTextContent("选中周期交易日2026-10-22");
});
