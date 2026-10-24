import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { disclosureBatch, disclosureRunId, disclosureMonitoring, disclosureTrigger, disclosureAt } from "./disclosure-monitoring-fixture";
import { instrumentId, researchData, runId } from "./fixtures";

const props = { state: "partial" as const, admission: null, submitting: false, notice: null, replay: true, onRefresh: vi.fn(), onRun: vi.fn() };
function panel(overrides: Record<string, unknown> = {}) {
  return <ResearchPanel {...props} research={researchData({ disclosure_monitoring: disclosureMonitoring(overrides) })} />;
}
function openEvents() { fireEvent.click(screen.getByText(/查看最近.*项披露事件/)); }

describe("SEC saved monitoring readback", () => {
  it.each([
    ["not_installed", "尚未安装监控计划"], ["valid", "计划有效期内"], ["expired", "计划已过期"],
    ["superseded", "已由新研究替代"], ["withdrawn", "计划已撤回"], ["unknown", "计划状态未知"],
  ])("mounts the separate SEC pane with %s", (state, label) => {
    render(panel({ state }));
    const pane = screen.getByRole("region", { name: "SEC 披露监控记录" });
    expect(within(pane).getByText(label)).toBeVisible();
    expect(within(pane).getByText(/8-K · 8-K\/A/)).toBeVisible();
    expect(within(pane).getByText(/不是经营影响结论/)).toBeVisible();
    expect(within(pane).getByText(/不代表监控进程当前在线/)).toBeVisible();
    expect(within(pane).getByText(/交易保持锁定/)).toBeVisible();
    expect(within(pane).getByText("冻结索引截止时间")).toBeVisible();
  });
  it.each([
    ["events_unresolved", "仍有事件尚未解决"], ["review_unverified", "事件处理提案未通过完整复核"],
    ["replacement_rejected", "监控计划接替未完成"],
  ])("distinguishes %s from audit rejection", (coverage_reason, label) => {
    render(panel({ state: "withdrawn", condition: null, coverage_reason }));
    expect(screen.getByText(new RegExp(label))).toBeVisible();
    expect(screen.queryByText(/研究审查未通过/)).not.toBeInTheDocument();
    expect(screen.queryByText(/8-K · 8-K\/A/)).not.toBeInTheDocument();
  });
  it.each([
    ["true", "该次检查收录了指定类型的新披露"], ["false", "该次检查未收录指定类型的新披露"],
    ["unknown", "该次披露检查结果未知"],
  ])("keeps %s observation separate from investment impact", (result, label) => {
    render(panel({ latest_check: { result, checked_at: disclosureAt, scheduled_at: disclosureAt, reason: result === "unknown" ? "observation_unverified" : null } }));
    expect(screen.getByText(label)).toBeVisible();
    if (result === "unknown") expect(screen.getByText(/披露原件或时间依据尚未核验/)).toBeVisible();
  });
  it.each([
    ["addressed", "事件已复核处理"], ["unresolved", "事件仍未解决"],
    ["unverified", "事件复核尚未获确认"], ["not_performed", "尚未进行事件复核"],
  ])("does not equate completed research with %s event review", (event_review, label) => {
    render(panel({ triggers: [disclosureTrigger({ event_review })] })); openEvents();
    expect(screen.getByText("研究已完成")).toBeVisible();
    expect(screen.getByText(label)).toBeVisible();
    expect(screen.getByRole("link", { name: "查看本次事件的研究" })).toHaveAttribute("href", `/research/${instrumentId}?run_id=${disclosureRunId}`);
    expect(screen.getByText("0000320193-26-000001")).toBeVisible();
    expect(screen.queryByRole("link", { name: /正文/ })).not.toBeInTheDocument();
    expect(screen.getByText(/设备送达状态未知/)).toBeVisible();
  });
  it("groups the visible subset by sealed batch, retaining the total without implying new work", () => {
    const triggers = Array.from({ length: 7 }, (_, index) => disclosureTrigger({ trigger_id: `trigger-${index}`, event_key: `sec:0000320193:0000320193-26-00000${index}`, checked_at: `2026-10-24T11:3${index}:00Z`, batch_id: index > 3 ? disclosureBatch : null }));
    render(panel({ triggers, trigger_count: 7 })); openEvents();
    expect(screen.getByText(/共记录 7 项披露事件/)).toBeVisible();
    expect(screen.getAllByRole("link", { name: "查看本次事件的研究" })).toHaveLength(5);
    expect(screen.getAllByText("同一封存批次")).toHaveLength(1);
    expect(screen.getByText(/仅显示最近 5 项/)).toHaveTextContent("不表示批次已拆分");
    expect(screen.getByText(/本区只列披露成员/)).toBeVisible();
    expect(screen.queryByText("0000320193-26-000000")).not.toBeInTheDocument();
  });
  it("preserves unknown budget without fetching, writing, or mutating the response", () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    const monitoring = disclosureMonitoring({ triggers: [disclosureTrigger({ status: "unknown", usage_status: "unknown", notification_status: "unknown", event_review: "unverified" })] });
    const original = JSON.stringify(monitoring);
    render(<ResearchPanel {...props} research={researchData({ disclosure_monitoring: monitoring })} />); openEvents();
    const pane = screen.getByRole("region", { name: "SEC 披露监控记录" });
    expect(within(pane).getByText(/用量未知，预算继续保留/)).toBeVisible();
    expect(within(pane).queryByRole("button")).not.toBeInTheDocument();
    expect(fetch).not.toHaveBeenCalled(); expect(JSON.stringify(monitoring)).toBe(original); fetch.mockRestore();
  });
  it.each([
    ["pending", "等待处理"], ["blocked", "处理受阻"], ["admitted", "研究已登记"],
    ["completed", "研究已完成"], ["failed", "研究失败"], ["unknown", "研究结果未知"],
    ["covered", "已由其他研究覆盖"],
  ])("preserves %s independently of unresolved events", (status, label) => {
    render(panel({ triggers: [disclosureTrigger({ status, blocked_reason: status === "blocked" ? "overflow" : null })] })); openEvents();
    expect(screen.getByText(label, { exact: true })).toBeVisible();
    expect(screen.getByText("事件仍未解决")).toBeVisible();
    if (status === "blocked") expect(screen.getByText(/待处理任务达到容量上限/)).toBeVisible();
  });
  it.each([
    ["events_unresolved", "仍有事件尚未解决"], ["review_unverified", "事件处理提案未通过完整复核"],
    ["replacement_rejected", "监控计划接替未完成"],
  ])("also explains the price coverage %s without claiming audit veto", (coverage_reason, label) => {
    const monitoring = { observed_at: disclosureAt, state: "withdrawn", coverage_reason, plan_id: null,
      condition: null, latest_check: null, review_due_at: null, valid_until: null, replacement_run_id: null,
      trigger_count: 0, triggers: [], execution_locked: true };
    render(<ResearchPanel {...props} research={researchData({ monitoring })} />);
    expect(screen.getByText(new RegExp(label))).toBeVisible();
    expect(screen.queryByText(/研究审查未通过/)).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "SEC 披露监控记录" })).not.toBeInTheDocument();
  });
  it("keeps price and disclosure dimensions independent", () => {
    const monitoring = { observed_at: disclosureAt, state: "valid", coverage_reason: null, plan_id: null,
      condition: null, latest_check: null, review_due_at: null, valid_until: null, replacement_run_id: null,
      trigger_count: 0, triggers: [], execution_locked: true };
    render(<ResearchPanel {...props} research={researchData({ monitoring, disclosure_monitoring: disclosureMonitoring({ state: "withdrawn", condition: null, coverage_reason: "events_unresolved" }) })} />);
    expect(within(screen.getByRole("region", { name: "价格监控记录" })).getByText("计划有效期内")).toBeVisible();
    expect(within(screen.getByRole("region", { name: "SEC 披露监控记录" })).getByText("计划已撤回")).toBeVisible();
  });
  it("links exact replacement and does not turn malformed event identity into a source URL", () => {
    render(panel({ state: "superseded", replacement_run_id: runId, triggers: [disclosureTrigger({ event_key: "https://attacker.invalid/not-an-accession", run_id: null })] })); openEvents();
    expect(screen.getByRole("link", { name: "查看替代披露研究" })).toHaveAttribute("href", `/research/${instrumentId}?run_id=${runId}`);
    expect(screen.getByText("无法核验申报号")).toBeVisible();
    expect(screen.queryByRole("link", { name: "查看本次事件的研究" })).not.toBeInTheDocument();
    expect(screen.queryByText(/attacker/)).not.toBeInTheDocument();
  });
  it("keeps empty observations unknown and removes SEC content for legacy or forbidden responses", () => {
    const { rerender } = render(panel({ triggers: [], trigger_count: 0, condition: null }));
    expect(screen.getByText(/尚无已保存的披露检查/)).toBeVisible();
    expect(screen.getByText(/本次读回未列出披露事件明细/)).toBeVisible();
    rerender(<ResearchPanel {...props} research={researchData()} />);
    expect(screen.queryByRole("region", { name: "SEC 披露监控记录" })).not.toBeInTheDocument();
    rerender(<ResearchPanel {...props} state="forbidden" research={researchData({ disclosure_monitoring: disclosureMonitoring() })} />);
    expect(screen.queryByRole("region", { name: "SEC 披露监控记录" })).not.toBeInTheDocument();
  });
});
