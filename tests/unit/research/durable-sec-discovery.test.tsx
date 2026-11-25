import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { DurableSecDiscovery } from "../../../app/components/durable-sec-discovery";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { researchData } from "./fixtures";
import { disclosureMonitoring } from "./disclosure-monitoring-fixture";
import type { components } from "../../../app/lib/generated/schema";
import { runtimeContractSchemas } from "../../../app/lib/generated/runtime-contract";
const at = "2026-11-17T03:00:00Z";
const view: components["schemas"]["DurableSecView"] = {
  observed_at: at, rule_id: "00000000-0000-4000-8000-000000000001", state: "active",
  effective_at: "2026-11-17T00:00:00Z", forms: ["8-K", "8-K/A"], execution_locked: true,
  latest_check: { scheduled_at: at, checked_at: at, result: "true", coverage: "partial", interval_start: "2026-11-17T00:00:00Z", interval_end: at, reason: null, coverage_details: null },
  event_count: 1, events: [{ event_key: "sec:0000123456:0000123456-26-000099", form: "8-K",
    accepted_at: "2026-11-17T01:00:00Z", first_seen_at: at, metadata_status: "recorded" }],
};
describe("durable SEC discovery facts", () => {
  it("shows capture failure without changing the previously saved discovery", () => {
    const discovery = { ...view, observed_at: "2026-11-17T03:06:00Z", latest_collection: {
      scheduled_at: "2026-11-17T03:05:00Z", claimed_at: "2026-11-17T03:05:01Z",
      finished_at: "2026-11-17T03:05:31Z", status: "failed" as const,
    } };
    expect(runtimeContractSchemas.DurableSecView.required).not.toContain("latest_collection");
    render(<DurableSecDiscovery discovery={discovery} />);
    expect(within(screen.getByRole("region", { name: "最近 SEC 采集回执" }))
      .getByText("采集失败")).toBeVisible();
    expect(within(screen.getByRole("region", { name: "最近公告发现检查" }))
      .getByText(/新公告 · 部分覆盖/)).toBeVisible();
  });

  it("accepts discovery events from an engine without notification readback", () => {
    expect(runtimeContractSchemas.DurableSecEventView.required).not.toContain("notice");
    expect(runtimeContractSchemas.DurableSecEventView.required).not.toContain("event_id");
    render(<DurableSecDiscovery discovery={view} />);
    expect(screen.getByText("暂无可核验通知记录。")).toBeVisible();
  });
  it("explains unread history separately from bad rows without inventing a negative", () => {
    render(<DurableSecDiscovery discovery={{ ...view, latest_check: { ...view.latest_check!, result: "unknown", reason: "coverage_incomplete", coverage_details: { unread_history_files: 1, invalid_rows: 0, coverage_start_known: true } } }} />);
    expect(screen.getByText(/另有 1 份历史分片未纳入本次核验/)).toBeVisible();
    expect(screen.getByText(/已核验原件没有发现坏行/)).toBeVisible();
    expect(screen.getByText(/独立可证的新增仍会单独核验/)).toBeVisible();
    expect(screen.queryByText(/完整覆盖区间未发现新公告/)).not.toBeInTheDocument();
  });
  it("shows a verified capture and a missing acceptance cutoff separately", () => {
    render(<DurableSecDiscovery discovery={{ ...view, latest_check: {
      ...view.latest_check!, result: "unknown", reason: "accepted_cutoff_unproven",
      evaluation_version: "accepted_cutoff_v1", source_received_at: at,
      accepted_complete_through_at: null,
    } }} />);
    expect(screen.getByText(/SEC 原件收到时间/)).toBeVisible();
    expect(screen.getByText(/尚无完整受理截止证明/)).toBeVisible();
    expect(screen.queryByText(/完整覆盖区间未发现新公告/)).not.toBeInTheDocument();
  });
  it("labels a historical negative without presenting it as current coverage", () => {
    render(<DurableSecDiscovery discovery={{ ...view, latest_check: {
      ...view.latest_check!, result: "false", coverage: "complete",
    } }} />);
    expect(screen.getByText(/历史记录（按旧口径）/)).toBeVisible();
    expect(screen.getByText(/当前受理区间完整性未知/)).toBeVisible();
  });
  it("mounts active discovery beside an expired research plan without granting admission", () => {
    render(<ResearchPanel state="partial" admission={null} submitting={false} notice={null} replay={true} onRefresh={vi.fn()} onRun={vi.fn()}
      research={researchData({ durable_disclosure: view, disclosure_monitoring: disclosureMonitoring({ state: "expired" }) })} />);
    expect(screen.getByRole("region", { name: "长期 SEC 公告发现" })).toBeVisible();
    expect(screen.getByText("规则已启用")).toBeVisible();
    expect(screen.getByText("计划已过期")).toBeVisible();
    expect(screen.queryByRole("button", { name: "重新运行冻结研究" })).not.toBeInTheDocument();
  });
  it.each([["active", "规则已启用"], ["paused", "规则已暂停"], ["revoked", "规则已撤销"], ["unknown", "规则状态待核验"]] as const)("shows %s independently of research expiry", (state, label) => {
    render(<DurableSecDiscovery discovery={{ ...view, state }} />);
    expect(screen.getByText(label)).toBeVisible();
    expect(screen.getByText(/不随旧研究计划到期/)).toBeVisible();
    expect(screen.getByText(/每日增量名额与未知费用仍独立核验/)).toBeVisible();
  });
  it("retains partial positive and distinct acceptance/first-discovery times with no calls", () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    render(<DurableSecDiscovery discovery={view} />);
    expect(screen.getByText(/新公告 · 部分覆盖/)).toBeVisible();
    expect(screen.getByText("SEC 接受时间")).toBeVisible();
    expect(screen.getByText("首次发现时间")).toBeVisible();
    expect(screen.getByText(/未覆盖部分保持未知/)).toBeVisible();
    expect(fetch).not.toHaveBeenCalled(); fetch.mockRestore();
  });
  it("does not infer no events from a missing check", () => {
    render(<DurableSecDiscovery discovery={{ ...view, latest_check: null }} />);
    expect(screen.getByText(/尚无可核验的已保存检查；不表示没有公告/)).toBeVisible();
    expect(screen.queryByText(/完整覆盖区间未发现/)).not.toBeInTheDocument();
  });
  it("keeps metadata conflict visible", () => {
    render(<DurableSecDiscovery discovery={{ ...view, events: [{ ...view.events[0], metadata_status: "conflict" }] }} />);
    expect(screen.getByText(/公告元数据出现冲突/)).toBeVisible();
  });
  it.each([["unresolved", "未决"], ["addressed", "已回答"]] as const)("shows the original %s daily review on its exact event", (status, label) => {
    const eventId = "00000000-0000-4000-8000-000000000099";
    const otherId = "00000000-0000-4000-8000-000000000098";
    const rationale = "调查的经营和财务影响仍缺乏证据。";
    const review = { context_hash: "a".repeat(64), items: [{ event_id: eventId, status, rationale, evidence_refs: [] }] };
    const event = view.events[0];
    const discovery: components["schemas"]["DurableSecView"] = { ...view, event_count: 3, events: [
      { ...event, event_id: otherId, event_key: "sec:0000123456:0000123456-26-000098" },
      { ...event, event_id: eventId, research_status: "bound" },
      { ...event, event_key: "sec:0000123456:0000123456-26-000097" },
    ] };
    const research = researchData({ intent: { ...researchData().intent, durable_sec_pending_proposal: review }, durable_disclosure: discovery });
    render(<ResearchPanel state="ready" admission={null} submitting={false} notice={null} replay={true} onRefresh={vi.fn()} onRun={vi.fn()} research={research} />);
    const card = screen.getByRole("heading", { name: "8-K · 0000123456-26-000099" }).closest("article")!;
    expect(within(card).getByText(`本次日研判断 · ${label}`)).toBeVisible();
    expect(within(card).getByText(rationale)).toBeVisible();
    expect(within(card).getByText("已关联增量材料，暂无逐事件处理回执。")).toBeVisible();
    expect(screen.getAllByText(rationale)).toHaveLength(1);
  });
  it.each([
    ["increment_failed", "原增量研究失败，暂无逐事件处理回执。"],
    ["addressed_daily", "已保存日研的逐事件处理回执。"],
    ["addressed_increment", "已保存增量研究的逐事件处理回执。"],
  ] as const)("shows %s on the original event without an action", (research_status, label) => {
    render(<DurableSecDiscovery discovery={{ ...view, events: [{ ...view.events[0], research_status }] }} />);
    expect(screen.getByText(label)).toBeVisible();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
  it("shows restricted discovery as observation only with no action or notice", () => {
    render(<DurableSecDiscovery discovery={{ ...view, events: [
      { ...view.events[0], research_status: "observation_only" },
    ] }} />);
    expect(screen.getByText("仅记录公告发现，未触发通知或研究。")).toBeVisible();
    expect(screen.queryByText("暂无可核验通知记录。")).not.toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
  it("shows the original preparation rejection without claiming a failed model run", () => {
    const event = { ...view.events[0], research_status: "bound" as const,
      preparation_rejection: { rejected_at: at, reason: "ineligible" as const } };
    render(<DurableSecDiscovery discovery={{ ...view, events: [event] }} />);
    expect(screen.getByText("原增量在准备阶段被拒，未创建研究任务。")).toBeVisible();
    expect(screen.getByText("原回执未记录具体拦截条件。")).toBeVisible();
    expect(screen.queryByText("已关联增量材料，暂无逐事件处理回执。")).not.toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
  it("shows the original timeout as an unconfirmed server result with one attempt", () => {
    const id = "00000000-0000-4000-8000-000000000099";
    render(<DurableSecDiscovery discovery={{ ...view, events: [{ ...view.events[0], notice: {
      outbox_id: id, status: "dead_letter", attempt_count: 1, attempted_at: at, finished_at: at,
      server_result: "unconfirmed", device_result: "unknown", failure_reason: "READ_TIMEOUT",
    } }] }} />);
    expect(screen.getByText("发送尝试已结束 · 1 次尝试")).toBeVisible();
    expect(screen.getByText("等待 Bark 响应超时")).toBeVisible();
    expect(screen.getByText("手机实收")).toBeVisible();
    expect(screen.getAllByText("待核验")).toHaveLength(2);
    expect(screen.queryByText("Bark 已接受")).not.toBeInTheDocument();
    expect(screen.getByText(id)).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
  it("shows a Bark acknowledgment without treating it as device receipt", () => {
    render(<DurableSecDiscovery discovery={{ ...view, events: [{ ...view.events[0], notice: {
      outbox_id: "00000000-0000-4000-8000-000000000099", status: "published", attempt_count: 1,
      attempted_at: at, finished_at: at, server_result: "accepted", device_result: "unknown",
      failure_reason: null,
    } }] }} />);
    expect(screen.getByText("Bark 已接受")).toBeVisible();
    expect(screen.getByText("手机实收")).toBeVisible();
    expect(screen.getByText("待核验")).toBeVisible();
  });
});
