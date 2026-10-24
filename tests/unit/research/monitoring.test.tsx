import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { ResearchMonitoring, type PriceMonitoringView } from "../../../app/components/research-monitoring";
import { researchData } from "./fixtures";

const instrumentId = "0198d500-1000-7000-8000-000000009001";
const replacement = "0198d500-1000-7000-8000-000000009011";
const at = "2026-10-24T11:30:00Z";

function monitoring(overrides: Partial<PriceMonitoringView> = {}): PriceMonitoringView {
  return {
    observed_at: at, state: "valid", plan_id: "plan-1", valid_until: "2026-10-25T12:00:00Z",
    review_due_at: "2026-10-25T10:00:00Z", replacement_run_id: null,
    condition: { currency: "USD", feed: "l1", max_age_seconds: 60, operator: "gte",
      price_basis: "unadjusted_bid_ask_midpoint", purpose: "review", signal: "schwab.l1.bid_ask_midpoint",
      source: "schwab", threshold: "123.4567", unit: "currency" },
    latest_check: null, triggers: [], trigger_count: 0, execution_locked: true, ...overrides,
  };
}

function trigger(overrides: Partial<PriceMonitoringView["triggers"][number]> = {}): PriceMonitoringView["triggers"][number] {
  return { trigger_id: "trigger-1", cause: "crossed", checked_at: at, status: "pending", blocked_reason: null,
    run_id: null, usage_status: "not_started", notification_status: "queued", ...overrides };
}

describe("saved price monitoring readback", () => {
  it.each([
    ["not_installed", "尚未安装监控计划"], ["valid", "计划有效期内"],
    ["expired", "计划已过期"], ["superseded", "已由新研究替代"],
    ["withdrawn", "计划已撤回"], ["unknown", "计划状态未知"],
  ] as const)("preserves %s without asserting live monitoring", (state, label) => {
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring({ state })} />);
    expect(screen.getByText(label, { exact: true })).toBeVisible();
    expect(screen.getByText(/读取时点/)).toHaveTextContent("2026-10-24 19:30:00 北京时间");
    expect(screen.getByText(/不代表监控进程当前在线/)).toBeVisible();
    expect(screen.getByText(/交易保持锁定/)).toBeVisible();
    expect(screen.queryByText("正在持续监控")).not.toBeInTheDocument();
  });

  it("keeps missing checks distinct from a false condition and shows the price basis", () => {
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring()} />);
    expect(screen.getByText(/尚无已保存的检查/)).toHaveTextContent("不能据此认定条件未满足或没有触发");
    expect(screen.getByText(/买卖报价中间价/)).toHaveTextContent("未复权");
    expect(screen.getByText(/不低于/)).toHaveTextContent("123.457 USD");
    expect(screen.getByText(/60 秒/)).toBeVisible();
  });

  it.each([
    ["audit_rejected", "研究审查未通过"], ["source_invalid", "监控所需来源未通过核验"],
    ["no_condition", "未提供可安装的价格复核条件"], ["run_failed", "研究运行失败"],
    ["usage_unknown", "预算继续保留"], ["awaiting_reconciliation", "研究结果仍待核对"],
  ] as const)("preserves the %s coverage gap without exposing internal codes", (coverage_reason, text) => {
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring({ state: "not_installed", condition: null, coverage_reason })} />);
    expect(screen.getByText(new RegExp(text))).toBeVisible();
    expect(screen.queryByText(coverage_reason)).not.toBeInTheDocument();
    expect(screen.queryByText(/不低于/)).not.toBeInTheDocument();
  });

  it("preserves the lower price direction and exact threshold without mutating its source", () => {
    const data = monitoring();
    data.condition = { ...data.condition!, operator: "lte" };
    const original = JSON.stringify(data);
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={data} />);
    expect(screen.getByText(/不高于/)).toHaveTextContent("123.457 USD");
    expect(screen.getByTitle("精确阈值：123.4567 USD")).toBeVisible();
    expect(JSON.stringify(data)).toBe(original);
  });

  it.each([
    ["true", "该次检查条件已满足"], ["false", "该次检查条件未满足"], ["unknown", "该次检查结果未知"],
  ] as const)("reports only the saved %s check result", (result, label) => {
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring({
      latest_check: { result, scheduled_at: at, checked_at: at, reason: result === "unknown" ? "time_unproven" : null },
    })} />);
    expect(screen.getByText(label)).toBeVisible();
    expect(screen.getByText("计划检查时间")).toBeVisible();
    expect(screen.getByText("实际检查时间")).toBeVisible();
    if (result === "unknown") expect(screen.getByText(/报价时间无法核验/)).toBeVisible();
  });

  it.each([
    ["observation_missing", "缺少已保存的观测"], ["authority_mismatch", "报价来源或标的不匹配"],
    ["time_unproven", "报价时间无法核验"], ["price_missing", "缺少可用价格"],
  ] as const)("explains %s in plain language", (reason, text) => {
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring({
      latest_check: { result: "unknown", scheduled_at: at, checked_at: at, reason },
    })} />);
    expect(screen.getByText(new RegExp(text))).toBeVisible();
    expect(screen.queryByText(reason)).not.toBeInTheDocument();
  });

  it("links to the exact replacement and triggered research without changing the historical run", () => {
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring({
      state: "superseded", replacement_run_id: replacement, trigger_count: 1,
      triggers: [trigger({ run_id: replacement, status: "completed", usage_status: "known", notification_status: "published" })],
    })} />);
    const href = `/research/${instrumentId}?run_id=${replacement}`;
    expect(screen.getByRole("link", { name: "查看替代研究" })).toHaveAttribute("href", href);
    fireEvent.click(screen.getByText(/查看最近/));
    expect(screen.getByRole("link", { name: "查看本次触发的研究" })).toHaveAttribute("href", href);
    expect(screen.getByText(/设备送达状态未知/)).toBeVisible();
    expect(screen.queryByText("设备已送达")).not.toBeInTheDocument();
  });

  it("preserves unknown usage and budget protection without fetches or write controls", () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring({ trigger_count: 1,
      triggers: [trigger({ status: "blocked", blocked_reason: "budget", usage_status: "unknown", notification_status: "unknown" })],
    })} />);
    fireEvent.click(screen.getByText(/查看最近/));
    expect(screen.getByText(/预算不足或预算仍被占用/)).toBeVisible();
    expect(screen.getByText(/用量未知，预算继续保留/)).toBeVisible();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    expect(fetchSpy).not.toHaveBeenCalled();
    fetchSpy.mockRestore();
  });

  it.each([
    ["pending", "等待处理"], ["blocked", "处理受阻"], ["admitted", "研究已登记"],
    ["completed", "研究已完成"], ["failed", "研究失败"], ["unknown", "研究结果未知"],
    ["covered", "已由其他研究覆盖"],
  ] as const)("keeps research %s distinct from notification processing", (status, label) => {
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring({ trigger_count: 1,
      triggers: [trigger({ status, notification_status: "dead_letter" })],
    })} />);
    fireEvent.click(screen.getByText(/查看最近/));
    expect(screen.getByText(label, { exact: true })).toBeVisible();
    expect(screen.getByText("通知处理失败")).toBeVisible();
    expect(screen.getByText(/设备送达状态未知/)).toBeVisible();
  });

  it("shows only the five most recent triggers while retaining the total", () => {
    const triggers = Array.from({ length: 7 }, (_, index) => trigger({ trigger_id: `trigger-${index}`,
      checked_at: `2026-10-24T11:3${index}:00Z`, run_id: `run-${index}` }));
    render(<ResearchMonitoring instrumentId={instrumentId} monitoring={monitoring({ triggers, trigger_count: 7 })} />);
    expect(screen.getByText(/已记录 7 次触发/)).toBeVisible();
    fireEvent.click(screen.getByText(/查看最近/));
    const links = screen.getAllByRole("link", { name: "查看本次触发的研究" });
    expect(links).toHaveLength(5);
    expect(links[0]).toHaveAttribute("href", `/research/${instrumentId}?run_id=run-6`);
    expect(links[4]).toHaveAttribute("href", `/research/${instrumentId}?run_id=run-2`);
  });

  it("mounts after frozen research when provided and leaves legacy payloads unchanged", () => {
    const research = { ...researchData(), monitoring: monitoring() };
    const props = { state: "partial" as const, admission: null, submitting: false, notice: null, replay: true, onRefresh: vi.fn(), onRun: vi.fn() };
    const { rerender } = render(<ResearchPanel {...props} research={research} />);
    expect(screen.getByRole("region", { name: "价格监控记录" })).toBeVisible();
    expect(within(screen.getByRole("region", { name: "价格监控记录" })).getByText(/冻结研究内容保持原样/)).toBeVisible();
    rerender(<ResearchPanel {...props} research={researchData()} />);
    expect(screen.queryByRole("region", { name: "价格监控记录" })).not.toBeInTheDocument();
  });
});
