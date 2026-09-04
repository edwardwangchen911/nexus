import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ResearchInputs } from "../../../app/components/research-inputs";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { actionEvent, actionInputs, actionObservation } from "./action-fixtures";
import { decisionTime, navHistoryInputs, researchAdmissionData, researchData } from "./fixtures";

function expandEvent() {
  fireEvent.click(screen.getByText("查看公司行动观察记录"));
  fireEvent.click(screen.getByText("观察 1 · 第 1 页"));
  return screen.getByRole("table", { name: "观察 1 的来源字段" });
}

describe("frozen action observations", () => {
  it("does not assign dividend meaning to an unknown category's ex-date", () => {
    render(<ResearchInputs inputs={{ ...actionInputs(),
      corporate_action_observations: [actionObservation({ events: [actionEvent({
        category: "unknown_action_category", ex_date: "2026-08-12",
      })] })],
    }} asOf={decisionTime} />);
    const table = expandEvent();
    expect(within(table).getByRole("row", { name: /^来源 ex-date 日期/ })).toHaveTextContent("2026-08-12");
    expect(within(table).queryByText(/^除息日期/)).not.toBeInTheDocument();
  });
  it("separates zero, tiny, unknown and values outside the presentation contract", () => {
    render(<ResearchInputs inputs={actionInputs()} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "公司行动观测资料" });
    expect(screen.getByRole("heading", { name: "TEST · 已冻结公司行动资料" })).toBeVisible();
    expect(region).toHaveTextContent("2026-08-01 至 2026-08-23");
    expect(region).toHaveTextContent("来源完整性未知");
    expect(region).toHaveTextContent("不是历史公告时间");
    expect(region).toHaveTextContent("不用于历史回测或账本入账");
    expect(region).toHaveTextContent("2 页");
    expect(region).toHaveTextContent("已绑定 1 条 / 未绑定 2 条");
    const table = expandEvent();
    expect(within(table).getByRole("row", { name: /^来源数值（rate）/ })).toHaveTextContent("0");
    expect(within(table).getByRole("row", { name: /^原数值（old_rate）/ })).toHaveTextContent("<0.001");
    expect(within(table).getByRole("row", { name: /^新数值（new_rate）/ })).toHaveTextContent("原件数值超出展示范围");
    expect(within(table).getByRole("row", { name: /^来源 ex-date 日期/ })).toHaveTextContent("未知");
    expect(within(table).getByRole("row", { name: /^来源币种/ })).toHaveTextContent("未知（原件为空）");
    expect(region).not.toHaveTextContent("USD");
    expect(region).toHaveTextContent(actionEvent().provider_event_id);
    expect(region).toHaveTextContent("类别内索引 0");
    fireEvent.click(screen.getByText("公司行动证据与版本"));
    expect(region).toHaveTextContent("2026-08-25 02:30:00 北京时间");
    expect(region).toHaveTextContent("b".repeat(64));
  });

  it("uses neutral labels for action-only waiting research and keeps execution unavailable", () => {
    render(<ResearchPanel state="empty" research={null} admission={researchAdmissionData({
      inputs: actionInputs(), eligible: false, reason: "prompt_authority_unavailable",
    })} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByText("资料已冻结 · 研究待生成")).toBeVisible();
    expect(screen.getByText(/公司行动资料已冻结，AI 研究尚未生成/)).toBeVisible();
    expect(screen.queryByText(/财报资料已就绪|财报已冻结/)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /运行冻结研究/ })).not.toBeInTheDocument();
  });

  it("keeps mixed source headings neutral and preserves existing NAV rendering", () => {
    render(<ResearchInputs inputs={{ ...navHistoryInputs(), corporate_action_observations: [actionObservation()] }} asOf={decisionTime} />);
    expect(screen.getByRole("heading", { name: "TEST · 已冻结研究资料" })).toBeVisible();
    expect(screen.getByRole("region", { name: "公司行动观测资料" })).toBeVisible();
    expect(screen.getByRole("region", { name: "发行商净值历史资料" })).toBeVisible();
  });

  it("keeps SEC and action sources separate in mixed frozen inputs", () => {
    const inputs = actionInputs();
    inputs.filings = [{
      evidence_ref_id: "0198d600-1000-7000-8000-000000001584", source: "sec", form: "10-K",
      accession_number: "0000000001-26-000001", accepted_at: "2026-08-20T12:00:00Z",
      received_at: "2026-08-20T12:01:00Z", revision: "c".repeat(64), content_hash: "c".repeat(64),
      disclosure_truncated: false,
    }];
    render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    expect(screen.getByRole("heading", { name: "TEST · 已冻结研究资料" })).toBeVisible();
    expect(screen.getByText("SEC / Alpaca · 本地只读证据")).toBeVisible();
    expect(screen.getByRole("heading", { name: "SEC 10-K" })).toBeVisible();
  });

  it.each([
    { rate: null, expected: "未知" }, { rate: "-0.0000001", expected: "(-0.001, 0)" },
    { rate: "123.456789", expected: "123.457" },
    { rate: "9".repeat(40), expected: "9".repeat(40) },
  ])("preserves numeric presentation boundary $expected", ({ rate, expected }) => {
    render(<ResearchInputs inputs={{ ...actionInputs(), corporate_action_observations: [actionObservation({
      events: [actionEvent({ rate })],
    })] }} asOf={decisionTime} />);
    const table = expandEvent();
    const row = within(table).getByRole("row", { name: /^来源数值（rate）/ });
    expect(within(row).getByRole("cell")).toHaveTextContent(expected);
    expect(within(row).getByRole("cell")).not.toHaveTextContent("Infinity");
  });

  it("does not claim no actions when zero observations bind to this instrument", () => {
    render(<ResearchInputs inputs={{ ...actionInputs(), corporate_action_observations: [actionObservation({
      events: [], bound_event_count: 0, unbound_event_count: 3,
    })] }} asOf={decisionTime} />);
    expect(screen.getByText(/没有可展示的已绑定观测，不代表该证券没有公司行动/)).toBeVisible();
    expect(screen.getByText(/已绑定 0 条 \/ 未绑定 3 条/)).toBeVisible();
    expect(screen.queryByText("查看公司行动观察记录")).not.toBeInTheDocument();
  });

  it("retains truncation counts and at most 32 displayed observations", () => {
    const events = Array.from({ length: 32 }, (_, index) => actionEvent({ index, provider_event_id: `synthetic-${index}` }));
    render(<ResearchInputs inputs={{ ...actionInputs(), corporate_action_observations: [actionObservation({
      events, total_event_count: 40, bound_event_count: 38, unbound_event_count: 2, truncated: true,
    })] }} asOf={decisionTime} />);
    expect(screen.getByText(/显示 32 \/ 38 条已绑定观测/)).toBeVisible();
    fireEvent.click(screen.getByText("查看公司行动观察记录"));
    expect(screen.getAllByText(/^观察 \d+ · 第 1 页$/)).toHaveLength(32);
  });

  it("renders absent optional fields without fabricating action evidence", () => {
    const { corporate_action_observations: omitted, ...old } = actionInputs();
    void omitted;
    render(<ResearchInputs inputs={old as ReturnType<typeof actionInputs>} asOf={decisionTime} />);
    expect(screen.queryByRole("region", { name: "公司行动观测资料" })).not.toBeInTheDocument();
  });

  it("uses only the exact run's frozen inputs during replay and never current admission", () => {
    render(<ResearchPanel state="ready" research={researchData({ inputs: actionInputs() })}
      admission={researchAdmissionData({ inputs: navHistoryInputs() })} replay
      submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByRole("region", { name: "公司行动观测资料" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "发行商净值历史资料" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /运行冻结研究/ })).not.toBeInTheDocument();
  });
});
