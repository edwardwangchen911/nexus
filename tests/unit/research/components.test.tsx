import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useRef, useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { BriefPanel } from "../../../app/(protected)/briefs/[sessionDate]/page";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { DecisionCard } from "../../../app/components/decision-card";
import { EvidenceDrawer } from "../../../app/components/evidence-drawer";
import { ResearchInputs } from "../../../app/components/research-inputs";
import type { DataStateKind } from "../../../app/components/data-state";
import { briefData, evidence, researchAdmissionData, researchData } from "./fixtures";

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"], ["empty", "暂无数据"], ["ready", "数据已就绪"],
  ["partial", "数据不完整"], ["stale", "数据已过期"], ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

describe("research evidence docket", () => {
  it.each(["6-K", "497K"])("distinguishes inert %s text from financial metrics and expands on demand", (form) => {
    const source = "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/doc.htm";
    const excerpt = "Company has not decided. <script>alert('not executable')</script>";
    render(<ResearchInputs asOf="2026-09-02T06:00:00Z" inputs={{
      display_symbol: "TEST", total_facts: 0, truncated: false, facts: [],
      filings: [{ evidence_ref_id: "filing-1", source: "sec", form, accession_number: "0000000001-26-000001", accepted_at: "2026-08-21T20:00:00Z", received_at: "2026-09-02T05:59:00Z", revision: "a".repeat(64), content_hash: "a".repeat(64), disclosure_excerpt: excerpt, disclosure_truncated: true, source_url: source }],
    }} />);
    expect(screen.getByRole("heading", { name: form === "497K" ? "TEST · 已冻结基金资料" : "TEST · 已冻结披露" })).toBeVisible();
    if (form === "497K") expect(screen.getByText(/不是公司财报，也不是基金最新持仓/)).toBeVisible();
    expect(screen.getByText(/文本披露，不是结构化财务指标/)).toBeVisible();
    expect(screen.getByText("尚无结构化财务指标。")).toBeVisible();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    const details = screen.getByText("查看冻结披露正文（节选）").closest("details")!;
    expect(details).not.toHaveAttribute("open");
    fireEvent.click(screen.getByText("查看冻结披露正文（节选）"));
    expect(screen.getByText(excerpt)).toBeVisible();
    expect(details.querySelector("script")).toBeNull();
    expect(screen.getByRole("link", { name: "查看 SEC 原文" })).toHaveAttribute("href", source);
  });
  it("shows frozen portfolio context and missing market evidence without a model", () => {
    render(<ResearchInputs asOf="2026-09-02T06:00:00Z" inputs={{
      display_symbol: "TEST", total_facts: 0, truncated: false, filings: [], facts: [],
      portfolio_context: {
        instrument_id: "instrument", portfolio_snapshot_id: "snapshot",
        snapshot_as_of: "2026-09-02T03:33:00Z", research_as_of: "2026-09-02T06:00:00Z",
        source: "fosun", currency: "USD", position_weight: "0.4", cash_weight: "0.6",
        valuation_reason: null, snapshot_decision_ready: true, execution_locked: true,
        diagnostic_run_id: "diagnostic", optimization_need: "RESEARCH_REQUIRED",
        diagnostic_input_hash: "d".repeat(64), diagnostics_truncated: false,
        research_focus: {
          question: "该持仓触发单一标的集中度预警：现有证据支持维持，还是需要研究减仓？",
          diagnostic_code: "CONCENTRATION_TOP1", policy_scope: "currency:USD",
          policy_version: "real_advisory_v1", observed_weight: "0.7", reference_limit: "0.35",
          weight_basis: "same_currency_securities_excluding_cash",
        },
        diagnostics: [{ code: "MARKET_EVIDENCE", severity: "warning", actionability: "research", unknown_reason: "MARKET_EVIDENCE_UNAVAILABLE" }],
        evidence_ref_id: "evidence", content_hash: "a".repeat(64),
      },
    }} />);
    expect(screen.getByRole("heading", { name: "此持仓的冻结组合背景" })).toBeVisible();
    expect(screen.getByText("40%（USD 资产内）")).toBeVisible();
    expect(screen.getByRole("region", { name: "本次组合研究问题" })).toBeVisible();
    expect(screen.getByText(/均为同币种证券内比例，不含现金/)).toBeVisible();
    expect(screen.getByText(/预警触发研究，不代表已决定减仓/)).toBeVisible();
    expect(screen.getByText(/历史风险证据尚未齐备/)).toBeVisible();
    expect(screen.getByText(/不是实时仓位/)).toBeVisible();
    expect(screen.queryByRole("button", { name: /交易|下单/ })).not.toBeInTheDocument();
  });
  it("shows frozen financial inputs without pretending an AI report exists", () => {
    render(<ResearchPanel state="empty" research={null} admission={researchAdmissionData({
      inputs: {
        display_symbol: "AAPL", total_facts: 1, truncated: false,
        filings: [{ evidence_ref_id: "filing-1", source: "sec", form: "10-Q", accession_number: "0000320193-26-000001", accepted_at: "2026-08-20T20:00:00Z", received_at: "2026-08-24T20:00:00Z", revision: "filing-v1", content_hash: "a".repeat(64) }],
        facts: [{ id: "fact-1", evidence_ref_id: "filing-1", fact: "net_income", raw_tag: "NetIncomeLoss", value: "123.45", unit: "USD", period_kind: "duration", period_start: "2026-04-01", period_end: "2026-06-30" }],
      },
    })} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByRole("heading", { name: "AAPL · 已冻结财报" })).toBeVisible();
    expect(screen.getByText("123.45")).toBeVisible();
    expect(screen.getByText(/2026-04-01.*2026-06-30/)).toBeVisible();
    expect(screen.getByText("0000320193-26-000001")).toBeVisible();
    expect(screen.getByText(/财报资料已就绪，AI 研究尚未生成/)).toBeVisible();
    expect(screen.queryByRole("heading", { name: "委员会决策记录" })).not.toBeInTheDocument();
    expect(screen.queryByText("请从市场或每日简报进入，并保留 URL 中的 as_of。")).not.toBeInTheDocument();
  });
  it.each(states)("renders research %s as an explicit visible state", (state, label) => {
    render(<ResearchPanel
      state={state}
      research={["ready", "partial", "stale"].includes(state) ? researchData() : null}
      admission={null}
      submitting={false}
      notice={null}
      onRefresh={vi.fn()}
      onRun={vi.fn()}
    />);
    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "个股研究证据页" })).toHaveAttribute("data-state", state);
  });

  it.each(states)("renders brief %s as an explicit visible state", (state, label) => {
    render(<BriefPanel state={state} brief={["ready", "partial", "stale"].includes(state) ? briefData() : null} onRefresh={vi.fn()} />);
    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "每日 Agent 简报" })).toHaveAttribute("data-state", state);
  });

  it("places the daily brief on the research step and labels its evidence period in Chinese", () => {
    render(<BriefPanel state="ready" brief={briefData()} onRefresh={vi.fn()} />);

    expect(screen.getByText("每日研究简报")).toBeVisible();
    expect(screen.getByText("交易日")).toBeVisible();
    expect(screen.getByText("研究证据").closest("li")).toHaveAttribute("aria-current", "step");
  });

  it("shows SDK subscription usage without claiming API spend or enabling execution", () => {
    render(<ResearchPanel state="partial" research={researchData({
      billing_mode: "chatgpt_subscription", input_tokens: 300, output_tokens: 80,
    })} admission={null} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByText(/模型来源：Codex SDK/)).toBeVisible();
    expect(screen.getByText(/300 \/ 80/)).toBeVisible();
    expect(screen.getByText(/交易保持锁定/)).toBeVisible();
  });

  it("does not present partial SDK usage as an exact total", () => {
    render(<ResearchPanel state="partial" research={researchData({
      status: "failed", intent: null, billing_mode: "chatgpt_subscription",
      input_tokens: 300, output_tokens: 80, abstention_reason: "model_usage_uncertain",
      failed_stage_input: { stage: "critique", request_hash: "b".repeat(64), frozen_inputs_hash: "c".repeat(64) },
    })} admission={null} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByText(/仅为已知下限，不会自动重试/)).toBeVisible();
    fireEvent.click(screen.getByText("失败调用的输入记录"));
    expect(screen.getByText("阶段：反证")).toBeVisible();
  });

  it("renders stance, paired arguments, votes, invalidation and versions without a score or trade action", () => {
    render(<ResearchPanel state="ready" research={researchData()} admission={researchAdmissionData()} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);

    expect(screen.getByText("时点冻结研究")).toBeVisible();
    expect(screen.getByText("研究证据").closest("li")).toHaveAttribute("aria-current", "step");
    expect(screen.getByRole("heading", { name: "委员会决策记录" })).toBeVisible();
    expect(screen.getByText("保持观察", { exact: true })).toBeVisible();
    expect(screen.getByRole("heading", { name: "支持论点" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "反方论点" })).toBeVisible();
    expect(screen.getByText(/空头角色认为增长放缓/)).toBeVisible();
    expect(screen.getByTitle("revenue_growth_ttm")).toBeVisible();
    expect(screen.getByText("committee-v1", { exact: true })).toBeVisible();
    expect(document.body).not.toHaveTextContent(/\d+\s*\/\s*100|置信度/);
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
  });

  it("keeps cached partial/stale evidence visible and disables research creation", () => {
    for (const state of ["partial", "stale"] as const) {
      const { unmount } = render(<ResearchPanel state={state} research={researchData()} admission={researchAdmissionData()} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
      expect(screen.getByRole("heading", { name: "委员会决策记录" })).toBeVisible();
      expect(screen.queryByRole("button", { name: "重新运行冻结研究" })).not.toBeInTheDocument();
      unmount();
    }
  });

  it("enables a first frozen research run only from an eligible admission", () => {
    const onRun = vi.fn();
    render(<ResearchPanel state="empty" research={null} admission={researchAdmissionData()} submitting={false} notice={null} onRefresh={vi.fn()} onRun={onRun} />);
    fireEvent.click(screen.getByRole("button", { name: "运行冻结研究" }));
    expect(onRun).toHaveBeenCalledOnce();
  });

  it("explains an ineligible admission and keeps research creation disabled", () => {
    render(<ResearchPanel
      state="empty"
      research={null}
      admission={researchAdmissionData({ eligible: false, reason: "data_snapshot_unavailable", data_snapshot_id: null })}
      submitting={false}
      notice={null}
      onRefresh={vi.fn()}
      onRun={vi.fn()}
    />);
    expect(screen.getByText(/该时点没有可冻结的数据快照/)).toBeVisible();
    expect(screen.queryByRole("button", { name: /运行冻结研究/ })).not.toBeInTheDocument();
  });

  it("suppresses a fundamental stance for 00656.HK and explains the HKEX evidence gap", () => {
    const hongKong = researchData({ intent: { ...researchData().intent, display_symbol: "00656.HK", stance: "watch", exposure_action: "none" } });
    render(<DecisionCard research={hongKong} />);
    expect(screen.getByText(/香港交易所财报证据尚未准入/)).toBeVisible();
    expect(screen.queryByText(/买入候选|减仓候选/)).not.toBeInTheDocument();
  });

  it.each([
    ["abstained", null, "missing_evidence"],
    ["failed", null, "model_failed"],
    ["abstained", "budget_exhausted", "budget_exhausted"],
  ] as const)("explains terminal %s recovery without enabling a strategy target", (status, budgetReason, reason) => {
    render(<ResearchPanel
      state="partial"
      research={researchData({ status, intent: null, role_votes: [], disagreement: null, invalidation_conditions: [], quality_status: null, budget_reason: budgetReason, abstention_reason: reason })}
      admission={researchAdmissionData()}
      submitting={false}
      notice={null}
      onRefresh={vi.fn()}
      onRun={vi.fn()}
    />);
    expect(screen.getByText(new RegExp(reason))).toBeVisible();
    expect(screen.getAllByText(/不能支持策略目标/).length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: "重新运行冻结研究" })).not.toBeInTheDocument();
  });

  it("shows complete evidence identity and returns focus after Escape", async () => {
    function Harness() {
      const [open, setOpen] = useState(false);
      const triggerRef = useRef<HTMLButtonElement>(null);
      return <>
        <button ref={triggerRef} type="button" onClick={() => setOpen(true)}>查看证据</button>
        <EvidenceDrawer evidence={evidence} excerpt="现金流质量稳定。" open={open} onClose={() => setOpen(false)} returnFocusRef={triggerRef} />
      </>;
    }
    render(<Harness />);
    const trigger = screen.getByRole("button", { name: "查看证据" });
    fireEvent.click(trigger);
    const dialog = screen.getByRole("dialog", { name: "证据原文与定位" });
    expect(dialog).toHaveTextContent("不可变证据");
    expect(dialog).toHaveTextContent("SEC");
    expect(dialog).toHaveTextContent(evidence.object_id);
    expect(dialog).toHaveTextContent(evidence.location);
    expect(dialog).toHaveTextContent("2026-08-25 02:30:00 北京时间");
    expect(dialog).toHaveTextContent(evidence.content_hash);
    expect(dialog).toHaveTextContent("现金流质量稳定。");
    fireEvent.keyDown(dialog, { key: "Escape" });
    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it("exposes every evidence reference attached to one claim", () => {
    const secondEvidence = { ...evidence, object_id: "0000320193-26-000002", content_hash: "b".repeat(64), location: `memory://sha256/${"b".repeat(64)}#item-8` };
    const current = researchData();
    render(<DecisionCard research={researchData({
      evidence: [evidence, secondEvidence],
      intent: {
        ...current.intent!,
        evidence_refs: [evidence, secondEvidence],
        theses: [{ ...current.intent!.theses[0], evidence_refs: [evidence, secondEvidence] }],
      },
    })} />);
    expect(screen.getByRole("button", { name: "查看证据定位 2" })).toBeVisible();
  });

  it("keeps pending research disabled and visibly busy", () => {
    const onRun = vi.fn();
    render(<ResearchPanel state="empty" research={null} admission={researchAdmissionData()} submitting notice={null} onRefresh={vi.fn()} onRun={onRun} />);
    const button = screen.getByRole("button", { name: "正在登记研究" });
    expect(button).toBeDisabled();
    expect(button).toHaveAttribute("aria-busy", "true");
    fireEvent.click(button);
    expect(onRun).not.toHaveBeenCalled();
  });

  it("closes an unmounted selection, restores its exact trigger and reopens safely", async () => {
    const current = researchData();
    const hostile = { ...evidence, location: 'https://untrusted.invalid/<script>not executable</script>' };
    render(<DecisionCard research={researchData({ intent: {
      ...current.intent!, theses: [{ text: "<img src=x onerror=alert(1)>", evidence_refs: [hostile] }],
    } })} />);
    const opener = screen.getAllByRole("button", { name: "查看证据定位" })[0];
    fireEvent.click(opener);
    expect(screen.getByRole("dialog")).toHaveTextContent(hostile.location);
    expect(screen.getByRole("dialog").querySelector("a, img, script")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "关闭证据抽屉" }));
    await waitFor(() => expect(opener).toHaveFocus());
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    fireEvent.click(opener);
    expect(screen.getByRole("dialog")).toHaveTextContent("<img src=x onerror=alert(1)>");
    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
    await waitFor(() => expect(opener).toHaveFocus());
  });
});

describe("saved experimental research stages", () => {
  it("shows critique lineage and retained stages after a terminal failure", () => {
    render(<DecisionCard research={researchData({
      status: "failed", intent: null, abstention_reason: "model_usage_uncertain",
      stage_trace: { evidence_hash: "a".repeat(64), status: "verified", stages: [
        { stage: "research", status: "complete", input_tokens: 100, output_tokens: 50, roles: [] },
        { stage: "critique", status: "partial", input_tokens: 80, output_tokens: 30, roles: [
          { role: "bear", claims: [{ text: "缺少价格，无法验证估值。", evidence_refs: [evidence] }], reviewed_claim_ids: ["bull:0"] },
        ] },
        { stage: "synthesis", status: "unavailable", input_tokens: 0, output_tokens: 0, roles: [] },
      ] },
    })} />);
    fireEvent.click(screen.getByText("反证 · 仅保存部分输出"));
    expect(screen.getByText("回应论点：多头 #1")).toBeVisible();
    expect(screen.getByText("缺少价格，无法验证估值。")).toBeVisible();
    expect(screen.getByText("综合 · 未保存有效输出")).toBeVisible();
    expect(screen.queryByRole("button", { name: /运行|下单/ })).not.toBeInTheDocument();
  });

  it("does not render invalid stage claims", () => {
    render(<DecisionCard research={researchData({ stage_trace: {
      evidence_hash: "a".repeat(64), status: "invalid", stages: [],
    } })} />);
    expect(screen.getByText("阶段记录校验失败，暂不展示论点。")).toBeVisible();
  });
});
