import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useRef, useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { BriefPanel } from "../../../app/(protected)/briefs/[sessionDate]/page";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { DecisionCard } from "../../../app/components/decision-card";
import { EvidenceDrawer } from "../../../app/components/evidence-drawer";
import type { DataStateKind } from "../../../app/components/data-state";
import { briefData, evidence, researchData } from "./fixtures";

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"], ["empty", "暂无数据"], ["ready", "数据已就绪"],
  ["partial", "数据不完整"], ["stale", "数据已过期"], ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

describe("research evidence docket", () => {
  it.each(states)("renders research %s as an explicit visible state", (state, label) => {
    render(<ResearchPanel
      state={state}
      research={["ready", "partial", "stale"].includes(state) ? researchData() : null}
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

  it("renders stance, paired arguments, votes, invalidation and versions without a score or trade action", () => {
    render(<DecisionCard research={researchData()} />);

    expect(screen.getByRole("heading", { name: "委员会决策记录" })).toBeVisible();
    expect(screen.getByText("保持观察", { exact: true })).toBeVisible();
    expect(screen.getByRole("heading", { name: "支持论点" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "反方论点" })).toBeVisible();
    expect(screen.getByText(/空头角色认为增长放缓/)).toBeVisible();
    expect(screen.getByText("revenue_growth_ttm", { exact: true })).toBeVisible();
    expect(screen.getByText("committee-v1", { exact: true })).toBeVisible();
    expect(document.body).not.toHaveTextContent(/\d+\s*\/\s*100|置信度/);
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
  });

  it("keeps cached partial/stale evidence visible and disables research creation", () => {
    for (const state of ["partial", "stale"] as const) {
      const { unmount } = render(<ResearchPanel state={state} research={researchData()} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
      expect(screen.getByRole("heading", { name: "委员会决策记录" })).toBeVisible();
      expect(screen.queryByRole("button", { name: "重新运行冻结研究" })).not.toBeInTheDocument();
      unmount();
    }
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
    expect(dialog).toHaveTextContent("SEC");
    expect(dialog).toHaveTextContent(evidence.object_id);
    expect(dialog).toHaveTextContent(evidence.location);
    expect(dialog).toHaveTextContent("2026-08-24 18:30:00 UTC");
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
});
