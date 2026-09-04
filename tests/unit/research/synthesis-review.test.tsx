import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { DecisionCard } from "../../../app/components/decision-card";
import { evidence, researchData, synthesisResearchData } from "./fixtures";

function openReviews(): HTMLElement {
  const toggle = screen.getByText("逐项审查（模型自评）");
  const details = toggle.closest("details")!;
  expect(details).not.toHaveAttribute("open");
  fireEvent.click(toggle);
  return details;
}

describe("saved synthesis self-assessment", () => {
  it("distinguishes model sufficiency and resolution from system readiness", () => {
    const research = synthesisResearchData();
    const original = JSON.stringify(research);
    render(<DecisionCard research={research} />);
    const details = openReviews();
    expect(within(details).getByText(/不是独立核验结论/)).toBeVisible();
    expect(within(details).getByText("模型自评的审查充分性：充分")).toBeVisible();
    expect(within(details).getAllByText("模型自评：已解决")).toHaveLength(2);
    expect(within(details).getAllByText("模型未列出缺失证据（不等于没有缺口）")).toHaveLength(2);
    expect(within(details).getAllByText("模型未列出受阻结论")).toHaveLength(2);
    expect(within(details).getByText("估值依据仍未验证。")).toBeVisible();
    expect(document.querySelector(".decision-card__badges")).toHaveTextContent("资料不完整");
    expect(screen.getByText("仅观察")).toBeVisible();
    expect(screen.getByText(/交易保持锁定/)).toBeVisible();
    expect(details.querySelector('[data-state="ready"], [data-state="approved"]')).toBeNull();
    expect(screen.queryByRole("button", { name: /运行|下单|买入|卖出/ })).not.toBeInTheDocument();
    expect(JSON.stringify(research)).toBe(original);
  });

  it.each([
    ["accepted", "认可异议"], ["rejected", "不认可异议"], ["unresolved", "仍有疑问"],
  ] as const)("shows %s and the unresolved evidence without changing saved text", (status, label) => {
    render(<DecisionCard research={synthesisResearchData({ review_conclusion: "limited", reviews: [{
      claim_id: "bear:0", status, resolved: false, rationale: "合成理由：尚缺有效价格。",
      missing_evidence: ["合格价格", "流动性资料"], prevented_conclusion: "不能完成方案比较。", evidence_refs: [evidence],
    }, { claim_id: "evidence_audit:0", status: "accepted", resolved: true, rationale: "其他合成核对。",
      missing_evidence: [], prevented_conclusion: null, evidence_refs: [evidence] }] })} />);
    const details = openReviews();
    const review = within(details).getByRole("region", { name: "空头 #1 审查" });
    expect(within(review).getByText(`模型态度：${label}`)).toBeVisible();
    expect(within(review).getByText("模型自评：未解决")).toBeVisible();
    for (const text of ["合成理由：尚缺有效价格。", "合格价格", "流动性资料", "不能完成方案比较。"]) {
      expect(within(review).getByText(text)).toBeVisible();
    }
    expect(within(details).getByText("模型自评的审查充分性：有限")).toBeVisible();
  });

  it("uses the existing evidence drawer for inert review prose and restores focus", async () => {
    const rationale = '<img src=x onerror="alert(1)">';
    render(<DecisionCard research={synthesisResearchData({ review_conclusion: "insufficient", reviews: [
      { claim_id: "bear:0", status: "unresolved", resolved: false, rationale,
        missing_evidence: ["资料不足"], prevented_conclusion: "尚不能判断。", evidence_refs: [evidence] },
      { claim_id: "evidence_audit:0", status: "unresolved", resolved: false, rationale: "仍需来源核对。",
        missing_evidence: ["合格来源"], prevented_conclusion: "不能验证。", evidence_refs: [evidence] },
    ] })} />);
    const details = openReviews();
    expect(within(details).getByText("模型自评的审查充分性：不足")).toBeVisible();
    expect(within(details).getByText(rationale)).toBeVisible();
    expect(details.querySelector("img, script, iframe")).toBeNull();
    const opener = within(details).getAllByRole("button", { name: "查看证据定位" })[0];
    fireEvent.click(opener);
    const drawer = screen.getByRole("dialog");
    expect(drawer).toHaveTextContent(rationale);
    expect(drawer).toHaveTextContent(evidence.content_hash);
    expect(drawer.querySelector("img, script, iframe")).toBeNull();
    fireEvent.keyDown(drawer, { key: "Escape" });
    await waitFor(() => expect(opener).toHaveFocus());
  });

  it.each(["legacy", "invalid", "missing"])("does not display unverified or absent assessment for %s", (kind) => {
    const research = kind === "legacy" ? researchData() : synthesisResearchData();
    if (research.stage_trace && kind === "invalid") research.stage_trace.status = "invalid";
    if (research.stage_trace && kind === "missing") delete research.stage_trace.stages[0].roles[0].synthesis_assessment;
    render(<DecisionCard research={research} />);
    expect(screen.queryByText("逐项审查（模型自评）")).not.toBeInTheDocument();
    expect(screen.queryByText("模型自评的审查充分性：充分")).not.toBeInTheDocument();
  });
});
