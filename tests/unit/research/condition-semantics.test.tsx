import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DecisionCard } from "../../../app/components/decision-card";
import { researchData } from "./fixtures";

describe("research conditions are proposals, not evaluated results", () => {
  it.each(["v1", "v2", "v3"])("keeps %s conditions unverified even with references and a complete intent", (version) => {
    const base = researchData();
    const condition = { ...base.invalidation_conditions[0], signal: "committee.evidence_valid", operator: "is_false" as const, threshold: true, unit: "boolean" };
    render(<DecisionCard research={researchData({
      invalidation_conditions: [condition], intent: { ...base.intent!, prompt_version: `committee-${version}`, invalidation_conditions: [condition] },
    })} />);
    const region = screen.getByRole("heading", { name: "失效条件" }).closest("section")!;
    expect(region).toHaveTextContent("研究提出的待核验条件");
    expect(region).toHaveTextContent("这些失效条件尚无系统求值结果");
    expect(region).toHaveTextContent("不代表进入、退出或失效条件已经成立");
    expect(region).toHaveTextContent("committee.evidence_valid");
    expect(region).toHaveTextContent("运算符 is_false · 阈值 true");
    expect(region).not.toHaveTextContent("is_false 是");
    fireEvent.click(within(region).getByRole("button", { name: "查看证据定位" }));
    const dialog = screen.getByRole("dialog");
    expect(dialog).toHaveTextContent("待核验条件");
    expect(dialog).toHaveTextContent("不是求值结果");
    expect(dialog).toHaveTextContent("committee.evidence_valid 运算符 is_false · 阈值 true");
    expect(dialog).toHaveTextContent(condition.evidence_refs[0].content_hash);
  });

  it("preserves a false boolean threshold without rendering it as an observed false state", () => {
    const condition = { ...researchData().invalidation_conditions[0], signal: "committee.risk_triggered", operator: "is_true" as const, threshold: false, unit: "boolean" };
    render(<DecisionCard research={researchData({ invalidation_conditions: [condition] })} />);
    const region = screen.getByRole("heading", { name: "失效条件" }).closest("section")!;
    expect(region).toHaveTextContent("运算符 is_true · 阈值 false");
    expect(region).not.toHaveTextContent("is_true 否");
  });

  it("does not interpret an empty list as absence of invalidation risk", () => {
    render(<DecisionCard research={researchData({ invalidation_conditions: [] })} />);
    const region = screen.getByRole("heading", { name: "失效条件" }).closest("section")!;
    expect(region).toHaveTextContent("研究未提供失效条件");
    expect(region).toHaveTextContent("不代表不存在失效风险");
    expect(within(region).queryByRole("list")).not.toBeInTheDocument();
    expect(within(region).queryByRole("button")).not.toBeInTheDocument();
  });

  it("does not invent conditions when no intent was produced", () => {
    render(<DecisionCard research={researchData({ intent: null, status: "failed", invalidation_conditions: [] })} />);
    expect(screen.queryByRole("heading", { name: "失效条件" })).not.toBeInTheDocument();
    expect(screen.getByText(/尚未形成可供后续分析的研究结论/)).toBeVisible();
  });
});
