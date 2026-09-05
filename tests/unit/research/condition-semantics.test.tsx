import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DecisionCard } from "../../../app/components/decision-card";
import { concentrationResearch } from "./concentration-fixtures";
import { researchData } from "./fixtures";

describe("research conditions are proposals, not evaluated results", () => {
  it.each(["v1", "v2", "v3"])("shows separate saved entry, exit and invalidation for %s", (version) => {
    const base = concentrationResearch();
    const template = base.invalidation_conditions[0];
    const entry = { ...template, signal: "committee.support_ready", operator: "is_true" as const, threshold: true, unit: "boolean" as const };
    const exit = { ...template, signal: "committee.risk_triggered", operator: "is_true" as const, threshold: false, unit: "boolean" as const };
    const invalidation = { ...template, signal: "committee.evidence_valid", operator: "is_false" as const, threshold: true, unit: "boolean" as const };
    const research = { ...base, invalidation_conditions: [invalidation], intent: { ...base.intent!, prompt_version: `committee-${version}`, entry_conditions: [entry], exit_conditions: [exit], invalidation_conditions: [entry] } };
    const original = JSON.stringify(research);
    render(<DecisionCard research={research} />);
    for (const [title, condition] of [["进入条件", entry], ["退出条件", exit], ["失效条件", invalidation]] as const) {
      const section = screen.getByRole("heading", { name: title }).closest("section")!;
      expect(section).toHaveTextContent("已保存的研究表达式（待核验）");
      expect(section).toHaveTextContent(`这些${title}尚无系统求值结果`);
      expect(section).toHaveTextContent(condition.signal);
      expect(section).toHaveTextContent(`运算符 ${condition.operator} · 阈值 ${String(condition.threshold)}`);
      for (const other of [entry, exit, invalidation].filter((value) => value !== condition)) expect(section).not.toHaveTextContent(other.signal);
      expect(section).not.toHaveTextContent("已触发");
      expect(section).not.toHaveTextContent("未触发");
    }
    expect(screen.getByRole("region", { name: "组合预警核验" })).toHaveTextContent("已触发");
    expect(screen.queryByRole("button", { name: /下单|买入|卖出|执行/ })).not.toBeInTheDocument();
    expect(JSON.stringify(research)).toBe(original);
  });

  it("shows missing entry and exit as unknown without permission or invented expressions", () => {
    render(<DecisionCard research={researchData()} />);
    for (const title of ["进入条件", "退出条件"]) {
      const section = screen.getByRole("heading", { name: title }).closest("section")!;
      expect(section).toHaveTextContent(`研究未提供${title}`);
      expect(section).toHaveTextContent("不代表条件已满足或获得执行许可");
      expect(within(section).queryByRole("list")).not.toBeInTheDocument();
    }
  });

  it.each(["entry_conditions", "exit_conditions"] as const)("keeps %s drawer expressions inert", (key) => {
    const base = researchData();
    const signal = '<img src=x onerror="alert(1)">';
    const condition = { ...base.invalidation_conditions[0], signal, operator: "is_true" as const, threshold: false, unit: "boolean" as const };
    render(<DecisionCard research={researchData({ intent: { ...base.intent!, [key]: [condition] } })} />);
    const section = screen.getByRole("heading", { name: key === "entry_conditions" ? "进入条件" : "退出条件" }).closest("section")!;
    expect(section).toHaveTextContent(signal);
    expect(section.querySelector("img, script")).toBeNull();
    fireEvent.click(within(section).getByRole("button", { name: "查看证据定位" }));
    const dialog = screen.getByRole("dialog");
    expect(dialog).toHaveTextContent("这是已保存的研究表达式，不是求值结果");
    expect(dialog).toHaveTextContent(signal);
    expect(dialog.querySelector("img, script")).toBeNull();
  });

  it.each(["v1", "v2", "v3"])("keeps %s conditions unverified even with references and a complete intent", (version) => {
    const base = researchData();
    const condition = { ...base.invalidation_conditions[0], signal: "committee.evidence_valid", operator: "is_false" as const, threshold: true, unit: "boolean" as const };
    render(<DecisionCard research={researchData({
      invalidation_conditions: [condition], intent: { ...base.intent!, prompt_version: `committee-${version}`, invalidation_conditions: [condition] },
    })} />);
    const region = screen.getByRole("heading", { name: "失效条件" }).closest("section")!;
    expect(region).toHaveTextContent("已保存的研究表达式（待核验）");
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
    const condition = { ...researchData().invalidation_conditions[0], signal: "committee.risk_triggered", operator: "is_true" as const, threshold: false, unit: "boolean" as const };
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
    for (const name of ["进入条件", "退出条件", "失效条件"]) expect(screen.queryByRole("heading", { name })).not.toBeInTheDocument();
    expect(screen.getByText(/尚未形成可供后续分析的研究结论/)).toBeVisible();
  });
});
