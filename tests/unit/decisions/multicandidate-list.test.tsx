import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DecisionList, type DecisionCycle } from "../../../app/components/decisions/decision-list";
import { agentPaperCycle } from "../paper/fixtures";

function mixed(): DecisionCycle {
  return { ...structuredClone(agentPaperCycle), status: "completed", candidate_count: 3,
    candidates: [
      { ...agentPaperCycle.candidates[0], symbol: "AAPL", research_status: "completed", stance: "buy_candidate" },
      { ...agentPaperCycle.candidates[0], instrument_id: "second", symbol: "NVDA", research_status: "completed", stance: "reduce_candidate" },
      { ...agentPaperCycle.candidates[0], instrument_id: "third", symbol: "AVGO", research_status: "completed", stance: "watch" },
    ] };
}

describe("multi-candidate decision list", () => {
  it("names the portfolio cycle and displays every candidate's research opinion", () => {
    render(<DecisionList cycles={[mixed()]} />);
    const list = screen.getByRole("list", { name: "决策记录" });
    expect(within(list).getByText("组合周期 · 3 个候选")).toBeVisible();
    for (const text of ["AAPL · 买入", "NVDA · 减仓", "AVGO · 观察"]) {
      expect(within(list).getByText(text)).toBeVisible();
    }
    expect(screen.getByText("筛选匹配本周期任一候选的研究观点；组合取舍、目标与执行结果请查看详情。")).toBeVisible();
  });
  it.each(["buy", "sell", "watch"])("matches %s on any candidate, then composes with cycle status", (filter) => {
    render(<DecisionList cycles={[mixed()]} />);
    fireEvent.change(screen.getByLabelText("决策类型"), { target: { value: filter } });
    expect(screen.getAllByRole("link", { name: /查看决策/ })).toHaveLength(1);
    fireEvent.change(screen.getByLabelText("最终状态"), { target: { value: "failed" } });
    expect(screen.queryByRole("list", { name: "决策记录" })).not.toBeInTheDocument();
  });
  it("does not invent a hold opinion for a mixed cycle", () => {
    render(<DecisionList cycles={[mixed()]} />);
    fireEvent.change(screen.getByLabelText("决策类型"), { target: { value: "hold" } });
    expect(screen.queryByRole("list", { name: "决策记录" })).not.toBeInTheDocument();
  });
  it.each(["failed", "unavailable"] as const)("retains the cycle-level %s filter precedence", (status) => {
    const item = { ...mixed(), status, result_code: status === "failed" ? "AGENT_DECISION_CONFLICT" : "AGENT_PAPER_CALENDAR_UNAVAILABLE" };
    render(<DecisionList cycles={[item]} />);
    fireEvent.change(screen.getByLabelText("决策类型"), { target: { value: "buy" } });
    expect(screen.queryByRole("list", { name: "决策记录" })).not.toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("决策类型"), { target: { value: status === "failed" ? "model_failed" : "data_insufficient" } });
    expect(screen.getAllByRole("link", { name: /查看决策/ })).toHaveLength(1);
  });
  it("keeps single-candidate and empty-candidate records readable", () => {
    render(<DecisionList cycles={[agentPaperCycle, { ...mixed(), cycle_id: "empty", candidate_count: 0, candidates: [] }]} />);
    expect(screen.getByText(agentPaperCycle.candidates[0].symbol)).toBeVisible();
    expect(screen.getByText("无候选标的")).toBeVisible();
    fireEvent.change(screen.getByLabelText("决策类型"), { target: { value: "other" } });
    expect(screen.getAllByRole("link", { name: /查看决策/ })).toHaveLength(1);
  });
});
