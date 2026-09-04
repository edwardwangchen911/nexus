import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { DecisionCard } from "../../../app/components/decision-card";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { researchData, researchAdmissionData, researchEnvelope, runId } from "./fixtures";
import { providerInputs } from "./provider-fixtures";
import { concentrationResearch } from "./concentration-fixtures";

describe("saved portfolio warning observations", () => {
  it("renders preserved results from the real BFF response validator", async () => {
    const data = concentrationResearch();
    const response = await sanitizeEngineResponse(Response.json(researchEnvelope(data)), undefined, `/internal/v1/research-runs/${runId}`);
    expect(response.status).toBe(200);
    const envelope = await response.json();
    expect(envelope.data.intent.condition_evaluations).toEqual(data.intent!.condition_evaluations);
    render(<DecisionCard research={envelope.data} />);
    expect(screen.getByRole("region", { name: "组合预警核验" })).toHaveTextContent("已触发");
  });
  it("separates supplied true and false results from unevaluated invalidation expressions", () => {
    render(<DecisionCard research={concentrationResearch()} />);
    const region = screen.getByRole("region", { name: "组合预警核验" });
    expect(within(region).getByRole("region", { name: "单一持仓预警" })).toHaveTextContent("已触发");
    expect(within(region).getByRole("region", { name: "单一持仓严重预警" })).toHaveTextContent("未触发");
    expect(region).toHaveTextContent("同币种证券");
    expect(region).toHaveTextContent("不含现金");
    expect(region).toHaveTextContent("不是当前实时状态");
    expect(region).toHaveTextContent("2026-08-25 00:00:00 北京时间");
    expect(region).toHaveTextContent("不代表研究充分");
    expect(region).not.toHaveTextContent("已通过");
    const conditions = screen.getByRole("heading", { name: "失效条件" }).closest("section")!;
    expect(conditions).toHaveTextContent("这些失效条件尚无系统求值结果");
    expect(within(conditions).queryByText("已触发")).not.toBeInTheDocument();
  });

  it("keeps context evidence and observed time with the result inside the original drawer", () => {
    render(<DecisionCard research={concentrationResearch()} />);
    const warning = screen.getByRole("region", { name: "单一持仓预警" });
    fireEvent.click(within(warning).getByRole("button", { name: "查看核验证据" }));
    const drawer = screen.getByRole("dialog");
    expect(drawer).toHaveTextContent("单一持仓预警：已触发");
    expect(drawer).toHaveTextContent("2026-08-25 00:00:00 北京时间");
    expect(drawer).toHaveTextContent("研究观察，不是买卖结论");
    expect(drawer).toHaveTextContent("d".repeat(64));
  });

  it("does not turn unknown into false or success", () => {
    const data = concentrationResearch("unknown");
    render(<DecisionCard research={data} />);
    const region = screen.getByRole("region", { name: "组合预警核验" });
    expect(within(region).getAllByText("无法核验", { exact: true })).toHaveLength(2);
    expect(region).toHaveTextContent("冻结集中度资料不足");
    expect(within(region).queryByText("未触发", { exact: true })).not.toBeInTheDocument();
  });

  it.each([undefined, null, []])("does not recompute old or absent results (%s)", (value) => {
    const base = researchData();
    render(<DecisionCard research={researchData({ intent: { ...base.intent!, condition_evaluations: value } })} />);
    expect(screen.queryByRole("region", { name: "组合预警核验" })).not.toBeInTheDocument();
  });

  it("does not attach new admission materials or results to an old saved run", () => {
    const old = researchData();
    const admission = researchAdmissionData({ inputs: providerInputs(), data_snapshot_id: "0198d600-1000-7000-8000-000000008888" });
    const props = { state: "partial" as const, admission, submitting: false, notice: null, onRefresh: vi.fn(), onRun: vi.fn() };
    const view = render(<ResearchPanel {...props} research={concentrationResearch()} />);
    expect(screen.getByRole("region", { name: "组合预警核验" })).toHaveTextContent("已触发");
    view.rerender(<ResearchPanel {...props} research={old} />);
    expect(screen.queryByRole("region", { name: "组合预警核验" })).not.toBeInTheDocument();
    expect(screen.getByText(/当前资料与下方研究使用不同冻结快照/)).toBeVisible();
  });
});
