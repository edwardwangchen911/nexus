import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ProposalComparison } from "../../../app/components/proposal-comparison";
import { draftResearchContext, proposalGroup } from "./fixtures";
import { researchData } from "../research/fixtures";

describe("frozen draft research context", () => {
  it.each([undefined, null])("does not invent context for legacy %s", (research_context) => {
    render(<ProposalComparison group={proposalGroup({ research_context })} onStartShadow={vi.fn()} onReject={vi.fn()} />);
    expect(screen.queryByRole("region", { name: "冻结研究依据" })).not.toBeInTheDocument();
  });

  it.each([
    ["completed", "STAGED_ACTION_SEMANTICS_UNVERIFIED", "行动语义尚未通过校验"],
    ["failed", "RESEARCH_FAILED", "研究运行失败"],
    ["abstained", "RESEARCH_ABSTAINED", "研究已弃权"],
  ] as const)("shows exact saved %s context without actions or completion claims", (run_status, unavailable_reason, message) => {
    const context = { ...draftResearchContext(), run_status, unavailable_reason };
    const start = vi.fn();
    render(<ProposalComparison group={proposalGroup({ research_context: context })} onStartShadow={start} onReject={vi.fn()} />);
    const region = screen.getByRole("region", { name: "冻结研究依据" });
    expect(region).toHaveTextContent(message);
    expect(region).toHaveTextContent("仅辅助阅读，不参与本草稿目标权重计算");
    expect(region).toHaveTextContent("2026-08-24 08:00:00 北京时间");
    expect(region).toHaveTextContent("2026-08-25 08:10:00 北京时间");
    expect(within(region).getByText(context.output_hash).closest("details")).not.toHaveAttribute("open");
    expect(within(region).queryByRole("button")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "启动影子验证" })).not.toBeInTheDocument();
    expect(start).not.toHaveBeenCalled();
  });

  it("shows saved claims and unknowns as inert text, never current live research", () => {
    const intent = researchData().intent!;
    const marker = '<img src=x onerror="alert(1)">';
    intent.theses = [{ text: marker, evidence_refs: intent.evidence_refs }];
    intent.uncertainties = ["费用证据未知"];
    const { container } = render(<ProposalComparison group={proposalGroup({
      research_context: { ...draftResearchContext(), intent, unavailable_reason: null },
    })} onStartShadow={vi.fn()} onReject={vi.fn()} />);
    const region = screen.getByRole("region", { name: "冻结研究依据" });
    expect(region).toHaveTextContent(marker);
    expect(region).toHaveTextContent("费用证据未知");
    expect(region).toHaveTextContent("不会授予执行权限");
    expect(container.querySelector("img")).toBeNull();
    expect(within(region).queryByRole("link")).not.toBeInTheDocument();
  });
});
