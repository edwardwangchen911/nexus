import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PortfolioDecisionDetail } from "../../../app/components/decisions/portfolio-decision-detail";
import { synthesisUsageDetail } from "../paper/synthesis-usage-fixture";

describe("historical synthesis usage disposition", () => {
  it.each([
    ["accepted", "历史未知已接受，预算保留"],
    ["not_accepted", "历史未知尚未接受，预算保留"],
    ["invalid", "历史接受证明失效，预算保留"],
  ] as const)("shows %s without changing the failed cycle into success", (status, label) => {
    const detail = structuredClone(synthesisUsageDetail);
    detail.synthesis_usage_disposition!.status = status;
    render(<PortfolioDecisionDetail detail={detail} />);
    const notice = screen.getByRole("note", { name: "组合用量历史处置" });
    expect(notice).toHaveTextContent("组合失败／用量未知／未执行");
    expect(notice).toHaveTextContent(label);
    expect(notice).toHaveTextContent("后续周期仍须独立通过准入校验");
    expect(screen.queryByRole("button", { name: /批准|运行|重试/ })).not.toBeInTheDocument();
    expect(notice).not.toHaveTextContent(/用量为零|恢复成功|新周期已获准/);
  });

  it("does not invent a disposition for existing historical detail", () => {
    const detail = structuredClone(synthesisUsageDetail);
    delete detail.synthesis_usage_disposition;
    render(<PortfolioDecisionDetail detail={detail} />);
    expect(screen.queryByRole("note", { name: "组合用量历史处置" })).not.toBeInTheDocument();
  });
});
