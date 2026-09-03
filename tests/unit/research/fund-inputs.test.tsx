import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ResearchInputs } from "../../../app/components/research-inputs";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { decisionTime, fundObservationInputs, researchAdmissionData } from "./fixtures";

describe("frozen issuer classification and holdings inputs", () => {
  it("preserves original percent units, zero, negative weights, and unknown denominators", () => {
    render(<ResearchInputs inputs={fundObservationInputs()} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "发行商分类与持仓资料" });
    expect(screen.getByRole("heading", { name: "TEST · 已冻结发行商资料" })).toBeVisible();
    expect(region).toHaveTextContent("2026-08-20");
    expect(region).toHaveTextContent("权重分母未知");
    expect(region).toHaveTextContent("不把未列出部分视为现金");
    expect(region).toHaveTextContent("不用于历史回测或组合行业风险计算");
    fireEvent.click(within(region).getByText("查看发行商行业分类"));
    expect(within(region).getByRole("table", { name: "发行商行业分类原始权重" })).toHaveTextContent("100%");
    fireEvent.click(within(region).getByText("查看发行商持仓"));
    const holdings = within(region).getByRole("table", { name: "发行商持仓原始权重" });
    expect(holdings).toHaveTextContent("-0.126%");
    expect(holdings).toHaveTextContent("<0.001%");
    expect(within(holdings).getByRole("row", { name: /Zero holding/ })).toHaveTextContent("0%");
    expect(holdings).toHaveTextContent("未知");
    expect(region).not.toHaveTextContent("10000%");
    expect(region).not.toHaveTextContent(/\d+\.\d{4,}/);
  });

  it("shows independent receipt times and original source identity on demand", () => {
    const inputs = fundObservationInputs();
    render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "发行商分类与持仓资料" });
    fireEvent.click(within(region).getByText("分类与持仓证据来源"));
    expect(region).toHaveTextContent("2026-08-25 02:00:00 北京时间");
    expect(region).toHaveTextContent("2026-08-25 02:30:00 北京时间");
    expect(within(region).getByRole("link", { name: "查看发行商产品页" })).toHaveAttribute("href", inputs.issuer_fund_observations[0].html_url);
    expect(region).toHaveTextContent("b".repeat(64));
    expect(region).toHaveTextContent("c".repeat(64));
    expect(screen.queryByRole("button", { name: /运行|交易|下单/ })).not.toBeInTheDocument();
  });

  it("keeps truncation and untrusted public labels visible as plain text", () => {
    const inputs = fundObservationInputs();
    const observation = inputs.issuer_fund_observations[0];
    observation.truncated = true;
    observation.total_holdings = 101;
    observation.industries[0].label = "<script>unsafe()</script>";
    observation.holdings[0].name = "<img src=x onerror=unsafe()>";
    const { container } = render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    expect(screen.getByText(/显示 3 \/ 101 项持仓/)).toBeVisible();
    fireEvent.click(screen.getByText("查看发行商行业分类"));
    fireEvent.click(screen.getByText("查看发行商持仓"));
    expect(screen.getByText("<script>unsafe()</script>")).toBeVisible();
    expect(screen.getByText("<img src=x onerror=unsafe()>")).toBeVisible();
    expect(container.querySelector("script, img, iframe")).toBeNull();
  });

  it("does not describe fund-only inputs as company financial reports or completed research", () => {
    render(<ResearchPanel state="empty" research={null} admission={researchAdmissionData({
      inputs: fundObservationInputs(), eligible: false, reason: "prompt_authority_unavailable",
    })} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByText("资料已冻结 · 研究待生成")).toBeVisible();
    expect(screen.getByText(/发行商分类与持仓资料已就绪，AI 研究尚未生成/)).toBeVisible();
    expect(screen.queryByText(/财报资料已就绪/)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /运行冻结研究/ })).not.toBeInTheDocument();
  });

  it("keeps old summaries without the optional field readable", () => {
    const { issuer_fund_observations: omitted, ...old } = fundObservationInputs();
    void omitted;
    render(<ResearchInputs inputs={old as ReturnType<typeof fundObservationInputs>} asOf={decisionTime} />);
    expect(screen.queryByRole("region", { name: "发行商分类与持仓资料" })).not.toBeInTheDocument();
  });
});
