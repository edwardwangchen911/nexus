import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ResearchInputs } from "../../../app/components/research-inputs";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { decisionTime, navHistoryInputs, researchAdmissionData } from "./fixtures";

describe("frozen issuer NAV inputs", () => {
  it("separates per-share NAV, fund assets, zero and unknown without losing small values", () => {
    render(<ResearchInputs inputs={navHistoryInputs()} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "发行商净值历史资料" });
    expect(screen.getByRole("heading", { name: "TEST · 已冻结发行商资料" })).toBeVisible();
    expect(region).toHaveTextContent("显示最近 2 / 45 条");
    expect(region).toHaveTextContent("2026-07-10 至 2026-08-23");
    expect(region).toHaveTextContent("不是市场成交价格或总收益序列");
    expect(region).toHaveTextContent("不用于历史回测");
    expect(region).toHaveTextContent("可能包含非交易日");
    expect(region).toHaveTextContent("币种来自产品页基础币种");
    fireEvent.click(within(region).getByText("查看净值观察记录"));
    const table = within(region).getByRole("table", { name: "发行商净值原始观察" });
    expect(table).toHaveTextContent("每份净值（USD / 份）");
    expect(table).toHaveTextContent("基金净资产（USD）");
    expect(table).toHaveTextContent("12.346");
    expect(table).toHaveTextContent("<0.001");
    expect(within(table).getByRole("row", { name: /2026-08-22/ })).toHaveTextContent("未知");
    expect(within(table).getByRole("row", { name: /2026-08-23/ })).toHaveTextContent("0");
    for (const cell of within(table).getAllByRole("cell")) {
      expect(cell).not.toHaveTextContent(/\d+\.\d{4,}/);
    }
    fireEvent.click(within(region).getByText("净值证据与来源"));
    expect(region).toHaveTextContent("2026-08-25 02:00:00 北京时间");
    expect(region).toHaveTextContent("2026-08-25 02:30:00 北京时间");
    expect(region).toHaveTextContent("e".repeat(64));
    expect(region).toHaveTextContent("f".repeat(64));
    expect(within(region).getByRole("link", { name: "查看发行商净值原件" })).toHaveAttribute("href", navHistoryInputs().issuer_nav_histories[0].workbook_url);
  });

  it("does not call NAV-only documents financial reports or completed research", () => {
    render(<ResearchPanel state="empty" research={null} admission={researchAdmissionData({
      inputs: navHistoryInputs(), eligible: false, reason: "prompt_authority_unavailable",
    })} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByText(/发行商净值历史资料已就绪，AI 研究尚未生成/)).toBeVisible();
    expect(screen.queryByText(/财报资料已就绪/)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /运行冻结研究/ })).not.toBeInTheDocument();
  });

  it("keeps historical responses without the optional field readable", () => {
    const { issuer_nav_histories: omitted, ...old } = navHistoryInputs();
    void omitted;
    render(<ResearchInputs inputs={old as ReturnType<typeof navHistoryInputs>} asOf={decisionTime} />);
    expect(screen.queryByRole("region", { name: "发行商净值历史资料" })).not.toBeInTheDocument();
  });
});
