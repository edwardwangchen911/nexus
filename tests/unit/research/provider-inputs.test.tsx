import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ResearchInputs } from "../../../app/components/research-inputs";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { decisionTime, researchAdmissionData, fundObservationInputs } from "./fixtures";
import { companyProfile, macroObservation, providerInputs } from "./provider-fixtures";

describe("frozen company and macro materials", () => {
  it("names actual providers without calling background-only inputs SEC financial reports", () => {
    render(<ResearchInputs inputs={providerInputs()} asOf={decisionTime} />);
    expect(screen.getByRole("heading", { name: "TEST · 已冻结研究资料" })).toBeVisible();
    expect(screen.getByText("FMP / FRED · 本地只读证据")).toBeVisible();
    expect(screen.queryByText("SEC 资料")).not.toBeInTheDocument();
    expect(screen.queryByText("财务事实")).not.toBeInTheDocument();
  });

  it("keeps company labels, unknown times and qualifications distinct from verified financial facts", () => {
    render(<ResearchInputs inputs={providerInputs()} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "公司背景资料" });
    expect(region).toHaveTextContent("Synthetic Company");
    expect(region).toHaveTextContent("不是 SEC 财务事实");
    expect(region).toHaveTextContent("不是已验证的组合风控分类");
    for (const label of ["来源发布时刻", "资料有效期", "行业分类体系"]) {
      expect(within(region).getByText(label).parentElement).toHaveTextContent("未知");
    }
    fireEvent.click(within(region).getByText("公司证据与来源"));
    expect(region).toHaveTextContent("本地内容哈希，不是来源修订编号");
    expect(region).toHaveTextContent("2026-08-25 01:00:00 北京时间");
    expect(region).toHaveTextContent("2026-08-25 02:00:00 北京时间");
    expect(region).toHaveTextContent(companyProfile().receipt_content_hash);
  });

  it.each([
    ["4.25", "Percent", "4.25%"], ["0", "Percent", "0%"],
    ["-0.123456", "Percent", "-0.123%"], ["0.0000001", "Percent", "<0.001%"],
    ["-0.0000001", "Percent", "(-0.001, 0)%"], ["123.456789", "Index 1982-1984=100", "123.457"],
  ])("preserves macro %s %s without ratio scaling or hidden small values", (value, unit, expected) => {
    const inputs = providerInputs(); inputs.macro_observations = [{ ...macroObservation(), value, unit }];
    render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "宏观观察资料" });
    expect(within(region).getByText(expected, { exact: true })).toBeVisible();
    expect(region).toHaveTextContent(unit);
    expect(region).not.toHaveTextContent("425%");
  });

  it("labels observation date, vintage, receipt and availability separately without historical eligibility", () => {
    render(<ResearchInputs inputs={providerInputs()} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "宏观观察资料" });
    expect(within(region).getByText("经济观察日期").parentElement).toHaveTextContent("2026-08-20");
    expect(within(region).getByText("历史版本日期（vintage）").parentElement).toHaveTextContent("2026-08-21");
    expect(within(region).getByText("本机收到时间").parentElement).toHaveTextContent("2026-08-25 01:30:00 北京时间");
    expect(within(region).getByText("证据可用时间").parentElement).toHaveTextContent("2026-08-25 02:30:00 北京时间");
    expect(region).toHaveTextContent("不是精确发布时刻");
    expect(region).toHaveTextContent("不可用于历史回测");
    fireEvent.click(within(region).getByText("DGS10 证据与日历绑定"));
    expect(region).toHaveTextContent(macroObservation().calendar_evidence_ref_id);
    expect(region).toHaveTextContent(macroObservation().raw_content_hash);
  });

  it("renders full company text as text only within an expandable disclosure", () => {
    const inputs = providerInputs();
    inputs.company_profiles[0].description = '<img src=x onerror="alert(1)"> ' + "长描述".repeat(300);
    render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "公司背景资料" });
    fireEvent.click(within(region).getByText("查看公司描述与标识"));
    expect(within(region).getByText(inputs.company_profiles[0].description)).toBeVisible();
    expect(region.querySelector("img, script")).toBeNull();
    expect(region).toHaveTextContent("0000000001");
  });

  it("keeps nullable company descriptions and identifiers explicitly unknown", () => {
    const inputs = providerInputs();
    inputs.company_profiles = [{ ...companyProfile(), company_name: null, description: null, cik: null, isin: null, cusip: null, sector: null, industry: null }];
    render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    const region = screen.getByRole("region", { name: "公司背景资料" });
    for (const label of ["公司名称", "来源行业标签", "来源板块标签"]) {
      expect(within(region).getByText(label).parentElement).toHaveTextContent("未知");
    }
    fireEvent.click(within(region).getByText("查看公司描述与标识"));
    expect(within(region).getByText("公司描述：未知")).toBeVisible();
    for (const label of ["CIK", "ISIN", "CUSIP"]) {
      expect(within(region).getByText(label).parentElement).toHaveTextContent("未知");
    }
  });

  it("keeps SEC and issuer materials visible alongside the new providers", () => {
    const inputs = { ...providerInputs(), issuer_fund_observations: fundObservationInputs().issuer_fund_observations,
      filings: [{ evidence_ref_id: "0198d600-1000-7000-8000-000000001595", source: "sec" as const, form: "10-K",
        accession_number: "0000000001-26-000001", accepted_at: decisionTime, received_at: decisionTime,
        revision: "f".repeat(64), content_hash: "f".repeat(64), disclosure_truncated: false }],
    };
    render(<ResearchInputs inputs={inputs} asOf={decisionTime} />);
    expect(screen.getByText("SEC / 发行商 / FMP / FRED · 本地只读证据")).toBeVisible();
    expect(screen.getByRole("heading", { name: "SEC 10-K" })).toBeVisible();
    expect(screen.getByRole("region", { name: "发行商分类与持仓资料" })).toBeVisible();
    expect(screen.getByRole("region", { name: "公司背景资料" })).toBeVisible();
    expect(screen.getByRole("region", { name: "宏观观察资料" })).toBeVisible();
  });

  it("keeps old responses without either optional field readable", () => {
    const { company_profiles, macro_observations, ...old } = fundObservationInputs();
    void company_profiles; void macro_observations;
    render(<ResearchInputs inputs={old as ReturnType<typeof fundObservationInputs>} asOf={decisionTime} />);
    expect(screen.getByRole("region", { name: "发行商分类与持仓资料" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "公司背景资料" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "宏观观察资料" })).not.toBeInTheDocument();
  });

  it.each(["company", "macro", "both"])("uses accurate page empty-state for %s inputs without offering execution", (kind) => {
    const inputs = providerInputs();
    if (kind === "company") inputs.macro_observations = [];
    if (kind === "macro") inputs.company_profiles = [];
    render(<ResearchPanel state="empty" research={null} admission={researchAdmissionData({ inputs, eligible: false, reason: "prompt_authority_unavailable" })}
      submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByText("资料已冻结 · 研究待生成")).toBeVisible();
    expect(screen.queryByText(/财报资料已就绪/)).not.toBeInTheDocument();
    expect(screen.getByText(/AI 研究尚未生成/)).toHaveTextContent("已冻结");
    expect(screen.queryByRole("button", { name: /运行冻结研究|买入|卖出|下单/ })).not.toBeInTheDocument();
  });
});
