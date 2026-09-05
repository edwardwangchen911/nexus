import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ResearchInputs } from "../../../app/components/research-inputs";
import { researchAdmissionData, researchAdmissionEnvelope, fundObservationInputs, instrumentId, decisionTime } from "./fixtures";
import type { components } from "../../../app/lib/generated/schema";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";

function inputs() {
  return {
    ...fundObservationInputs(), issuer_fund_observations: [], facts: [], total_facts: 0,
    filings: [{
      evidence_ref_id: "0198d600-1000-7000-8000-000000001581", source: "sec" as const,
      form: "8-K", accession_number: "0000000042-26-000001", revision: "synthetic-v2",
      accepted_at: "2026-09-02T20:00:00Z", received_at: "2026-09-05T12:00:00Z",
      content_hash: "a".repeat(64), disclosure_excerpt: "Parent announces results in exhibit.",
      disclosure_truncated: false,
      source_url: "https://www.sec.gov/Archives/edgar/data/1/000000004226000001/parent.htm",
      exhibit: {
        number: "99.1" as const, parent_href: "ex99.htm",
        source_url: "https://www.sec.gov/Archives/edgar/data/1/000000004226000001/ex99.htm",
        parent_received_at: "2026-09-05T11:00:00Z", parent_raw_content_hash: "b".repeat(64),
        received_at: "2026-09-05T12:00:00Z", raw_content_hash: "c".repeat(64),
        excerpt: "Quarter / cumulative · USD millions\n" + "Context. ".repeat(1500)
          + "GAAP reconciliation and operating cash flow. <img src=x onerror=alert(1)>",
        truncated: false as const, accounting_status: "unaudited" as const,
        filing_status: "furnished_not_filed" as const,
      },
    }],
  };
}

describe("separate SEC parent and earnings exhibit", () => {
  it("shows frozen quarter metrics through BFF without inventing currency or changing legacy evidence", async () => {
    const data = inputs();
    const quarter_metrics: components["schemas"]["ExhibitQuarterMetrics"] = {
      source_kind: "issuer_exhibit_table" as const,
      statement_title: "CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS — UNAUDITED" as const,
      accounting_basis: "gaap" as const, audit_status: "unaudited" as const,
      filing_status: "furnished_not_filed" as const, period_scope: "fiscal_quarter_ended" as const,
      current_period_end: "2026-08-02", comparable_period_end: "2025-08-03",
      current_column_label: "August 2, 2026", comparable_column_label: "August 3, 2025",
      original_unit_label: "(IN MILLIONS, EXCEPT PER SHARE DATA)" as const,
      raw_content_hash: "c".repeat(64),
      metrics: [
        { metric: "net_revenue" as const, row_path: ["Net revenue"], current_value: "1234", comparable_value: "987", scale: 0, unit: "millions" as const, currency: null },
        { metric: "gaap_net_income" as const, row_path: ["Net income"], current_value: "-12.3456", comparable_value: "0", scale: 4, unit: "millions" as const, currency: null },
        { metric: "gaap_diluted_eps" as const, row_path: ["Net income per share:", "Diluted"], current_value: "0.00001", comparable_value: "0.98", scale: 5, unit: "per_share" as const, currency: null },
      ],
    };
    const projectedInputs = { ...data, filings: data.filings.map((f) => ({ ...f, exhibit: { ...f.exhibit, quarter_metrics } })) };
    const response = await sanitizeEngineResponse(
      Response.json(researchAdmissionEnvelope(researchAdmissionData({ inputs: projectedInputs }))),
      `https://nexus.test/api/v1/instruments/${instrumentId}/research-admission?as_of=${encodeURIComponent(decisionTime)}`,
    );
    expect(response.status).toBe(200);
    const envelope = await response.json();
    expect(envelope.data.inputs.filings[0].exhibit.quarter_metrics).toEqual(quarter_metrics);
    render(<ResearchInputs inputs={envelope.data.inputs} asOf="2026-09-05T13:00:00Z" />);
    const table = screen.getByRole("table", { name: "附件季度经营指标" });
    expect(table).toHaveTextContent("2026-08-02");
    expect(table).toHaveTextContent("2025-08-03");
    expect(table).toHaveTextContent("GAAP 净利润");
    expect(table).toHaveTextContent("-12.346");
    expect(table).toHaveTextContent("币种未确认");
    expect(table).not.toHaveTextContent("USD");
    expect(within(table).getByText("<0.001")).toBeInTheDocument();
    expect(screen.getByText(/仅核对附件中的三项季度指标/)).toBeVisible();
  });

  it("shows complete late tables, distinct sources and receipt times as inert text", () => {
    const data = inputs();
    const { container } = render(<ResearchInputs inputs={data} asOf="2026-09-05T13:00:00Z" />);
    const toggle = screen.getByText("查看业绩附件 99.1（完整文本）");
    fireEvent.click(toggle);
    const details = toggle.closest("details")!;
    expect(details).toHaveTextContent("GAAP reconciliation and operating cash flow.");
    expect(details).toHaveTextContent("Quarter / cumulative · USD millions");
    expect(within(details).getByRole("link", { name: "查看 SEC 附件原件" }))
      .toHaveAttribute("href", data.filings[0].exhibit.source_url);
    expect(screen.getByText(/未经审计/)).toBeVisible();
    expect(screen.getByText("父文件收到时间")).toBeVisible();
    expect(screen.getByText("附件收到时间")).toBeVisible();
    expect(container.querySelector("img, script, iframe")).toBeNull();
    expect(screen.queryByText("财务数值（最多 3 位小数）")).not.toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "附件季度经营指标" })).not.toBeInTheDocument();
  });

  it("keeps legacy parent-only material without inventing an exhibit", () => {
    const data = inputs();
    render(<ResearchInputs inputs={{ ...data, filings: data.filings.map((f) => ({ ...f, exhibit: undefined })) }} asOf="2026-09-05T13:00:00Z" />);
    expect(screen.queryByText(/查看业绩附件/)).not.toBeInTheDocument();
    expect(screen.getByText("查看冻结披露正文")).toBeVisible();
  });

  it("does not present an unavailable 8-K bundle as read evidence", () => {
    const data = inputs();
    render(<ResearchInputs inputs={{ ...data, filings: data.filings.map((f) => ({ ...f, exhibit: undefined, disclosure_excerpt: null })) }} asOf="2026-09-05T13:00:00Z" />);
    expect(screen.getByText("披露正文或附件暂不可用，不作为已读取的文本证据。")).toBeVisible();
    expect(screen.queryByText(/查看业绩附件/)).not.toBeInTheDocument();
  });
});
