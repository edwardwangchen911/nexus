import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ResearchInputs } from "../../../app/components/research-inputs";
import { researchAdmissionData } from "./fixtures";

function inputs() {
  return {
    ...researchAdmissionData().inputs!, facts: [], total_facts: 0,
    filings: [{
      evidence_ref_id: "00000000-0000-0000-0000-000000000001", source: "sec" as const,
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
