import { it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { ResearchInputs } from "../../../app/components/research-inputs";

it("shows the unresolved amendment boundary even when the main text is not truncated", () => {
  render(<ResearchInputs asOf="2026-09-02T06:00:00Z" inputs={{
    display_symbol: "TEST", total_facts: 0, truncated: false, facts: [],
    issuer_distributions: [], issuer_fund_observations: [], issuer_nav_histories: [],
    corporate_action_observations: [], company_profiles: [], macro_observations: [],
    current_price_trends: [], news_observations: [],
    filings: [{ evidence_ref_id: "amendment-1", source: "sec", form: "8-K/A",
      accession_number: "0000000001-26-000002", accepted_at: "2026-09-01T20:00:00Z",
      received_at: "2026-09-02T05:59:00Z", revision: "a".repeat(64), content_hash: "a".repeat(64),
      disclosure_excerpt: "Limited amendment text for investors.", disclosure_truncated: false,
    }],
  }} />);
  expect(screen.getByText(/原件关联尚未核实/)).toBeVisible();
  expect(screen.getByText(/附件尚未采集/)).toBeVisible();
  expect(screen.getByText(/相关财务数值是否需要更正仍待核验/)).toBeVisible();
  expect(screen.getByText(/不能将它当作完整替代文件或执行许可/)).toBeVisible();
});
