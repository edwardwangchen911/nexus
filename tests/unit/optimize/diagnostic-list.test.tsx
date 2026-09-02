import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  DiagnosticList,
  OptimizationNeedPanel,
} from "../../../app/components/diagnostic-list";
import { diagnosticReport } from "./fixtures";

describe("portfolio optimization diagnostics", () => {
  it.each([
    ["BLOCKED", "当前无法判断"],
    ["RESEARCH_REQUIRED", "需要补充证据"],
    ["OPTIMIZATION_CANDIDATE", "值得比较优化方案"],
    ["NO_ACTION", "保持现状"],
  ] as const)("explains %s without a score", (need, label) => {
    render(<OptimizationNeedPanel report={diagnosticReport(need)} />);

    expect(screen.getByRole("heading", { name: label })).toBeVisible();
    expect(screen.queryByText(/\d+\s*\/\s*100/)).not.toBeInTheDocument();
  });

  it("renders current value, limit, server-computed headroom, and every unknown reason", () => {
    render(<DiagnosticList report={diagnosticReport()} />);

    const concentration = screen.getByRole("listitem", { name: /CONCENTRATION_TOP1/ });
    expect(concentration).toHaveTextContent("当前值");
    expect(concentration).toHaveTextContent("0.2 ratio");
    expect(concentration).toHaveTextContent("阈值");
    expect(concentration).toHaveTextContent("0.15");
    expect(concentration).toHaveTextContent("余量");
    expect(concentration).toHaveTextContent("-0.05");
    for (const reason of [
      "ADV20_MISSING",
      "DRAWDOWN_252_COVERAGE_INSUFFICIENT",
      "SOURCE_CONFLICT",
    ]) {
      expect(screen.getByText(reason)).toBeVisible();
    }
  });
});
