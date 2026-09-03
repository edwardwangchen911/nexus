import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  DiagnosticList,
  OptimizationNeedPanel,
} from "../../../app/components/diagnostic-list";
import { diagnosticReport } from "./fixtures";

describe("portfolio optimization diagnostics", () => {
  it("does not turn no diagnostic trigger into an economic recommendation", () => {
    render(<OptimizationNeedPanel report={diagnosticReport("NO_ACTION")} />);
    expect(screen.getByText(/该状态不代表已完成成本收益比较，也不代表保持现状更优/)).toBeVisible();
    expect(screen.queryByText(/尚未比较/)).not.toBeInTheDocument();
    expect(screen.getByText(/不代表保持现状更优/)).toBeVisible();
  });
  it("labels currency scope and trading-day windows without changing thresholds", () => {
    const report = diagnosticReport();
    report.items = [{ ...report.items[0], diagnostic_code: "VOLATILITY_63", policy_scope: "currency:USD", unit: "ratio", metric_value: "0.123456789", reference_limit: "0.15", headroom: "0.026543211", unknown_reason: "VOLATILITY_63_COVERAGE_INSUFFICIENT", headroom_reason: null }];
    render(<DiagnosticList report={report} />);
    expect(screen.getByRole("heading", { name: "63 个交易日年化波动率" })).toBeVisible();
    const item = screen.getByRole("listitem");
    expect(item).toHaveTextContent("币种范围：USD");
    expect(item).toHaveTextContent("63 个交易日窗口内有效价格覆盖不足");
    expect(item).toHaveTextContent("12.346%");
    expect(item).toHaveTextContent("2.654 个百分点");
    expect(report.items[0].metric_value).toBe("0.123456789");
  });
  it.each([
    ["BLOCKED", "当前无法判断"],
    ["RESEARCH_REQUIRED", "需要补充证据"],
    ["OPTIMIZATION_CANDIDATE", "值得比较优化方案"],
    ["NO_ACTION", "未触发优化条件"],
  ] as const)("explains %s without a score", (need, label) => {
    render(<OptimizationNeedPanel report={diagnosticReport(need)} />);

    expect(screen.getByRole("heading", { name: label })).toBeVisible();
    expect(screen.queryByText(/\d+\s*\/\s*100/)).not.toBeInTheDocument();
  });

  it("renders current value, limit, server-computed headroom, and every unknown reason", () => {
    render(<DiagnosticList report={diagnosticReport()} />);

    const concentration = screen.getByRole("listitem", { name: /CONCENTRATION_TOP1/ });
    expect(concentration).toHaveTextContent("当前值");
    expect(concentration).toHaveTextContent("20%");
    expect(concentration).toHaveTextContent("阈值");
    expect(concentration).toHaveTextContent("15%");
    expect(concentration).toHaveTextContent("余量");
    expect(concentration).toHaveTextContent("-5 个百分点");
    for (const reason of [
      "ADV20_MISSING",
      "DRAWDOWN_252_COVERAGE_INSUFFICIENT",
      "SOURCE_CONFLICT",
    ]) {
      expect(screen.getByText(reason)).toBeVisible();
    }
  });
});
