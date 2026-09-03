import { describe, expect, it } from "vitest";
import { formatDisplayTime, formatMetric, formatPercent, snapshotTimeLabel } from "../../../app/lib/display-format";

describe("consistent display semantics", () => {
  it("does not invent the meaning of an unknown snapshot timestamp", () => {
    expect(snapshotTimeLabel("gateway_observed")).toBe("本机读取时间");
    expect(snapshotTimeLabel("provider_event")).toBe("数据源时间");
    expect(snapshotTimeLabel("unknown")).toBe("快照记录时间（含义未确认）");
  });
  it("converts offset timestamps to Beijing time including midnight rollover", () => {
    expect(formatDisplayTime("2026-09-02T20:01:02.123456Z")).toBe("2026-09-03 04:01:02 北京时间");
    expect(formatDisplayTime("2026-09-02T16:01:02-04:00")).toBe("2026-09-03 04:01:02 北京时间");
    expect(formatDisplayTime("2026-09-03T04:01:02+08:00")).toBe("2026-09-03 04:01:02 北京时间");
  });
  it("does not invent a time or timezone for missing or invalid evidence", () => {
    expect(formatDisplayTime(null)).toBe("未知");
    expect(formatDisplayTime("2026-09-02T12:00:00")).toBe("时区未提供");
    expect(formatDisplayTime("invalid")).toBe("无效时间");
    expect(formatDisplayTime("2026-02-30T12:00:00Z")).toBe("无效时间");
  });
  it.each([["1", "100%"], ["0.1", "10%"], ["0.123456789", "12.346%"], ["0.0000001", "<0.001%"], ["-0.0000001", "(-0.001, 0)%"], ["9007199254740993.12345", "900719925474099312.345%"]])("scales %s before rounding", (value, expected) => {
    expect(formatPercent(value)).toBe(expected);
  });
  it("distinguishes percentages, percentage points, coefficients and scaled aggregates", () => {
    expect(formatMetric("0.2", "ratio", "CONCENTRATION_TOP1:USD")).toBe("20%");
    expect(formatMetric("-0.05", "ratio", "CONCENTRATION_TOP1", true)).toBe("-5 个百分点");
    expect(formatMetric("0.245678", "ratio", "CONCENTRATION_HHI")).toBe("0.246");
    expect(formatMetric("0.65", "ratio", "correlation")).toBe("0.65");
    expect(formatMetric("0.65", "ratio", "CORRELATION_CLUSTER")).toBe("65%");
    expect(formatMetric("1.2", "ratio", "BETA_126")).toBe("1.2");
    expect(formatMetric("0.123456", "annualized_ratio", "VOLATILITY_20")).toBe("12.346%");
    expect(formatMetric(2000, "basis_points", "top1")).toBe("20%");
    expect(formatMetric(6500, "basis_points", "correlation")).toBe("0.65");
    expect(formatMetric(245678, "parts_per_million", "hhi")).toBe("0.246");
    expect(formatMetric("1.23456", "trading_days")).toBe("1.235 个交易日");
    expect(formatPercent(null)).toBe("未知");
    expect(formatPercent("")).toBe("无效数值");
  });
});
