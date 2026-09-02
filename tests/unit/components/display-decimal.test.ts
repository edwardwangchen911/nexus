import { describe, expect, it } from "vitest";
import { formatDisplayDecimal } from "../../../app/lib/display-decimal";

describe("decimal presentation", () => {
  it.each([
    ["123.450000000000", "123.45"],
    ["0.123456789012", "0.123"],
    ["999.9995", "1000"],
    ["-1.2345", "-1.235"],
    ["-0.0004", "0"],
    ["-0.0005", "-0.001"],
    ["9007199254740993.1235", "9007199254740993.124"],
  ])("rounds %s without binary float conversion", (value, expected) => {
    expect(formatDisplayDecimal(value)).toBe(expected);
  });

  it("preserves missing and invalid values instead of making them zero", () => {
    expect(formatDisplayDecimal(null)).toBe("未知");
    expect(formatDisplayDecimal(undefined)).toBe("未知");
    for (const value of ["", "NaN", "Infinity", Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(formatDisplayDecimal(value)).toBe("无效数值");
    }
  });

  it("retains existing money formatting within the three-decimal limit", () => {
    expect(formatDisplayDecimal("1234.565", { group: true, fixedDigits: 2 })).toBe("1,234.57");
    expect(formatDisplayDecimal("1.123456", { fixedDigits: 6 })).toBe("1.123");
    expect(formatDisplayDecimal(-0.00001)).toBe("0");
  });
});
