import { describe, expect, it } from "vitest";
import { actorLabel, policyScopeLabel, reasonLabel, statusLabel } from "../../../app/lib/display-labels";

describe("plain-language display labels", () => {
  it("does not infer a past unlock action from an unlocked state", () => {
    expect(statusLabel("unlocked")).toBe("未锁定");
  });
  it.each([["disabled", "已禁用"], ["fixture", "离线示例"], ["read_only", "只读"]])("labels the valid integration mode %s", (code, label) => {
    expect(statusLabel(code)).toBe(label);
  });
  it("describes a model execution failure without inventing its cause", () => {
    expect(reasonLabel("model_execution_failed")).toBe("模型执行未完成，具体原因尚未确认");
  });
  it("explains why conflicting research evidence abstains", () => {
    expect(reasonLabel("conflicting_facts")).toBe("研究报告了尚未解决的事实矛盾");
  });
  it.each([["DRAWDOWN_252", 252], ["BETA_CORRELATION_126", 126], ["VOLATILITY_63", 63], ["VOLATILITY_20", 20]])("describes the %s coverage window without requiring every day", (code, days) => {
    expect(reasonLabel(`${code}_COVERAGE_INSUFFICIENT`)).toContain(`${days} 个交易日窗口内有效价格覆盖不足`);
  });
  it("labels known actors and leaves an unknown actor identifiable", () => {
    expect(actorLabel("user")).toBe("用户");
    expect(actorLabel("system")).toBe("系统");
    expect(actorLabel("service")).toBe("后台服务");
    expect(actorLabel("unexpected")).toBe("未识别操作方（unexpected）");
  });
  it("clarifies policy scope while preserving its identity", () => {
    expect(policyScopeLabel("portfolio")).toBe("组合范围");
    expect(policyScopeLabel("currency:USD")).toBe("币种范围：USD");
    expect(policyScopeLabel("instrument:0198d600-1000-7000-8000-000000001501")).toBe("标的范围：0198d600-1000-7000-8000-000000001501");
    expect(policyScopeLabel("custom_scope")).toBe("未识别范围（custom_scope）");
  });
});
