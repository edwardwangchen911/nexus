import { describe, expect, it } from "vitest";

import {
  attentionPresentation,
  cycleDecisionPresentation,
  executionPresentation,
  loopPresentation,
  riskPresentation,
} from "../../../app/lib/product-status";
import type { components } from "../../../app/lib/generated/schema";
import {
  agentPaperCycle,
  cancellableOrder,
  filledOrder,
  instrumentId,
} from "../paper/fixtures";

type Cycle = components["schemas"]["AgentPaperCycleResponse"];
type Candidate = components["schemas"]["AgentPaperCandidateResponse"];
type Order = components["schemas"]["PaperOrderResponse"];
type Loop = components["schemas"]["WorkbenchLoopResponse"];
type Session = components["schemas"]["AgentPaperSessionSummaryResponse"];
type Attention = components["schemas"]["WorkbenchAttentionResponse"];

const hold: Candidate = {
  instrument_id: instrumentId,
  symbol: "AAPL",
  selection_reasons: ["quality_rank"],
  research_status: "completed",
  stance: "hold",
  sizing_action: "maintain",
  intent_hash: "a".repeat(64),
  output_id: "0198f7a0-1111-7777-8111-111111111126",
  rejection_code: null,
};

const watch: Candidate = {
  ...hold,
  instrument_id: "0198f7a0-1111-7777-8111-111111111128",
  symbol: "MSFT",
  stance: "watch",
  sizing_action: "none",
};

function cycle(patch: Partial<Cycle> & {
  risk_outcomes?: NonNullable<Cycle["decision"]>["risk_outcomes"];
} = {}): Cycle {
  const { risk_outcomes, ...cyclePatch } = patch;
  const value = structuredClone(agentPaperCycle);
  Object.assign(value, cyclePatch);
  if (risk_outcomes !== undefined && value.decision) {
    value.decision.risk_outcomes = risk_outcomes;
  }
  value.candidate_count = value.candidates.length;
  return value;
}

function order(status: Order["status"]): Order {
  const value = structuredClone(cancellableOrder);
  value.status = status;
  return value;
}

const session: Session = {
  decision_session: "2026-09-15",
  status: "ready",
  cycle_count: 1,
  completed_count: 0,
  latest_result_code: "CYCLE_PREPARED",
  last_completed_decision_at: null,
  next_eligible_at: "2026-09-15T14:35:00Z",
  next_eligible_reason: "NEXT_SLOT",
  execution_locked: true,
};

describe("central product-state vocabulary", () => {
  it.each([
    ["unavailable", "unknown", "阻断原因待核对"],
    ["failed", "model_failed", "运行失败"],
    ["prepared", "in_progress", "决策进行中"],
    ["completed", "decision_recorded", "决策已记录"],
  ] as const)("maps cycle status %s without calling every outcome no-trade", (status, key, label) => {
    expect(cycleDecisionPresentation(cycle({ status })).summary).toMatchObject({ key, label });
  });

  it.each([
    ["buy_candidate", "increase", "buy", "买入"],
    ["reduce_candidate", "reduce", "sell", "减仓"],
    ["hold", "maintain", "hold", "持有"],
    ["watch", "none", "watch", "观察"],
  ] as const)("maps candidate stance %s to %s", (stance, sizingAction, key, label) => {
    const candidate = { ...hold, stance, sizing_action: sizingAction };
    const result = cycleDecisionPresentation(cycle({
      status: "completed",
      result_code: "NO_ORDER_REQUIRED",
      candidates: [candidate],
    }));
    expect(result.candidates[0]).toMatchObject({ key, label });
  });

  it("keeps data-insufficient research separate from hold and watch", () => {
    const candidate: Candidate = {
      ...hold,
      research_status: "unavailable",
      stance: null,
      sizing_action: null,
      intent_hash: null,
      output_id: null,
      rejection_code: "RESEARCH_OUTPUT_NOT_ACTIONABLE",
    };
    expect(cycleDecisionPresentation(cycle({ candidates: [candidate] })).candidates[0]).toMatchObject({
      key: "data_insufficient",
      label: "数据不足",
      rawCode: "RESEARCH_OUTPUT_NOT_ACTIONABLE",
    });
  });

  it("preserves every candidate in canonical API order without choosing a cycle-wide winner", () => {
    const buy = { ...hold, stance: "buy_candidate" as const, sizing_action: "increase" as const };
    const reduce = { ...watch, stance: "reduce_candidate" as const, sizing_action: "reduce" as const };
    const result = cycleDecisionPresentation(cycle({ candidates: [watch, buy, reduce] }));

    expect(result.candidates.map(({ instrumentId, symbol, key }) => ({ instrumentId, symbol, key }))).toEqual([
      { instrumentId: watch.instrument_id, symbol: "MSFT", key: "watch" },
      { instrumentId: hold.instrument_id, symbol: "AAPL", key: "buy" },
      { instrumentId: watch.instrument_id, symbol: "MSFT", key: "sell" },
    ]);
    expect(result).not.toHaveProperty("winner");
  });

  it.each([
    [["rejected"], "risk_rejected", "风控拒绝"],
    [["reduced"], "risk_reduced", "风控缩减"],
    [["approved"], "risk_approved", "风控通过"],
    [["no_change"], "risk_no_change", "无需调整"],
    [[], "not_evaluated", "尚未评估"],
  ] as const)("maps deterministic risk outcome %j", (riskOutcomes, key, label) => {
    expect(riskPresentation(cycle({ risk_outcomes: [...riskOutcomes] }))).toMatchObject({ key, label });
  });

  it.each([
    [["no_change", "approved"], "risk_approved", "风控通过"],
    [["no_change", "reduced"], "risk_reduced", "风控缩减"],
    [["no_change", "rejected"], "risk_rejected", "风控拒绝"],
  ] as const)("keeps material risk outcome priority for %j", (riskOutcomes, key, label) => {
    expect(riskPresentation(cycle({ risk_outcomes: [...riskOutcomes] }))).toMatchObject({ key, label });
  });

  it.each([
    [cycle({ status: "completed", result_code: "NO_ORDER_REQUIRED", order_ids: [] }), [], "no_order", "无需下单"],
    [cycle({ status: "awaiting_market" }), [], "waiting_market", "等待执行行情"],
    [cycle(), [order("submitted")], "submitted", "订单已提交"],
    [cycle({ status: "awaiting_market" }), [order("partially_filled")], "partially_filled", "部分成交"],
    [cycle({ status: "completed" }), [filledOrder], "filled", "已成交"],
    [cycle({ status: "completed" }), [order("cancelled")], "cancelled", "已取消"],
    [cycle({ status: "completed" }), [order("rejected")], "rejected", "订单被拒绝"],
    [cycle(), [order("reconciling")], "reconciling", "正在核对结果"],
  ] as const)("maps execution state to %s", (cycleValue, orders, key, label) => {
    expect(executionPresentation(cycleValue, [...orders])).toMatchObject({ key, label });
  });

  it("marks contradictory execution facts as unknown with one safe action", () => {
    const result = executionPresentation(cycle({ status: "partially_filled" }), []);
    expect(result).toMatchObject({
      key: "result_unknown",
      label: "结果未知",
      safeActions: ["verify_original_result"],
    });
  });

  it("preserves an unknown result code without treating it as success or failure", () => {
    const result = executionPresentation(cycle({ status: "completed", result_code: "NEW_RESULT_CODE", order_ids: [] }), []);
    expect(result).toMatchObject({
      key: "unknown",
      label: "已保存状态，需查看记录代码",
      tone: "neutral",
      rawCode: "NEW_RESULT_CODE",
    });
  });

  it.each([
    ["online", null, "online", "循环在线", "30 秒内收到心跳"],
    ["offline", "HEARTBEAT_STALE", "offline", "循环离线", "心跳已经过期"],
    ["offline", "HEARTBEAT_INCOMPATIBLE", "offline", "循环版本不兼容", "循环报告的版本与当前代码预期不匹配"],
    ["unknown", "HEARTBEAT_MISSING", "unknown", "循环状态未知", "未找到循环心跳"],
  ] as const)("explains loop liveness reason %s", (status, reasonCode, key, label, detail) => {
    const loop: Loop = { status, reason_code: reasonCode, started_at: null, last_seen_at: null, stale_after_seconds: 30 };
    expect(loopPresentation(loop, session)).toMatchObject({ key, label, detail: expect.stringContaining(detail), rawCode: reasonCode });
  });

  it.each([
    ["data", "AGENT_PAPER_DATA_UNAVAILABLE", "RISK_INPUT_REQUIRED", "unknown", "已保存状态，需查看记录代码", "wait"],
    ["model", "AGENT_PAPER_MODEL_FAILED", "AGENT_DECISION_CONFLICT", "model_failed", "运行失败", "view_decision"],
    ["risk", "AGENT_PAPER_RISK_REJECTED", "RISK_REJECTED", "risk_rejected", "风控拒绝", "view_decision"],
    ["execution", "PAPER_ORDER_RECONCILING", "paper_result_unknown", "result_unknown", "结果未知", "verify_original_result"],
    ["service", "AGENT_PAPER_LOOP_OFFLINE", "HEARTBEAT_STALE", "loop_offline", "循环离线", "wait"],
    ["service", "AGENT_PAPER_LOOP_OFFLINE", "HEARTBEAT_INCOMPATIBLE", "loop_offline", "循环版本不兼容", "wait"],
  ] as const)("maps attention code %s", (kind, titleCode, reasonCode, key, label, safeAction) => {
    const item: Attention = {
      kind,
      severity: "warning",
      title_code: titleCode,
      reason_code: reasonCode,
      safe_action: "wait",
      href: "/settings",
      cycle_id: null,
      order_id: null,
    };
    const result = attentionPresentation(item);
    expect(result).toMatchObject({ key, label, rawCode: reasonCode, safeActions: [safeAction] });
  });

  it("does not trust a recognized title code when the attention kind disagrees", () => {
    const item: Attention = {
      kind: "service",
      severity: "warning",
      title_code: "AGENT_PAPER_RISK_REJECTED",
      reason_code: "RISK_REJECTED",
      safe_action: "view_decision",
      href: "/decisions/0198f7a0-1111-7777-8111-111111111120",
      cycle_id: "0198f7a0-1111-7777-8111-111111111120",
      order_id: null,
    };
    expect(attentionPresentation(item)).toMatchObject({
      key: "unknown",
      label: "已保存状态，需查看记录代码",
      tone: "neutral",
      rawCode: "RISK_REJECTED",
    });
  });

  it("keeps unknown attention reason codes neutral and inspectable", () => {
    const item: Attention = {
      kind: "service",
      severity: "critical",
      title_code: "AGENT_PAPER_LOOP_OFFLINE",
      reason_code: "NEW_REASON_CODE",
      safe_action: "wait",
      href: "/settings",
      cycle_id: null,
      order_id: null,
    };
    expect(attentionPresentation(item)).toMatchObject({
      key: "unknown",
      label: "已保存状态，需查看记录代码",
      tone: "neutral",
      rawCode: "NEW_REASON_CODE",
    });
  });
});

it("does not turn research admission failures into missing data", () => {
  expect(cycleDecisionPresentation(cycle({ status: "unavailable", result_code: "RESEARCH_OUTPUT_NOT_ACTIONABLE" })).summary.label).toBe("研究未通过交易准入");
  expect(cycleDecisionPresentation(cycle({ status: "unavailable", result_code: "RESEARCH_ADMISSION_UNAVAILABLE" })).summary.label).toBe("阻断原因待核对");
  expect(cycleDecisionPresentation(cycle({ status: "failed", result_code: "AGENT_DECISION_CONFLICT" })).summary.label).toBe("运行失败");
});
