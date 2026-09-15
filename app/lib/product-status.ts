import type { components } from "./generated/schema";

type Cycle = components["schemas"]["AgentPaperCycleResponse"];
type Session = components["schemas"]["AgentPaperSessionSummaryResponse"];
type Order = components["schemas"]["PaperOrderResponse"];
type Loop = components["schemas"]["WorkbenchLoopResponse"];
type Attention = components["schemas"]["WorkbenchAttentionResponse"];
type SafeAction = Attention["safe_action"];

export type ProductTone = "neutral" | "positive" | "warning" | "critical";

export type ProductPresentation<Key extends string> = {
  key: Key;
  label: string;
  detail: string;
  tone: ProductTone;
  rawCode?: string | null;
};

type CandidateKey = "buy" | "sell" | "hold" | "watch" | "data_insufficient";
type DecisionSummaryKey = "in_progress" | "decision_recorded" | "data_insufficient" | "model_failed";
type RiskKey = "not_evaluated" | "risk_approved" | "risk_reduced" | "risk_rejected";
type ExecutionKey = "no_order" | "waiting_market" | "submitted" | "partially_filled" | "filled"
  | "cancelled" | "rejected" | "reconciling" | "result_unknown" | "unknown";
type LoopKey = "online" | "offline" | "unknown";
type AttentionKey = "data_insufficient" | "model_failed" | "risk_rejected" | "result_unknown"
  | "loop_offline" | "unknown";

export type CycleDecisionPresentation = {
  summary: ProductPresentation<DecisionSummaryKey>;
  candidates: Array<ProductPresentation<CandidateKey> & {
    instrumentId: string;
    symbol: string;
  }>;
};

export type ActionableProductPresentation<Key extends string> = ProductPresentation<Key> & {
  safeActions: SafeAction[];
};

const DATA_REASON_CODES = new Set([
  "OPEN_ORDER_EXISTS",
  "NO_ELIGIBLE_CANDIDATE",
  "RISK_INPUT_REQUIRED",
  "RESEARCH_ADMISSION_UNAVAILABLE",
  "RESEARCH_OUTPUT_NOT_ACTIONABLE",
  "PAPER_MARKET_WINDOW_UNAVAILABLE",
]);
const MODEL_REASON_CODES = new Set(["AGENT_DECISION_CONFLICT", "PAPER_BATCH_CONFLICT"]);

export function cycleDecisionPresentation(cycle: Cycle): CycleDecisionPresentation {
  return {
    summary: decisionSummary(cycle),
    candidates: cycle.candidates.map((candidate) => ({
      ...candidatePresentation(candidate),
      instrumentId: candidate.instrument_id,
      symbol: candidate.symbol,
    })),
  };
}

export function riskPresentation(cycle: Cycle): ProductPresentation<RiskKey> {
  const outcomes = cycle.decision?.risk_outcomes ?? [];
  if (outcomes.includes("rejected")) {
    return presentation("risk_rejected", "风控拒绝", "确定性风控拒绝了本次调整。", "critical", cycle.result_code);
  }
  if (outcomes.includes("reduced")) {
    return presentation("risk_reduced", "风控缩减", "确定性风控缩减了计划调整规模。", "warning", cycle.result_code);
  }
  if (outcomes.includes("approved")) {
    return presentation("risk_approved", "风控通过", "本次决策已通过确定性风控。", "positive", cycle.result_code);
  }
  return presentation("not_evaluated", "尚未评估", "当前没有已保存的风控结论。", "neutral", cycle.result_code);
}

export function executionPresentation(cycle: Cycle, orders: Order[]): ActionableProductPresentation<ExecutionKey> {
  if (cycle.status === "partially_filled" && orders.length === 0) {
    return actionable("result_unknown", "结果未知", "周期显示部分成交，但缺少关联订单事实。", "critical", cycle.result_code, ["verify_original_result"]);
  }
  if (orders.some((order) => order.status === "reconciling")) {
    return actionable("reconciling", "正在核对结果", "订单结果尚未确定，需核对原操作记录。", "warning", cycle.result_code, ["verify_original_result"]);
  }
  if (orders.some((order) => order.status === "partially_filled")) {
    return actionable("partially_filled", "部分成交", "关联订单已部分成交，剩余数量仍在跟踪。", "warning", cycle.result_code);
  }
  if (orders.some((order) => order.status === "rejected" || order.status === "risk_rejected")) {
    return actionable("rejected", "订单被拒绝", "至少一个关联订单已被拒绝。", "critical", cycle.result_code);
  }
  if (orders.length > 0 && orders.every((order) => order.status === "cancelled")) {
    return actionable("cancelled", "已取消", "关联订单已经取消。", "neutral", cycle.result_code);
  }
  if (orders.length > 0 && orders.every((order) => order.status === "filled")) {
    return actionable("filled", "已成交", "关联订单已经全部成交。", "positive", cycle.result_code);
  }
  if (orders.some((order) => ["proposed", "risk_approved", "awaiting_approval", "submitted"].includes(order.status))) {
    return actionable("submitted", "订单已提交", "关联模拟订单正在等待后续执行结果。", "warning", cycle.result_code);
  }
  if (orders.length === 0 && cycle.result_code === "NO_ORDER_REQUIRED") {
    return actionable("no_order", "无需下单", "本周期已完成，确定性结果不需要创建订单。", "neutral", cycle.result_code);
  }
  if (orders.length === 0 && cycle.status === "awaiting_market") {
    return actionable("waiting_market", "等待执行行情", "周期正在等待符合约束的执行行情。", "warning", cycle.result_code);
  }
  return actionable(
    "unknown",
    "已保存状态，需查看记录代码",
    "当前代码尚未映射为产品状态，请查看原始记录。",
    "neutral",
    cycle.result_code,
  );
}

export function loopPresentation(loop: Loop, session: Session): ProductPresentation<LoopKey> {
  const sessionLabel = sessionStatusLabel(session.status);
  if (loop.status === "online") {
    return presentation("online", "循环在线", `30 秒内收到心跳；已保存会话状态为${sessionLabel}。`, "positive", session.latest_result_code);
  }
  if (loop.status === "offline") {
    return presentation("offline", "循环离线", `心跳已经过期；已保存会话状态仍为${sessionLabel}。`, "warning", session.latest_result_code);
  }
  return presentation("unknown", "循环状态未知", `当前无法确认循环心跳；已保存会话状态为${sessionLabel}。`, "neutral", session.latest_result_code);
}

export function attentionPresentation(item: Attention): ActionableProductPresentation<AttentionKey> & {
  href: string;
  cycleId: string | null;
  orderId: string | null;
} {
  const base = knownAttention(item) ?? actionable(
    "unknown",
    "已保存状态，需查看记录代码",
    "当前原因代码尚未映射，请查看原始记录。",
    "neutral",
    item.reason_code,
    [item.safe_action],
  );
  return {
    ...base,
    href: item.href,
    cycleId: item.cycle_id ?? null,
    orderId: item.order_id ?? null,
  };
}

function decisionSummary(cycle: Cycle): ProductPresentation<DecisionSummaryKey> {
  if (cycle.status === "unavailable") {
    return presentation("data_insufficient", "数据不足", "当前证据不足，未形成可执行决策。", "warning", cycle.result_code);
  }
  if (cycle.status === "failed") {
    return presentation("model_failed", "模型失败", "Agent 决策流程失败，未把失败解释为不交易。", "critical", cycle.result_code);
  }
  if (cycle.status === "prepared" || cycle.status === "researching") {
    return presentation("in_progress", "决策进行中", "Agent 正在形成或补充本周期决策。", "neutral", cycle.result_code);
  }
  return presentation("decision_recorded", "决策已记录", "本周期已有可追溯的决策事实。", "neutral", cycle.result_code);
}

function candidatePresentation(candidate: Cycle["candidates"][number]): ProductPresentation<CandidateKey> {
  if (candidate.research_status !== "completed" || candidate.stance === null) {
    return presentation("data_insufficient", "数据不足", "该候选未形成完整研究结论。", "warning", candidate.rejection_code);
  }
  if (candidate.stance === "buy_candidate") {
    return presentation("buy", "买入", "Agent 建议增加该标的仓位。", "positive");
  }
  if (candidate.stance === "reduce_candidate") {
    return presentation("sell", "减仓", "Agent 建议减少该标的仓位。", "warning");
  }
  if (candidate.stance === "hold") {
    return presentation("hold", "持有", "Agent 建议维持当前仓位。", "neutral");
  }
  return presentation("watch", "观察", "Agent 建议继续观察，当前不调整仓位。", "neutral");
}

function knownAttention(item: Attention): ActionableProductPresentation<Exclude<AttentionKey, "unknown">> | null {
  if (item.kind === "data" && item.title_code === "AGENT_PAPER_DATA_UNAVAILABLE" && DATA_REASON_CODES.has(item.reason_code)) {
    return actionable("data_insufficient", "数据不足", "决策所需证据尚未齐备。", "warning", item.reason_code, ["view_evidence"]);
  }
  if (item.kind === "model" && item.title_code === "AGENT_PAPER_MODEL_FAILED" && MODEL_REASON_CODES.has(item.reason_code)) {
    return actionable("model_failed", "模型失败", "Agent 决策流程未正常完成。", "critical", item.reason_code, ["view_decision"]);
  }
  if (item.kind === "risk" && item.title_code === "AGENT_PAPER_RISK_REJECTED" && item.reason_code === "RISK_REJECTED") {
    return actionable("risk_rejected", "风控拒绝", "确定性风控拒绝了本次调整。", "warning", item.reason_code, ["view_decision"]);
  }
  if (item.kind === "execution" && item.title_code === "PAPER_ORDER_RECONCILING") {
    return actionable("result_unknown", "结果未知", "订单结果尚未确定，请核对原操作结果。", "critical", item.reason_code, ["verify_original_result"]);
  }
  if (item.kind === "service" && item.title_code === "AGENT_PAPER_LOOP_OFFLINE" && item.reason_code === "HEARTBEAT_STALE") {
    return actionable("loop_offline", "循环离线", "循环心跳已经超过 30 秒。", "warning", item.reason_code, ["wait"]);
  }
  return null;
}

function sessionStatusLabel(status: Session["status"]): string {
  return ({
    calendar_unavailable: "交易日历不可用",
    not_started: "尚未开始",
    ready: "等待运行",
    in_progress: "运行中",
    completed: "已完成",
    stopped: "已停止",
    closed: "已收盘",
    historical: "历史记录",
  } as const)[status];
}

function presentation<Key extends string>(
  key: Key,
  label: string,
  detail: string,
  tone: ProductTone,
  rawCode?: string | null,
): ProductPresentation<Key> {
  return { key, label, detail, tone, rawCode };
}

function actionable<Key extends string>(
  key: Key,
  label: string,
  detail: string,
  tone: ProductTone,
  rawCode?: string | null,
  safeActions: SafeAction[] = [],
): ActionableProductPresentation<Key> {
  return { ...presentation(key, label, detail, tone, rawCode), safeActions };
}
