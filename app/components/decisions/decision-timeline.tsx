import { CheckCircle2, CircleDashed, ShieldAlert } from "lucide-react";

import { formatDisplayDecimal } from "../../lib/display-decimal";
import { formatDisplayTime } from "../../lib/display-format";
import { cycleDecisionPresentation, executionPresentation, riskPresentation } from "../../lib/product-status";
import { researchAdmissionLabel, researchJobLabel } from "../../lib/paper-research-status";
import type { components } from "../../lib/generated/schema";

type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];
type Order = components["schemas"]["PaperOrderResponse"];

const stages = ["运行窗口", "市场与研究证据", "Agent 决策", "确定性仓位计算", "确定性风控", "订单结论", "执行结果", "后续表现"] as const;

export function DecisionTimeline({ detail, paperHref = "/paper" }: { detail: Detail; paperHref?: string }): React.ReactElement {
  const { cycle, orders } = detail;
  const decision = cycleDecisionPresentation(cycle);
  const risk = riskPresentation(cycle);
  const execution = executionPresentation(cycle, orders);
  const resultUnknown = execution.key === "result_unknown" || execution.key === "unknown";
  const executionState = resultUnknown || execution.key === "rejected" || execution.key === "reconciling"
    ? "attention"
    : execution.key === "filled" || execution.key === "cancelled" || execution.key === "no_order"
      ? "known"
      : "pending";
  const marketFacts = cycle.decision_market
    ? `Schwab Level One · 实时 · ${formatDisplayTime(cycle.decision_market.quote_at)}`
    : "未知：本周期没有单独保存决策报价投影。";
  const candidateFacts = cycle.candidates.length
    ? cycle.candidates.map((candidate) => `${candidate.symbol} · ${researchJobLabel(candidate, cycle)} · ${researchAdmissionLabel(candidate, cycle)}`).join("；")
    : "未知：本周期没有保存候选研究事实。";

  return <ol className="decision-timeline" aria-label="决策证据时间线">
    <Stage title={stages[0]} state="known" summary={`${cycle.decision_session} · 决策 ${formatDisplayTime(cycle.decision_time)} · 执行交易日 ${cycle.execution_session}`}>
      <FactDetails rows={[["周期 ID", cycle.cycle_id], ["决策槽位", formatDisplayTime(cycle.decision_slot_at)], ["原始结果代码", cycle.result_code]]} />
    </Stage>
    <Stage title={stages[1]} state={cycle.decision_market || cycle.candidates.length ? "known" : "unknown"} summary={`${marketFacts} ${candidateFacts}`}>
      <FactDetails rows={cycle.candidates.flatMap((candidate) => [[`${candidate.symbol} 输出 ID`, candidate.output_id ?? "未知"], [`${candidate.symbol} 意图哈希`, candidate.intent_hash ?? "未知"], [`${candidate.symbol} 准入原因`, candidate.selection_reasons.join("、") || "未知"]])} />
    </Stage>
    <Stage title={stages[2]} state={decision.candidates.length ? "known" : "unknown"} summary={decision.candidates.length ? decision.candidates.map((item) => `${item.symbol}：${item.label}`).join("；") : `未知：${decision.summary.detail}`}>
      <FactDetails rows={cycle.candidates.map((candidate) => [candidate.symbol, candidate.rejection_code ?? candidate.stance ?? "未知"])} />
    </Stage>
    <Stage title={stages[3]} state={cycle.decision ? "known" : "unknown"} summary={cycle.decision ? `目标现金权重 ${formatDisplayDecimal(cycle.decision.target_cash_weight)}；${cycle.candidates.map((candidate) => `${candidate.symbol} ${candidate.sizing_action ?? "未知"}`).join("；")}` : "未知：本周期没有保存确定性仓位计算结果。"}>
      {cycle.decision ? <FactDetails rows={[["仓位策略版本", cycle.decision.sizing_policy_version], ["决策 ID", cycle.decision.decision_id]]} /> : null}
    </Stage>
    <Stage title={stages[4]} state={risk.key === "not_evaluated" ? "unknown" : risk.key === "risk_rejected" ? "attention" : "known"} summary={risk.key === "not_evaluated" ? `未知：${risk.detail}` : `${risk.label}：${risk.detail}`}>
      {cycle.decision ? <FactDetails rows={[["风控结果", cycle.decision.risk_outcomes.join("、") || "未知"]]} /> : null}
    </Stage>
    <Stage title={stages[5]} state={orders.length || cycle.result_code === "NO_ORDER_REQUIRED" ? "known" : "unknown"} summary={orderConclusion(cycle.result_code, orders)}>
      {orders.length ? <FactDetails rows={orders.map((order) => [order.order_id, `${order.symbol ?? "未知标的"} · ${order.side} · ${order.status}`])} /> : null}
    </Stage>
    <Stage title={stages[6]} state={executionState} summary={resultUnknown ? "结果未知：现有周期与订单事实不能证明最终结果。" : `${execution.label}：${execution.detail}`}>
      {orders.length ? <ExecutionDetails orders={orders} /> : null}
      {resultUnknown ? <a className="button button--danger" href={paperHref}><ShieldAlert aria-hidden="true" size={17} />核对原操作结果</a> : null}
    </Stage>
    <Stage title={stages[7]} state="unknown" summary="未知：接口尚未提供该周期之后的统一表现归因。" />
  </ol>;
}

function Stage({ title, state, summary, children }: { title: typeof stages[number]; state: "known" | "unknown" | "attention" | "pending"; summary: string; children?: React.ReactNode }): React.ReactElement {
  const Icon = state === "known" ? CheckCircle2 : CircleDashed;
  return <li data-state={state}><section aria-label={title}>
    <span className="decision-stage__marker" aria-hidden="true"><Icon size={18} /></span>
    <div className="decision-stage__body"><div className="decision-stage__heading"><h2>{title}</h2>{state !== "known" ? <span>{state === "unknown" ? "未知" : state === "attention" ? "需核对" : "处理中"}</span> : null}</div><p>{summary}</p>{children}</div>
  </section></li>;
}

function FactDetails({ rows }: { rows: string[][] }): React.ReactElement | null {
  if (!rows.length) return null;
  return <details className="decision-facts"><summary>查看原始事实</summary><dl>{rows.map(([term, value], index) => <div key={`${term}-${index}`}><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>)}</dl></details>;
}

function ExecutionDetails({ orders }: { orders: Order[] }): React.ReactElement {
  return <details className="decision-facts"><summary>查看订单事件与成交</summary>{orders.map((order) => <div className="decision-execution" key={order.order_id}>
    <h3>{order.symbol ?? "未知标的"} · {order.status}</h3>
    <p className="numeric-data">已成交 {formatDisplayDecimal(order.filled_quantity)} / {formatDisplayDecimal(order.quantity)} · 剩余 {formatDisplayDecimal(order.remaining_quantity)}</p>
    <ol aria-label={`${order.symbol ?? order.order_id} 订单事件`}>{order.events.map((event) => <li key={event.event_seq}><span className="numeric-data">E{event.event_seq}</span><span>{event.from_status ?? "起始"} → {event.to_status}</span><time dateTime={event.occurred_at}>{formatDisplayTime(event.occurred_at)}</time></li>)}</ol>
    {order.fills.length ? <ul aria-label={`${order.symbol ?? order.order_id} 成交记录`}>{order.fills.map((fill) => <li key={fill.fill_id}><span className="numeric-data">{formatDisplayDecimal(fill.quantity)} 股 @ USD {formatDisplayDecimal(fill.price)}</span><time dateTime={fill.occurred_at}>{formatDisplayTime(fill.occurred_at)}</time></li>)}</ul> : <p>尚无成交记录。</p>}
  </div>)}</details>;
}

function orderConclusion(resultCode: string, orders: Order[]): string {
  if (orders.length) return `已关联 ${orders.length} 笔订单；展开查看精确订单 ID 与状态。`;
  if (resultCode === "NO_ORDER_REQUIRED") return "无需下单：确定性结果不需要创建订单。";
  return "未知：没有关联订单事实，无法证明订单结果。";
}

export function isPaperOrder(value: unknown): value is Order {
  return isPlain(value) && typeof value.order_id === "string" && typeof value.quantity === "string"
    && typeof value.filled_quantity === "string" && typeof value.remaining_quantity === "string"
    && typeof value.status === "string" && Array.isArray(value.events) && Array.isArray(value.fills)
    && value.events.every((event) => isPlain(event) && Number.isInteger(event.event_seq) && typeof event.to_status === "string")
    && value.fills.every((fill) => isPlain(fill) && typeof fill.fill_id === "string" && typeof fill.quantity === "string");
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
