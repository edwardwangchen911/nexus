import type { components } from "../lib/generated/schema";
import { useState } from "react";
import { formatPaperTime } from "./order-timeline";

export type AgentPaperCycleFact = components["schemas"]["AgentPaperCycleResponse"];

const statusLabels: Record<AgentPaperCycleFact["status"], string> = {
  prepared: "已准备", researching: "研究中", decision_recorded: "决策已保存",
  risk_evaluated: "风控已评估", awaiting_market: "等待行情", partially_filled: "部分成交",
  completed: "已完成", unavailable: "资料不足", failed: "运行失败",
};

const marketStatusLabels: Record<AgentPaperCycleFact["execution_market"]["status"], string> = {
  pending: "等待采集",
  qualified: "已用于模拟成交",
  probe: "采集验证完成",
  unavailable: "窗口不可用",
};

const resultExplanations = new Map<string, string>([
  ["NO_ORDER_REQUIRED", "Agent 已完成判断，确定性仓位计算结果为不调整，因此没有创建模拟订单。"],
  ["RESEARCH_OUTPUT_NOT_ACTIONABLE", "研究结果未通过可执行准入；本周期未创建模拟订单。"],
  ["RISK_INPUT_REQUIRED", "风险输入不足，系统已停止本周期，没有创建模拟订单。"],
  ["PAPER_ORDERS_AWAITING_MARKET", "Agent 已形成可执行方向并通过风控；模拟订单正在等待执行行情窗口。"],
]);

export function AgentPaperCycle({ cycles, unavailable = false }: {
  cycles: AgentPaperCycleFact[];
  unavailable?: boolean;
}): React.ReactElement {
  const [selectedCycleId, setSelectedCycleId] = useState<string | null>(null);
  const cycle = cycles.find((item) => item.cycle_id === selectedCycleId) ?? cycles[0] ?? null;
  if (!cycle) {
    return <section className="paper-orders" aria-labelledby="agent-cycle-title">
      <header><div><p className="eyebrow">只读事实链</p><h2 id="agent-cycle-title">当日 Agent 模拟周期</h2></div></header>
      <p className="paper-orders__empty">{unavailable ? "Agent 周期事实暂不可用；账户绩效与订单仍按各自事实显示。" : "尚无 Agent 模拟周期。"}</p>
    </section>;
  }
  const executionMarket = cycle.execution_market.source === "schwab"
    ? "执行分钟线 · Schwab pricehistory · 实时性未由响应声明"
    : "执行分钟线 · Alpaca IEX · 实时子集";
  return <section className="paper-orders" aria-labelledby="agent-cycle-title">
    <header className="paper-orders__heading"><div><p className="eyebrow">只读事实链 · 仅内部模拟</p><h2 id="agent-cycle-title">当日 Agent 模拟周期</h2><p>{statusLabels[cycle.status]} · 决策 <time dateTime={cycle.decision_time}>{formatPaperTime(cycle.decision_time)}</time> · 执行交易日 {cycle.execution_session}</p></div><div className="page-actions">{cycles.length > 1 ? <label className="paper-orders__filter">当日 Agent 模拟周期<select value={cycle.cycle_id} onChange={(event) => setSelectedCycleId(event.target.value)}>{cycles.map((item) => <option key={item.cycle_id} value={item.cycle_id}>{formatPaperTime(item.decision_time)} · {statusLabels[item.status]}</option>)}</select></label> : null}<span className="mode-badge">实盘锁定</span></div></header>
    <p>{cycle.decision_market ? <>决策报价 · Schwab Level One · 实时 · <time dateTime={cycle.decision_market.quote_at}>{formatPaperTime(cycle.decision_market.quote_at)}</time></> : "决策报价 · 历史周期未单独投影"}</p>
    <p>{executionMarket} · 连续 {cycle.execution_market.window_minutes} 分钟 · {marketStatusLabels[cycle.execution_market.status]}</p>
    <aside className="inline-notice" role="note" aria-label="Agent 运行结论">
      <strong>运行结论</strong>
      <p>{resultExplanations.get(cycle.result_code) ?? "系统已保存本周期结果；请结合周期状态和记录代码核对详细事实。"}</p>
      <p className="numeric-data">记录代码：{cycle.result_code}</p>
    </aside>
    {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- bounded table is keyboard-scrollable */}
    <div className="table-scroll" role="region" aria-label="Agent 模拟候选，可横向滚动" tabIndex={0}><table className="paper-position-table"><thead><tr><th>候选</th><th>准入原因</th><th>研究</th><th>方向</th><th>仓位动作</th></tr></thead><tbody>{cycle.candidates.map((candidate) => <tr key={candidate.instrument_id}><th>{candidate.symbol}</th><td>{candidate.selection_reasons.join("、")}</td><td>{candidate.research_status}</td><td>{candidate.stance ?? candidate.rejection_code ?? "待定"}</td><td>{candidate.sizing_action ?? "待定"}</td></tr>)}</tbody></table></div>
  </section>;
}
