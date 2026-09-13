import type { components } from "../lib/generated/schema";

export type AgentPaperCycleFact = components["schemas"]["AgentPaperCycleResponse"];

const statusLabels: Record<AgentPaperCycleFact["status"], string> = {
  prepared: "已准备", researching: "研究中", decision_recorded: "决策已保存",
  risk_evaluated: "风控已评估", awaiting_market: "等待行情", partially_filled: "部分成交",
  completed: "已完成", unavailable: "资料不足", failed: "运行失败",
};

export function AgentPaperCycle({ cycle, unavailable = false }: {
  cycle: AgentPaperCycleFact | null;
  unavailable?: boolean;
}): React.ReactElement {
  if (!cycle) {
    return <section className="paper-orders" aria-labelledby="agent-cycle-title">
      <header><div><p className="eyebrow">只读事实链</p><h2 id="agent-cycle-title">最新 Agent 模拟周期</h2></div></header>
      <p className="paper-orders__empty">{unavailable ? "Agent 周期事实暂不可用；账户绩效与订单仍按各自事实显示。" : "尚无 Agent 模拟周期。"}</p>
    </section>;
  }
  return <section className="paper-orders" aria-labelledby="agent-cycle-title">
    <header className="paper-orders__heading"><div><p className="eyebrow">只读事实链 · 仅内部模拟</p><h2 id="agent-cycle-title">最新 Agent 模拟周期</h2><p>{statusLabels[cycle.status]} · 决策 {cycle.decision_time} · 下一交易日 {cycle.execution_session}</p></div><span className="mode-badge">实盘锁定</span></header>
    <p>行情口径：Alpaca / IEX 实时子集，连续 {cycle.market.window_minutes} 分钟；不代表全市场。停止原因：{cycle.result_code}</p>
    {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- bounded table is keyboard-scrollable */}
    <div className="table-scroll" role="region" aria-label="Agent 模拟候选，可横向滚动" tabIndex={0}><table className="paper-position-table"><thead><tr><th>候选</th><th>准入原因</th><th>研究</th><th>方向</th><th>仓位动作</th></tr></thead><tbody>{cycle.candidates.map((candidate) => <tr key={candidate.instrument_id}><th>{candidate.symbol}</th><td>{candidate.selection_reasons.join("、")}</td><td>{candidate.research_status}</td><td>{candidate.stance ?? candidate.rejection_code ?? "待定"}</td><td>{candidate.sizing_action ?? "待定"}</td></tr>)}</tbody></table></div>
  </section>;
}
