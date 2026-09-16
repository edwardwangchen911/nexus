import type { components } from "../lib/generated/schema";
import { useState } from "react";
import { formatPaperTime } from "./order-timeline";

export type AgentPaperCycleFact = components["schemas"]["AgentPaperCycleResponse"];
export type AgentPaperSessionSummaryFact = components["schemas"]["AgentPaperSessionSummaryResponse"];
export type AgentPaperVersionObservationFact = components["schemas"]["AgentPaperVersionObservationResponse"];

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

const sessionStatusLabels: Record<AgentPaperSessionSummaryFact["status"], string> = {
  calendar_unavailable: "交易日历不可用",
  not_started: "今日尚未开始",
  ready: "当前时段可运行",
  in_progress: "周期处理中",
  completed: "最近周期已完成",
  stopped: "今日已停止",
  closed: "当前不在运行时段",
  historical: "历史交易日",
};

const nextReasonLabels: Partial<Record<NonNullable<AgentPaperSessionSummaryFact["next_eligible_reason"]>, string>> = {
  CURRENT_SLOT: "当前五分钟槽",
  NEXT_SLOT: "下一五分钟槽",
  SESSION_OPEN: "本交易日开始",
  NEXT_SESSION: "下一交易日开始",
  CURRENT_CYCLE_IN_PROGRESS: "当前周期结束后再判断",
  CALENDAR_UNAVAILABLE: "等待交易日历证据",
};

const versionReasonLabels: Record<AgentPaperVersionObservationFact["reason_codes"][number], string> = {
  VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE: "尚无 Agent 版本级绩效归因",
  VERSION_ORDER_PATH_UNOBSERVED: "尚无模拟订单与成交的持久化路径事实",
  VERSION_CHALLENGER_UNAVAILABLE: "尚无并行 challenger 版本",
};

export function AgentPaperVersionHistory({ history }: {
  history: AgentPaperVersionObservationFact[];
}): React.ReactElement {
  return <section className="paper-performance" aria-labelledby="agent-version-history-title">
    <header><div><p className="eyebrow">冻结身份 · 跨交易日只读聚合</p><h2 id="agent-version-history-title">Agent 版本历史</h2></div><span className="mode-badge">继续观察</span></header>
    <p>这里只记录依次出现过的冻结版本。顺序历史不是并行 challenger；当前没有正式晋升或淘汰结论。最近观察版本不代表当前进程在线或当前配置仍选中该版本。</p>
    {history.map((observation, index) => <article key={`${observation.prompt_hash}:${observation.model_hash}`}>
      <h3>{index === 0 ? "最近观察版本" : "历史冻结版本"}</h3>
      <dl>
      <div><dt>Prompt 版本</dt><dd>{observation.prompt_version}</dd></div>
      <div><dt>模型</dt><dd>{observation.model_id}</dd></div>
      <div><dt>模型配置</dt><dd>{observation.model_version}</dd></div>
      <div><dt>版本指纹</dt><dd className="numeric-data" title={`prompt ${observation.prompt_hash} · model ${observation.model_hash}`}>prompt {shortHash(observation.prompt_hash)} · model {shortHash(observation.model_hash)}</dd></div>
      <div><dt>观察进度</dt><dd>{observation.completed_count} / {observation.cycle_count} 个周期完成</dd></div>
      <div><dt>交易日覆盖</dt><dd>{observation.observed_sessions} 个交易日</dd></div>
      <div><dt>订单 / 成交</dt><dd>{observation.order_count} / {observation.fill_count}</dd></div>
      </dl>
      <aside className="inline-notice" role="note" aria-label={`Agent 版本观察结论 ${index + 1}`}>
      <strong>继续观察</strong>
      <ul>{observation.reason_codes.map((reason) => <li key={reason}>{versionReasonLabels[reason]}</li>)}</ul>
      </aside>
    </article>)}
    <p>同名标签可能包含不同冻结配置；每项统计只属于对应指纹，不触发任何版本变更或自动创建 challenger。</p>
  </section>;
}

function shortHash(value: string): string {
  return `${value.slice(0, 12)}…`;
}

export function AgentPaperSessionSummary({ summary }: {
  summary: AgentPaperSessionSummaryFact;
}): React.ReactElement {
  const latestExplanation = summary.latest_result_code
    ? resultExplanations.get(summary.latest_result_code) ?? `记录代码：${summary.latest_result_code}`
    : "今日尚无周期结果。";
  const nextLabel = summary.next_eligible_at
    ? `${nextReasonLabels[summary.next_eligible_reason ?? "CURRENT_SLOT"] ?? "下一可运行时段"} · ${formatPaperTime(summary.next_eligible_at)}`
    : nextReasonLabels[summary.next_eligible_reason ?? "CALENDAR_UNAVAILABLE"] ?? "暂无";
  return <section className="paper-performance" aria-labelledby="agent-session-summary-title">
    <header><div><p className="eyebrow">连续模拟 · 持久化状态</p><h2 id="agent-session-summary-title">今日连续运行状态</h2></div><span className="mode-badge">实盘锁定</span></header>
    <dl>
      <div><dt>今日状态</dt><dd>{sessionStatusLabels[summary.status]}</dd></div>
      <div><dt>周期进度</dt><dd>已完成 {summary.completed_count} / {summary.cycle_count} 个周期</dd></div>
      <div><dt>最近完成周期的决策时间</dt><dd>{summary.last_completed_decision_at ? formatPaperTime(summary.last_completed_decision_at) : "暂无"}</dd></div>
      <div><dt>下一可运行时段</dt><dd>{nextLabel}</dd></div>
    </dl>
    <aside className="inline-notice" role="note" aria-label="连续模拟最新结论"><strong>最新结论</strong><p>{latestExplanation}</p></aside>
    <p>只反映已保存周期，不代表前台循环进程在线；刷新页面不会触发行情、模型或订单。</p>
  </section>;
}

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
