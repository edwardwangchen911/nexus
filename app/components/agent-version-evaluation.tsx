import type { components } from "../lib/generated/schema";
import { formatDisplayTime, formatPercent } from "../lib/display-format";
import { formatPaperDecimal } from "./order-timeline";

export type AgentVersionEvaluationFact = components["schemas"]["AgentVersionEvaluationResponse"];

const statusLabels: Record<AgentVersionEvaluationFact["status"], string> = {
  observing: "观察中",
  PASS: "已通过",
  FAIL: "未通过",
  UNKNOWN: "证据不足",
};

const comparisonLabels: Record<AgentVersionEvaluationFact["comparisons"][number]["key"], string> = {
  hold_current: "保持现金",
  baseline_v1: "baseline_v1",
  market_benchmark: "市场基准（SPY）",
};

const slotStatusLabels: Record<AgentVersionEvaluationFact["slots"][number]["status"], string> = {
  scheduled: "等待登记时间",
  due: "已到登记时间",
  running: "双通道处理中",
  paired: "已配对",
  attention: "需要核查",
};

const laneStatusLabels: Record<AgentVersionEvaluationFact["slots"][number]["incumbent"]["status"], string> = {
  pending: "未创建",
  prepared: "已准备",
  researching: "研究中",
  decision_recorded: "已记录决策",
  risk_evaluated: "已完成风控",
  awaiting_market: "等待执行窗口",
  partially_filled: "部分成交",
  completed: "已完成",
  unavailable: "不可用",
  failed: "失败",
};

export function AgentVersionEvaluation({ evaluation }: {
  evaluation: AgentVersionEvaluationFact;
}): React.ReactElement {
  return <section className="paper-performance" aria-labelledby="agent-version-evaluation-title">
    <header><div><p className="eyebrow">AAPL · 冻结双通道 · 只读</p><h2 id="agent-version-evaluation-title">Agent 版本并行评价</h2></div><span className="mode-badge">{statusLabels[evaluation.status]}</span></header>
    <p>incumbent 与 challenger 使用预先冻结的版本、独立内部账户和同一观察槽；页面刷新不会运行研究或改变选中版本。</p>
    <dl>
      <div><dt>训练槽</dt><dd>{evaluation.train_completed} / 4 已配对</dd></div>
      <div><dt>样本外槽</dt><dd>{evaluation.oos_completed} / 20 已配对</dd></div>
      <div><dt>缺失 / 不可比</dt><dd>{evaluation.oos_missing} / {evaluation.oos_incomparable}</dd></div>
      <div><dt>安全边界</dt><dd>执行锁定 · 券商写入关闭 · 实盘写入关闭</dd></div>
      <div><dt>incumbent 指纹</dt><dd className="numeric-data" title={`${evaluation.incumbent_prompt_hash}:${evaluation.incumbent_model_hash}`}>{shortHash(evaluation.incumbent_prompt_hash)} · {shortHash(evaluation.incumbent_model_hash)}</dd></div>
      <div><dt>challenger 指纹</dt><dd className="numeric-data" title={`${evaluation.challenger_prompt_hash}:${evaluation.challenger_model_hash}`}>{shortHash(evaluation.challenger_prompt_hash)} · {shortHash(evaluation.challenger_model_hash)}</dd></div>
    </dl>
    <h3>评价运行时间线</h3>
    <p>只读展示数据库登记槽和已持久化的双通道进度；不会从页面推进、恢复或终结评价。</p>
    {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- bounded table is keyboard-scrollable */}
    <div className="table-scroll" role="region" aria-label="Agent 版本评价运行时间线，可横向滚动" tabIndex={0}><table className="paper-comparison-table"><thead><tr><th>槽位</th><th>登记时间</th><th>总体状态</th><th>incumbent</th><th>challenger</th><th>原因码</th></tr></thead><tbody>
      {evaluation.slots.map((slot, index) => <tr key={slot.decision_slot_at}>
        <th scope="row">{slot.phase === "train" ? `训练 ${index + 1}` : `样本外 ${index - 3}`}</th>
        <td><time className="numeric-data" dateTime={slot.decision_slot_at}>{formatDisplayTime(slot.decision_slot_at)}</time></td>
        <td>{slotStatusLabels[slot.status]}</td>
        <td>{laneStatus(slot.incumbent)}</td>
        <td>{laneStatus(slot.challenger)}</td>
        <td className="numeric-data">{slot.reason_code}</td>
      </tr>)}
    </tbody></table></div>
    {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- bounded table is keyboard-scrollable */}
    <div className="table-scroll" role="region" aria-label="Agent 版本绩效对照，可横向滚动" tabIndex={0}><table className="paper-comparison-table numeric-data"><thead><tr><th>通道</th><th>NAV</th><th>收益</th><th>最大回撤</th><th>换手</th><th>成本</th><th>订单 / 成交</th><th>硬风险异常</th></tr></thead><tbody>
      <LaneRow label="incumbent" metrics={evaluation.incumbent_metrics} />
      <LaneRow label="challenger" metrics={evaluation.challenger_metrics} />
    </tbody></table></div>
    <ul className="agent-version-evaluation__comparisons">{evaluation.comparisons.map((comparison) => <li key={comparison.key}>{comparisonLabels[comparison.key]}：{comparison.data_state === "READY" ? "同窗事实就绪" : "同窗事实不足"}（{comparison.reason_code}）</li>)}</ul>
    <aside className="inline-notice" role="note" aria-label="Agent 版本评价结论"><strong>{statusLabels[evaluation.status]}</strong><p className="numeric-data">{evaluation.reason_code}</p>{evaluation.gates.length > 0 ? <ul>{evaluation.gates.map((gate) => <li key={gate.key}>{gate.key}：{gate.status} · {gate.reason_code}</li>)}</ul> : <p>尚未终结，不展示推测性门禁结果。</p>}</aside>
  </section>;
}

function laneStatus(lane: AgentVersionEvaluationFact["slots"][number]["incumbent"]): string {
  const label = laneStatusLabels[lane.status];
  return lane.result_code ? `${label} · ${lane.result_code}` : label;
}

function LaneRow({ label, metrics }: {
  label: string;
  metrics: AgentVersionEvaluationFact["incumbent_metrics"];
}): React.ReactElement {
  return <tr><th scope="row">{label}</th>{metrics ? <>
    <td>USD {formatPaperDecimal(metrics.nav, 2)}</td><td>{formatPercent(metrics.total_return_ratio)}</td><td>{formatPercent(metrics.max_drawdown_ratio)}</td><td>{formatPercent(metrics.turnover_ratio)}</td><td>USD {formatPaperDecimal(metrics.total_cost, 2)}</td><td>{metrics.order_count} / {metrics.fill_count}</td><td>{metrics.hard_risk_count}</td>
  </> : <td colSpan={7}>尚无已保存绩效快照</td>}</tr>;
}

function shortHash(value: string): string {
  return `${value.slice(0, 12)}…`;
}
