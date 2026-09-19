import { ArrowRight, CalendarDays } from "lucide-react";
import { useMemo, useState } from "react";

import { formatDisplayTime } from "../../lib/display-format";
import { cycleDecisionPresentation, riskPresentation } from "../../lib/product-status";
import type { components } from "../../lib/generated/schema";

export type DecisionCycle = components["schemas"]["AgentPaperCycleResponse"];
type DecisionKey = "buy" | "sell" | "hold" | "watch" | "data_insufficient" | "model_failed" | "other";

const decisionOptions: Array<["" | DecisionKey, string]> = [
  ["", "全部类型"], ["buy", "买入"], ["sell", "减仓"], ["hold", "持有"], ["watch", "观察"],
  ["data_insufficient", "数据不足"], ["model_failed", "模型失败"], ["other", "其他"],
];
const statusOptions: Array<["" | DecisionCycle["status"], string]> = [
  ["", "全部状态"], ["prepared", "已准备"], ["researching", "研究中"], ["decision_recorded", "决策已记录"],
  ["risk_evaluated", "风控已评估"], ["awaiting_market", "等待行情"], ["partially_filled", "部分成交或待核对"],
  ["completed", "已完成"], ["unavailable", "数据不足"], ["failed", "模型失败"],
];

export function DecisionList({ cycles }: { cycles: DecisionCycle[] }): React.ReactElement {
  const [date, setDate] = useState("");
  const [decisionType, setDecisionType] = useState<"" | DecisionKey>("");
  const [status, setStatus] = useState<"" | DecisionCycle["status"]>("");
  const filtered = useMemo(() => cycles.filter((cycle) =>
    (!date || cycle.decision_session === date)
    && (!decisionType || decisionKey(cycle) === decisionType)
    && (!status || cycle.status === status)), [cycles, date, decisionType, status]);

  return <section className="decision-records" aria-label="决策记录列表">
    <div className="decision-filters" aria-label="决策记录筛选">
      <label><span>决策日期</span><input type="date" value={date} onChange={(event) => setDate(event.target.value)} /></label>
      <label><span>决策类型</span><select value={decisionType} onChange={(event) => setDecisionType(event.target.value as "" | DecisionKey)}>{decisionOptions.map(([value, label]) => <option key={value || "all"} value={value}>{label}</option>)}</select></label>
      <label><span>最终状态</span><select value={status} onChange={(event) => setStatus(event.target.value as "" | DecisionCycle["status"])}>{statusOptions.map(([value, label]) => <option key={value || "all"} value={value}>{label}</option>)}</select></label>
    </div>
    <p className="decision-records__count">当前筛选显示 <strong className="numeric-data">{filtered.length} / {cycles.length}</strong> 条已加载记录</p>
    {filtered.length ? <ul aria-label="决策记录">
      {filtered.map((cycle) => <DecisionRow key={cycle.cycle_id} cycle={cycle} />)}
    </ul> : <div className="decision-records__empty"><CalendarDays aria-hidden="true" size={22} /><p>当前筛选下没有已加载的决策记录。</p></div>}
  </section>;
}

function DecisionRow({ cycle }: { cycle: DecisionCycle }): React.ReactElement {
  const decision = cycleDecisionPresentation(cycle);
  const risk = riskPresentation(cycle);
  const candidate = decision.candidates[0] ?? null;
  const final = finalPresentation(cycle, risk.label);
  return <li>
    <article className="decision-row" data-tone={final.tone}>
      <time className="numeric-data" dateTime={cycle.decision_time}>{formatDisplayTime(cycle.decision_time)}</time>
      <div className="decision-row__decision"><small>决策</small><strong>{candidate?.label ?? decision.summary.label}</strong><span>{candidate?.symbol ?? "无候选标的"}</span></div>
      <div className="decision-row__result"><small>结果</small><strong>{final.label}</strong><code>{cycle.result_code}</code></div>
      <a className="button button--quiet" href={`/decisions/${cycle.cycle_id}`} aria-label={`查看决策 ${cycle.cycle_id.slice(0, 8)}`}>查看决策<ArrowRight aria-hidden="true" size={16} /></a>
    </article>
  </li>;
}

function decisionKey(cycle: DecisionCycle): DecisionKey {
  const summary = cycleDecisionPresentation(cycle);
  if (summary.summary.key === "data_insufficient" || summary.summary.key === "model_failed") return summary.summary.key;
  return summary.candidates[0]?.key ?? "other";
}

function finalPresentation(cycle: DecisionCycle, riskLabel: string): { label: string; tone: string } {
  if (cycle.status === "unavailable") return { label: "数据不足", tone: "warning" };
  if (cycle.status === "failed") return { label: "模型失败", tone: "critical" };
  if (riskLabel === "风控拒绝") return { label: "风控拒绝", tone: "critical" };
  if (cycle.status === "partially_filled") return cycle.order_ids.length
    ? { label: "部分成交", tone: "warning" }
    : { label: "结果未知", tone: "critical" };
  if (cycle.status === "completed") return { label: "已完成", tone: "positive" };
  return { label: statusOptions.find(([value]) => value === cycle.status)?.[1] ?? "未知", tone: "neutral" };
}

export function isDecisionCycle(value: unknown): value is DecisionCycle {
  if (!isPlain(value) || typeof value.cycle_id !== "string" || typeof value.decision_time !== "string"
    || typeof value.decision_session !== "string" || value.execution_locked !== true
    || !["prepared", "researching", "decision_recorded", "risk_evaluated", "awaiting_market", "partially_filled", "completed", "unavailable", "failed"].includes(String(value.status))
    || typeof value.result_code !== "string" || !Array.isArray(value.candidates) || !Array.isArray(value.order_ids)
    || !value.order_ids.every((id) => typeof id === "string")) return false;
  if (!value.candidates.every((candidate) => isPlain(candidate) && typeof candidate.instrument_id === "string"
    && typeof candidate.symbol === "string" && Array.isArray(candidate.selection_reasons)
    && ["pending", "completed", "unavailable"].includes(String(candidate.research_status)))) return false;
  if (value.decision !== null && (!isPlain(value.decision) || !Array.isArray(value.decision.risk_outcomes))) return false;
  return isPlain(value.execution_market) && (value.decision_market === null || isPlain(value.decision_market));
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
