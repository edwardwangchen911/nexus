"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";
import { validate as validateUuid } from "uuid";
import type { components } from "../../lib/generated/schema";
import { formatDisplayDecimal } from "../../lib/display-decimal";
import { formatDisplayTime } from "../../lib/display-format";
import { DataState } from "../data-state";
import { AgentPaperCycle } from "../agent-paper-cycle";
import { DecisionList, isDecisionCycle } from "../decisions/decision-list";
import { DecisionTimeline } from "../decisions/decision-timeline";
import { PortfolioDecisionDetail } from "../decisions/portfolio-decision-detail";

type View = "workspace" | "decisions" | "paper" | "portfolio" | "detail";
type Context = { pairId: string; lane: "incumbent" | "challenger" };
type Fact = components["schemas"]["EconomicLaneResponse"];
const titles: Record<View, string> = { workspace: "交易工作台", decisions: "决策记录", paper: "内部模拟盘", portfolio: "组合持仓", detail: "决策详情" };

export function EconomicContextBoundary({ view, cycleId, children }: { view: View; cycleId?: string; children: ReactNode }): React.ReactElement {
  const query = useSearchParams();
  let context: Context | null | "invalid" = null;
  if (query.has("pair_slot_id") || query.has("lane")) {
    const pairId = query.get("pair_slot_id"); const lane = query.get("lane");
    context = !pairId || !validateUuid(pairId) || (lane !== "incumbent" && lane !== "challenger")
      || [...query.keys()].some((key) => !["pair_slot_id", "lane"].includes(key))
      || query.getAll("pair_slot_id").length !== 1 || query.getAll("lane").length !== 1
      ? "invalid" : { pairId, lane };
  }
  if (context === null) return <>{children}</>;
  if (context === "invalid") return <DataState state="error" title="配对上下文无效" detail="请使用已登记配对的有效侧别链接。" />;
  return <EconomicLanePage key={`${context.pairId}:${context.lane}:${cycleId ?? ""}`} context={context} view={view} cycleId={cycleId} />;
}

function EconomicLanePage({ context, view, cycleId }: { context: Context; view: View; cycleId?: string }): React.ReactElement {
  const [fact, setFact] = useState<Fact | null>(null);
  const [state, setState] = useState<"loading" | "ready" | "stale" | "error" | "forbidden">("loading");
  const version = useRef(0);
  async function refresh() {
    const current = ++version.current;
    try {
      const response = await fetch(`/api/v1/paper/economic-pairs/${context.pairId}/lanes/${context.lane}`, { method: "GET", cache: "no-store", signal: AbortSignal.timeout(10_000) });
      if (current !== version.current) return;
      if (response.status === 401 || response.status === 403) { setFact(null); setState("forbidden"); return; }
      if (!response.ok) { setState(fact ? "stale" : "error"); return; }
      const value: unknown = await response.json();
      if (current !== version.current) return;
      if (!validLaneEnvelope(value, context, cycleId)) { setFact(null); setState("error"); return; }
      setFact(value.data); setState("ready");
    } catch { if (current === version.current) setState(fact ? "stale" : "error"); }
  }
  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => { globalThis.clearTimeout(timer); version.current += 1; };
    // Context is immutable for this keyed component; manual refresh uses current facts.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return <section className="page-stack" aria-label="经济配对只读页面" data-state={state}>
    <header className="page-heading"><div><p className="eyebrow">内部模拟 · 配对评价</p><h1>{titles[view]}</h1></div><button className="button button--quiet" type="button" onClick={() => void refresh()}>刷新配对事实</button></header>
    {state === "stale" ? <p role="status">显示上次配对事实，当前读取失败。</p> : null}
    {fact ? <EconomicLanePanel fact={fact} view={view} /> : <DataState state={state === "forbidden" ? "forbidden" : state === "loading" ? "loading" : "error"} title={state === "loading" ? "正在读取配对事实" : state === "forbidden" ? "无权读取配对事实" : "配对事实暂不可用"} detail="不会回退其他账户，也不会运行研究、采集或订单。" />}
  </section>;
}

export function economicContextQuery(pairId: string, lane: Context["lane"]): string {
  return `?pair_slot_id=${encodeURIComponent(pairId)}&lane=${lane}`;
}

export function EconomicLanePanel({ fact, view }: { fact: Fact; view: View }): React.ReactElement {
  const pair = fact.pair; const detail = fact.detail; const query = economicContextQuery(pair.pair_slot_id, fact.lane);
  const side = pair.execution?.lanes.find((lane) => lane.lane === fact.lane);
  const executionLabels = { unknown: "结果未知", unavailable: "执行不可用", partial: "部分执行", risk_rejected: "风控拒绝", filled: "已成交", not_required: "本阶段无需续买", no_trade: "合格不交易" };
  return <>
    <section className="pm-panel" aria-label="配对状态"><h2>{fact.lane === "incumbent" ? "基线侧 incumbent" : "挑战侧 challenger"}</h2>
      <p>计划槽位 {pair.ordinal} / {pair.planned_slots} · {pair.decision_session} · 实盘锁定</p>
      <p>本侧执行：{side ? executionLabels[side.status] : "尚无共同窗口结果"}。经济评价尚未完成，不代表样本外通过。</p>
      <p>配对比较：{pair.execution?.comparison_status === "incomparable" ? "不可比" : pair.execution?.comparison_status === "incomplete" ? "未完成" : "尚未评价"}</p>
      <nav aria-label="配对事实页面">{(["workspace", "decisions", "paper", "portfolio"] as const).map((page) => <a key={page} className="button button--quiet" href={`/${page}${query}`}>{titles[page]}</a>)}<a className="button button--quiet" href={`/${view === "detail" ? "decisions" : view}${economicContextQuery(pair.pair_slot_id, fact.lane === "incumbent" ? "challenger" : "incumbent")}`}>查看另一侧</a></nav>
      <p className="numeric-data">配对标识：{pair.pair_slot_id}</p>
    </section>
    {view === "decisions" ? detail ? <DecisionList cycles={[detail.cycle]} session={pair.decision_session} detailQuery={query} /> : <p>该槽位尚未启动周期。</p> : null}
    {view === "workspace" || view === "paper" ? <>
      <AgentPaperCycle cycles={detail ? [detail.cycle] : []} historical />
      {detail ? <a className="button button--quiet" href={`/decisions/${detail.cycle.cycle_id}${query}`}>查看完整决策</a> : null}
    </> : null}
    {detail && (view === "detail" || view === "paper") ? <><PortfolioDecisionDetail detail={detail} paperHref={`/paper${query}`} /><DecisionTimeline detail={detail} paperHref={`/paper${query}`} /></> : null}
    {view !== "decisions" && view !== "detail" ? <section className="pm-panel" aria-label="本侧账本与持仓"><h2>本侧账本与持仓</h2>
      <p>当前原生投影 · {formatDisplayTime(fact.actual.as_of)} · 币种 USD</p>
      <dl><div><dt>可用现金</dt><dd>{money(fact.actual.available_cash)}</dd></div><div><dt>预留现金</dt><dd>{money(fact.actual.reserved_cash)}</dd></div><div><dt>净资产</dt><dd>{money(fact.actual.nav)}</dd></div></dl>
      {fact.actual.positions === null ? <p>持仓未知</p> : fact.actual.positions.length === 0 ? <p>已核验无持仓</p> : <div className="table-scroll"><table aria-label="本侧当前持仓"><thead><tr><th>标的</th><th>数量（股）</th><th>预留数量（股）</th></tr></thead><tbody>{fact.actual.positions.map((position) => <tr key={position.instrument_id}><th>{position.symbol ?? `标的 …${position.instrument_id.slice(-8)}`}</th><td>{formatDisplayDecimal(position.quantity)}</td><td>{formatDisplayDecimal(position.reserved_quantity)}</td></tr>)}</tbody></table></div>}
      <p>冻结绩效快照：{fact.portfolio.performance ? formatDisplayTime(fact.portfolio.performance.as_of) : "尚未发布；收益未知"}。当前账本投影不代替绩效评价。</p>
    </section> : null}
  </>;
}

function money(value: string | null): string { return value === null ? "未知" : `USD ${formatDisplayDecimal(value, { fixedDigits: 2 })}`; }
function plain(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
function validLaneEnvelope(value: unknown, context: Context, cycleId?: string): value is { data: Fact } {
  if (!plain(value) || !plain(value.data) || !plain(value.data.pair)) return false;
  const data = value.data; const pair = value.data.pair;
  if (data.lane !== context.lane || typeof pair.pair_slot_id !== "string" || pair.pair_slot_id.toLowerCase() !== context.pairId.toLowerCase()
    || pair.execution_locked !== true || pair.planned_slots !== 24 || pair.economic_status !== "not_evaluated"
    || !Array.isArray(pair.lanes) || !plain(data.actual) || !plain(data.portfolio) || data.portfolio.mode !== "internal_paper" || data.portfolio.currency !== "USD") return false;
  const ref = pair.lanes.find((lane: unknown) => plain(lane) && lane.lane === context.lane);
  if (!plain(ref)) return false;
  if (ref.cycle_id === null) { if (data.detail !== null || cycleId) return false; }
  else if (!plain(data.detail) || !isDecisionCycle(data.detail.cycle) || data.detail.cycle.cycle_id !== ref.cycle_id
    || data.detail.cycle.status !== ref.cycle_status || (cycleId && cycleId !== ref.cycle_id)) return false;
  const actual = data.actual;
  if (actual.data_state === "UNKNOWN") return actual.as_of === null && actual.available_cash === null && actual.reserved_cash === null && actual.nav === null && actual.positions === null;
  return actual.data_state === "READY" && typeof actual.as_of === "string"
    && [actual.available_cash, actual.reserved_cash, actual.nav].every((item) => typeof item === "string" && /^-?\d+(\.\d+)?$/.test(item))
    && Array.isArray(actual.positions) && actual.positions.every((item: unknown) => plain(item) && typeof item.instrument_id === "string" && typeof item.quantity === "string" && typeof item.reserved_quantity === "string");
}
