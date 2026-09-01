"use client";

import { ArrowRight, RefreshCw } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { AuthorityLedger } from "../../components/authority-ledger";
import { IntelligenceHeader } from "../../components/intelligence-header";
import { OptimizationNeedPanel } from "../../components/diagnostic-list";
import { PortfolioValuationSummary } from "../../components/portfolio-summary";
import type { components } from "../../lib/generated/schema";

type Overview = components["schemas"]["OverviewResponse"];
type OverviewEnvelope = components["schemas"]["OverviewEnvelope"];
type PortfolioSnapshot = components["schemas"]["PortfolioSnapshotResponse"];
type Props = { state: DataStateKind; overview: Overview | null; onRefresh: () => void };

export default function OverviewPage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [overview, setOverview] = useState<Overview | null>(null);
  const overviewRef = useRef<Overview | null>(null);
  const refresh = useCallback(async () => {
    setState("loading");
    try {
      const response = await fetch("/api/v1/overview", { headers: { accept: "application/json" } });
      if (response.status === 403) { setOverview(null); setState("forbidden"); return; }
      if (response.status === 404) { setOverview(null); setState("empty"); return; }
      if (!response.ok) { setState(response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const data = (await response.json() as OverviewEnvelope).data;
      overviewRef.current = data; setOverview(data);
      setState("ready");
    } catch { setState(overviewRef.current ? "stale" : "error"); }
  }, []);
  useEffect(() => { const timer = globalThis.setTimeout(() => void refresh(), 0); return () => globalThis.clearTimeout(timer); }, [refresh]);
  return <OverviewPanel state={state} overview={overview} onRefresh={() => void refresh()} />;
}

export function OverviewPanel({ state, overview, onRefresh }: Props): React.ReactElement {
  const hasData = overview !== null && ["ready", "partial", "stale"].includes(state);
  const factsOnly = overview?.overview_mode === "PORTFOLIO_FACTS_ONLY"
    && overview.portfolio_snapshot !== null
    && overview.provider !== null;
  const optimizeHref = overview
    ? `/optimize?portfolio_id=${encodeURIComponent(overview.portfolio_id)}${overview.trading_day ? `&session_date=${overview.trading_day}` : ""}`
    : "/optimize";
  return (
    <section className="page-stack overview-page" aria-label="投资概览页面" data-state={state}>
      <IntelligenceHeader
        eyebrow={factsOnly ? "资产事实台" : "每日决策台"}
        title={factsOnly ? "今日资产概览" : "今日组合判断"}
        description={factsOnly ? "展示冻结的复星只读事实；尚未生成诊断、策略或交易建议。" : "先判断是否值得进一步优化。所有结论均来自冻结快照与确定性阈值，不使用 Agent 主观评分。"}
        actions={<><span className="mode-badge">仅研究 <code>RESEARCH_ONLY</code></span><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />{factsOnly ? "刷新概览" : "刷新判断"}</button></>}
      />
      {!hasData ? <DataState state={state} title={stateTitle(state)} detail={stateDetail(state)}>
        {state === "empty" ? <a className="button button--primary" href="/market">研究美股证据<ArrowRight aria-hidden="true" size={17} /></a> : null}
      </DataState> : factsOnly ? <PortfolioFactsOverview overview={overview} /> : overview.diagnostic_report ? <>
        {state === "stale" ? <p className="inline-notice inline-notice--error" role="status">显示最后已知结果；当前请求失败，不可用于新动作。</p> : null}
        <OptimizationNeedPanel report={overview.diagnostic_report} />
        <nav className="overview-links" aria-label="概览后续动作"><a className="button button--primary" href={optimizeHref}>打开组合诊断<ArrowRight aria-hidden="true" size={17} /></a>{overview.proposal_group ? <a className="button button--quiet" href={`/optimize/${overview.proposal_group.proposal_group_id}`}>查看三方案比较</a> : null}</nav>
        <section className="overview-fact-band" aria-label="冻结组合事实"><div><p className="eyebrow">决策证据</p><h2>只读事实快照</h2><p>用于复核判断来源，不会直接触发任何交易动作。</p></div><dl className="numeric-data"><div><dt>数据截止</dt><dd>{formatUtc(overview.snapshot_as_of)}</dd></div><div><dt>交易日</dt><dd>{overview.trading_day ?? "未知"}</dd></div><div><dt>诊断策略</dt><dd>{overview.diagnostic_report.policy_version}</dd></div><div><dt>组合 ID</dt><dd>{overview.portfolio_id}</dd></div></dl></section>
      </> : <DataState state="partial" title="权威证据不完整" detail="默认保持现状，不创建方案或订单。" />}
    </section>
  );
}

function PortfolioFactsOverview({ overview }: { overview: Overview }): React.ReactElement {
  const snapshot = overview.portfolio_snapshot!;
  const provider = overview.provider!;
  const currencies = [...new Set([
    ...snapshot.cash.map((bucket) => bucket.currency),
    ...snapshot.positions.map((position) => position.currency),
  ])].sort();
  return <>
    <section className="overview-fact-band" aria-label="复星只读组合事实">
      <div><p className="eyebrow">复星只读事实</p><h2>尚无诊断，保持现状</h2><p>{diagnosticUnavailableMessage(overview.diagnostic_unavailable_reason)}</p></div>
      <dl className="numeric-data">
        <div><dt>数据来源</dt><dd>复星（只读）</dd></div>
        <div><dt>质量状态</dt><dd>{snapshotStatusLabel(snapshot.status)} <code>{snapshot.status}</code></dd></div>
        <div><dt>快照截止</dt><dd>{formatUtc(overview.snapshot_as_of)}</dd></div>
        <div><dt>组合</dt><dd>{overview.portfolio_name ?? "复星只读组合"}</dd></div>
      </dl>
    </section>
    <AuthorityLedger snapshotStatus={snapshot.status} displayReadiness={snapshot.display_readiness} decisionReadiness={snapshot.decision_readiness} />
    {currencies.map((currency) => <PortfolioValuationSummary
      key={currency}
      provider={provider}
      snapshot={snapshot}
      currency={currency}
    />)}
    <nav className="overview-links" aria-label="概览后续动作">
      <a className="button button--primary" href="/portfolio">查看组合事实<ArrowRight aria-hidden="true" size={17} /></a>
      <a className="button button--quiet" href="/market">查看持仓行情</a>
    </nav>
  </>;
}

function diagnosticUnavailableMessage(reason: Overview["diagnostic_unavailable_reason"]): string {
  if (reason === "SNAPSHOT_NOT_READY") return "唯一缺失条件：冻结快照尚未达到 READY；保持现状。";
  return "唯一缺失条件：尚无已持久化诊断；保持现状。";
}

function stateTitle(state: DataStateKind): string {
  if (state === "empty") return "尚无可用诊断";
  if (state === "forbidden") return "当前身份无权读取概览";
  if (state === "partial") return "权威证据不完整";
  return state === "loading" ? "正在读取冻结诊断" : "概览暂时不可用";
}
function stateDetail(state: DataStateKind): string { return state === "partial" || state === "stale" ? "默认保持现状，不创建方案或订单。" : "可稍后刷新；页面不会用假数据补齐。"; }
function snapshotStatusLabel(status: PortfolioSnapshot["status"]): string { return status === "READY" ? "已就绪" : status === "STALE" ? "已过期" : "需要对账"; }
function formatUtc(value: string): string { return value.replace("T", " ").replace("Z", " UTC"); }
