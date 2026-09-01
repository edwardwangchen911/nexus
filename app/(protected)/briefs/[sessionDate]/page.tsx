"use client";

import { RefreshCw } from "lucide-react";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../../components/data-state";
import { DecisionPath } from "../../../components/decision-path";
import type { components } from "../../../lib/generated/schema";

type Brief = components["schemas"]["DailyBriefResponse"];
type BriefEnvelope = components["schemas"]["DailyBriefEnvelope"];
type BriefItem = components["schemas"]["DailyBriefItem"];

export default function DailyBriefPage(): React.ReactElement {
  const params = useParams<{ sessionDate: string }>();
  const sessionDate = params?.sessionDate ?? "";
  const [state, setState] = useState<DataStateKind>(sessionDate ? "loading" : "empty");
  const [brief, setBrief] = useState<Brief | null>(null);
  const briefRef = useRef<Brief | null>(null);
  const refresh = useCallback(async () => {
    if (!sessionDate) { setBrief(null); setState("empty"); return; }
    if (!briefRef.current) setState("loading");
    try {
      const response = await fetch(`/api/v1/briefs/${encodeURIComponent(sessionDate)}`, { headers: { accept: "application/json" } });
      if (response.status === 403) { briefRef.current = null; setBrief(null); setState("forbidden"); return; }
      if (response.status === 404) { briefRef.current = null; setBrief(null); setState("empty"); return; }
      if (!response.ok) { setState(briefRef.current ? "stale" : response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const data = (await response.json() as BriefEnvelope).data;
      briefRef.current = data;
      setBrief(data);
      setState(data.truncated || data.evidence_completeness.partial > 0 || data.evidence_completeness.failed > 0 ? "partial" : "ready");
    } catch { setState(briefRef.current ? "stale" : "error"); }
  }, [sessionDate]);
  useEffect(() => { const timer = globalThis.setTimeout(() => void refresh(), 0); return () => globalThis.clearTimeout(timer); }, [refresh]);
  return <BriefPanel state={state} brief={brief} onRefresh={() => void refresh()} />;
}

export function BriefPanel({ state, brief, onRefresh }: { state: DataStateKind; brief: Brief | null; onRefresh: () => void }): React.ReactElement {
  const hasData = brief !== null && ["ready", "partial", "stale"].includes(state);
  return (
    <section className="page-stack brief-page" aria-label="每日 Agent 简报" data-state={state}>
      <header className="page-heading">
        <div><p className="eyebrow">每日研究简报</p><h1>每日 Agent 简报</h1><p>按证据完整度分列候选、持仓风险、否决与预算降级项；点击标的进入同一时点的决策回放。</p></div>
        <div className="page-actions"><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />重新读取简报</button></div>
      </header>
      <DecisionPath current="research" />
      {!hasData ? <DataState state={state} title={state === "empty" ? "该 session 暂无简报" : "每日简报暂时不可用"} detail="简报只读取已持久化的 Agent 结果，不在页面调用模型。" /> : <>
        {state !== "ready" ? <p className="research-page__gate" role="note">简报为 {dataStateLabels[state]}；保留最后内容，但不能作为策略目标输入。</p> : null}
        <section className="brief-summary" aria-labelledby="brief-completeness-title">
          <div><p className="eyebrow">交易会话</p><h2 id="brief-completeness-title">{brief.session_date} 证据完整度</h2></div>
          <dl>
            <div><dt>总运行</dt><dd>{brief.evidence_completeness.total_runs}</dd></div>
            <div><dt>完整</dt><dd>{brief.evidence_completeness.complete}</dd></div>
            <div><dt>部分</dt><dd>{brief.evidence_completeness.partial}</dd></div>
            <div><dt>失败</dt><dd>{brief.evidence_completeness.failed}</dd></div>
          </dl>
        </section>
        <div className="brief-grid">
          <BriefSection title="研究候选" items={brief.candidates} asOf={brief.generated_at} />
          <BriefSection title="持仓风险" items={brief.holding_risks} asOf={brief.generated_at} />
          <BriefSection title="被否决项" items={brief.rejected} asOf={brief.generated_at} />
          <BriefSection title="预算降级观察" items={brief.budget_exhausted_watch} asOf={brief.generated_at} />
        </div>
        <section className="brief-ledger" aria-labelledby="brief-disagreement-title"><h2 id="brief-disagreement-title">委员会分歧</h2>{brief.disagreements.length ? <ul>{brief.disagreements.map((item) => <li key={item.run_id}><a href={researchHref(item.instrument_id, brief.generated_at)}>{item.instrument_id}</a><p>{item.summary}</p></li>)}</ul> : <p>没有记录到分歧。</p>}</section>
        <section className="brief-ledger" aria-labelledby="brief-invalidation-title"><h2 id="brief-invalidation-title">失效条件</h2>{brief.invalidations.length ? <ul>{brief.invalidations.map((item) => <li key={item.run_id}><a href={researchHref(item.instrument_id, brief.generated_at)}>{item.instrument_id}</a><p>{item.conditions.map((condition) => `${condition.signal} ${condition.operator} ${String(condition.threshold)}`).join("；")}</p></li>)}</ul> : <p>没有可展示的失效条件。</p>}</section>
      </>}
    </section>
  );
}

function BriefSection({ title, items, asOf }: { title: string; items: BriefItem[]; asOf: string }): React.ReactElement {
  return <section className="brief-section" aria-label={title}><h2>{title}</h2>{items.length ? <ul>{items.map((item) => <li key={item.run_id}><a href={researchHref(item.instrument_id, asOf)}>{item.instrument_id}</a><span>{item.stance ?? item.status}</span>{item.reason ? <code>{item.reason}</code> : null}</li>)}</ul> : <p>本 session 没有此类记录。</p>}</section>;
}

function researchHref(instrumentId: string, asOf: string): string {
  return `/research/${instrumentId}?as_of=${encodeURIComponent(asOf)}`;
}
