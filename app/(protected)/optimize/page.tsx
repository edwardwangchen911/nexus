"use client";

import { ArrowRight, RefreshCw } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { DecisionPath } from "../../components/decision-path";
import { DiagnosticList, OptimizationNeedPanel } from "../../components/diagnostic-list";
import type { components } from "../../lib/generated/schema";

type Overview = components["schemas"]["OverviewResponse"];
type OverviewEnvelope = components["schemas"]["OverviewEnvelope"];
type Props = {
  state: DataStateKind;
  overview: Overview | null;
  onRefresh: () => void;
};

export default function OptimizePage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [overview, setOverview] = useState<Overview | null>(null);
  const overviewRef = useRef<Overview | null>(null);
  const refresh = useCallback(async () => {
    setState("loading");
    try {
      const query = readOverviewQuery();
      const response = await fetch(`/api/v1/overview${query ? `?${query}` : ""}`, { headers: { accept: "application/json" } });
      if (response.status === 403) { setOverview(null); setState("forbidden"); return; }
      if (response.status === 404) { setOverview(null); setState("empty"); return; }
      if (!response.ok) { setState(response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const data = (await response.json() as OverviewEnvelope).data;
      overviewRef.current = data; setOverview(data); setState("ready");
    } catch { setState(overviewRef.current ? "stale" : "error"); }
  }, []);
  useEffect(() => { const timer = globalThis.setTimeout(() => void refresh(), 0); return () => globalThis.clearTimeout(timer); }, [refresh]);

  return <OptimizePanel state={state} overview={overview} onRefresh={() => void refresh()} />;
}

export function OptimizePanel({ state, overview, onRefresh }: Props): React.ReactElement {
  const diagnosticOverview = overview?.diagnostic_report ? overview : null;
  const hasData = diagnosticOverview !== null && ["ready", "partial", "stale"].includes(state);
  const hasReduceOnlyFinding = diagnosticOverview?.diagnostic_report?.items.some(
    (item) => item.actionability === "reduce_only",
  ) ?? false;
  return (
    <section className="page-stack optimize-page" aria-label="组合优化页面" data-state={state}>
      <header className="page-heading"><div><p className="eyebrow">组合决策诊断</p><h1>组合诊断与候选</h1><p>先确认最值得处理的问题，再由确定性代码生成一个只读候选。Agent 负责解释，不生成权重。</p></div><div className="page-actions"><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />刷新诊断</button></div></header>
      <DecisionPath current="diagnose" />
      {!hasData || diagnosticOverview === null ? <DataState state={state} title={state === "loading" ? "正在读取诊断" : "诊断暂时不可用"} detail="诊断数据缺失或不可用，暂不能判断是否需要调整；请核查数据后刷新。" /> : <>
        <OptimizationNeedPanel report={diagnosticOverview.diagnostic_report!} />
        <section className="proposal-admission" aria-labelledby="proposal-admission-title"><div><p className="eyebrow">当前决策路径</p><h2 id="proposal-admission-title">一个确定性候选</h2><p>前往组合页设置最大换手率和最低现金比例，生成一个当前持仓内的只读候选。Agent 负责解释，不生成权重。</p></div>
          <div className="page-actions"><a className="button button--primary" href="/portfolio">生成一个调整候选<ArrowRight aria-hidden="true" size={17} /></a>
            {diagnosticOverview.proposal_group ? <a className="button button--quiet" href={`/optimize/${diagnosticOverview.proposal_group.proposal_group_id}`}>查看历史三方案记录</a> : null}</div>
        </section>
        {!diagnosticOverview.proposal_group ? (
          <section className="proposal-admission" aria-label="历史方案状态">
            <div>
              <p className="eyebrow">历史只读记录</p>
              <h2>当前没有历史三方案记录</h2>
              <p>{hasReduceOnlyFinding
                ? "诊断存在降低仓位条件；新的分析请使用上方单候选入口，成本未知仍会阻止执行。"
                : "当前诊断未提供可直接用于确定性减仓的条目；未分析不构成任何保持判断。"}</p>
              <p>旧三方案只作为历史记录保留，不再作为新决策的创建入口。</p>
            </div>
          </section>
        ) : null}
        <DiagnosticList report={diagnosticOverview.diagnostic_report!} />
      </>}
    </section>
  );
}

function readOverviewQuery(): string { if (typeof location === "undefined") return ""; const source = new URLSearchParams(location.search); const output = new URLSearchParams(); for (const key of ["portfolio_id", "session_date"]) { const value = source.get(key); if (value) output.set(key, value); } return output.toString(); }
