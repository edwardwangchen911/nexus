"use client";

import { RefreshCw } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { DecisionList, isDecisionCycle } from "../../components/decisions/decision-list";
import type { components } from "../../lib/generated/schema";

type CyclePage = components["schemas"]["AgentPaperCyclePageResponse"];

export default function DecisionsPage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [page, setPage] = useState<CyclePage | null>(null);
  const pageRef = useRef<CyclePage | null>(null);
  const refresh = useCallback(async (): Promise<void> => {
    if (!pageRef.current) setState("loading");
    try {
      const response = await fetch("/api/v1/paper/cycles?limit=20", { cache: "no-store", signal: AbortSignal.timeout(10_000) });
      if (response.status === 401 || response.status === 403) { pageRef.current = null; setPage(null); setState("forbidden"); return; }
      if (response.status === 404) { pageRef.current = null; setPage(null); setState("empty"); return; }
      if (!response.ok) { setState(pageRef.current ? "stale" : response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const envelope: unknown = await response.json();
      if (!isCyclePageEnvelope(envelope)) throw new Error("invalid cycle page");
      pageRef.current = envelope.data;
      setPage(envelope.data);
      setState(envelope.data.items.length ? "ready" : "empty");
    } catch { setState(pageRef.current ? "stale" : "error"); }
  }, []);
  useEffect(() => { const timer = globalThis.setTimeout(() => void refresh(), 0); return () => globalThis.clearTimeout(timer); }, [refresh]);
  return <DecisionsPanel state={state} page={page} onRefresh={() => void refresh()} />;
}

export function DecisionsPanel({ state, page, onRefresh }: { state: DataStateKind; page: CyclePage | null; onRefresh: () => void }): React.ReactElement {
  const hasFacts = page !== null && ["ready", "partial", "stale"].includes(state);
  const title = ({ loading: "正在读取决策记录", empty: "暂无决策记录", ready: "决策记录已就绪", partial: "决策记录不完整", stale: "显示上次决策记录", error: "决策记录加载失败", forbidden: "无权读取决策记录" } as const)[state];
  return <section className="decisions-page" data-state={state} aria-label="决策记录页面">
    <header className="page-heading decisions-page__heading"><div><p className="eyebrow">DECISION RECORDS</p><h1>决策记录</h1><p>按周期查看 Agent 判断、确定性风控与内部模拟结果；筛选不会重放周期。</p></div><div className="page-actions"><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />刷新记录</button></div></header>
    <p className="decisions-boundary" role="note">仅显示本次加载的最多 20 条当前会话记录 · 实盘锁定 · 页面只读。组合提案、当前生效目标与计划归属请进入对应决策详情核对。</p>
    {hasFacts ? <DecisionList cycles={page.items} /> : <DataState state={state} title={title} detail="页面只读取已保存周期，不会运行 Agent 或创建订单。" announce={false} />}
  </section>;
}

function isCyclePageEnvelope(value: unknown): value is components["schemas"]["AgentPaperCyclePageEnvelope"] {
  if (!isPlain(value) || !isPlain(value.data) || !Array.isArray(value.data.items) || value.data.items.length > 20
    || !value.data.items.every(isDecisionCycle) || !isPlain(value.data.summary) || value.data.summary.execution_locked !== true
    || !Number.isInteger(value.data.summary.cycle_count) || !Number.isInteger(value.data.summary.completed_count)
    || !isPlain(value.meta) || typeof value.meta.as_of !== "string") return false;
  return true;
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
