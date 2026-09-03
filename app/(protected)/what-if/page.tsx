"use client";

import { RefreshCw } from "lucide-react";
import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { DecisionPath } from "../../components/decision-path";
import { WhatIfEditor } from "../../components/what-if-editor";
import type { components } from "../../lib/generated/schema";

type Group = components["schemas"]["OptimizationProposalGroupResponse"];
type GroupEnvelope = components["schemas"]["OptimizationProposalGroupEnvelope"];
type WhatIfRequest = components["schemas"]["WhatIfRequest"];
type WhatIfResponse = components["schemas"]["WhatIfResponse"];
type WhatIfEnvelope = components["schemas"]["WhatIfEnvelope"];
type Props = { state: DataStateKind; group: Group | null; proposalId: string | null; onRefresh: () => void; onEvaluate: (request: WhatIfRequest) => Promise<WhatIfResponse> };

export default function WhatIfPage(): React.ReactElement {
  const search = useSearchParams();
  const groupId = search.get("proposal_group_id");
  const proposalId = search.get("proposal_id");
  const [state, setState] = useState<DataStateKind>(groupId && proposalId ? "loading" : "empty");
  const [group, setGroup] = useState<Group | null>(null);
  const keys = useRef(new Map<string, string>());
  const groupRef = useRef<Group | null>(null);
  const refresh = useCallback(async () => {
    if (!groupId || !proposalId) { setGroup(null); setState("empty"); return; }
    setState("loading");
    try {
      const response = await fetch(`/api/v1/optimization-proposals/${encodeURIComponent(groupId)}`, { headers: { accept: "application/json" } });
      if (response.status === 403) { setGroup(null); setState("forbidden"); return; }
      if (response.status === 404) { setGroup(null); setState("empty"); return; }
      if (!response.ok) { setState(response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const data = (await response.json() as GroupEnvelope).data;
      groupRef.current = data; setGroup(data); setState("ready");
    } catch { setState(groupRef.current ? "stale" : "error"); }
  }, [groupId, proposalId]);
  useEffect(() => { const timer = globalThis.setTimeout(() => void refresh(), 0); return () => globalThis.clearTimeout(timer); }, [refresh]);

  async function evaluate(request: WhatIfRequest): Promise<WhatIfResponse> {
    const body = JSON.stringify(request);
    const key = keys.current.get(body) ?? `what-if-${globalThis.crypto?.randomUUID?.() ?? Date.now().toString(36)}`;
    keys.current.set(body, key);
    const response = await fetch("/api/v1/what-if/evaluate", { method: "POST", headers: { "content-type": "application/json", "idempotency-key": key }, body });
    if (!response.ok) { if (response.status < 500) keys.current.delete(body); throw new Error(await safeErrorCode(response)); }
    keys.current.delete(body);
    return (await response.json() as WhatIfEnvelope).data;
  }
  return <WhatIfPagePanel state={state} group={group} proposalId={proposalId} onRefresh={() => void refresh()} onEvaluate={evaluate} />;
}

export function WhatIfPagePanel({ state, group, proposalId, onRefresh, onEvaluate }: Props): React.ReactElement {
  const hasData = group !== null && proposalId !== null && ["ready", "partial", "stale"].includes(state);
  return <section className="page-stack what-if-page" aria-label="持仓权重情景试算页面" data-state={state}>
    <header className="page-heading"><div><p className="eyebrow">方案调整预览</p><h1>持仓权重情景试算</h1><p>基于已有方案调整持仓权重，查看风险与约束变化；结果仅供分析。</p></div><div className="page-actions"><span className="mode-badge">仅分析，不保存</span><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />重新读取方案</button></div></header>
    <DecisionPath current="what-if" />
    {!hasData ? <DataState state={state} title={state === "empty" ? "缺少方案上下文" : "情景试算暂时不可用"} detail="请从已有方案进入试算，当前缺少用于比较的方案。" /> : <WhatIfEditor group={group} proposalId={proposalId} actionsEnabled={state === "ready"} onEvaluate={onEvaluate} />}
  </section>;
}

async function safeErrorCode(response: Response): Promise<string> { try { return ((await response.json()) as { error?: { code?: string } }).error?.code ?? "REQUEST_FAILED"; } catch { return "REQUEST_FAILED"; } }
