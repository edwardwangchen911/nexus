"use client";

import { RefreshCw } from "lucide-react";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../../components/data-state";
import { ProposalComparison } from "../../../components/proposal-comparison";
import type { components } from "../../../lib/generated/schema";

type Group = components["schemas"]["OptimizationProposalGroupResponse"];
type GroupEnvelope = components["schemas"]["OptimizationProposalGroupEnvelope"];
type LifecycleEnvelope = components["schemas"]["OptimizationLifecycleEnvelope"];
type Props = { state: DataStateKind; group: Group | null; onRefresh: () => void; onStartShadow: (groupId: string, version: number) => Promise<void>; onReject: (groupId: string, version: number) => Promise<void> };

export default function ProposalGroupPage(): React.ReactElement {
  const params = useParams<{ proposalGroupId: string }>();
  const groupId = params?.proposalGroupId ?? "";
  const [state, setState] = useState<DataStateKind>("loading");
  const [group, setGroup] = useState<Group | null>(null);
  const keys = useRef(new Map<string, string>());
  const groupRef = useRef<Group | null>(null);
  const refresh = useCallback(async () => {
    if (!groupId) { setState("empty"); return; }
    setState("loading");
    try {
      const response = await fetch(`/api/v1/optimization-proposals/${encodeURIComponent(groupId)}`, { headers: { accept: "application/json" } });
      if (response.status === 403) { setGroup(null); setState("forbidden"); return; }
      if (response.status === 404) { setGroup(null); setState("empty"); return; }
      if (!response.ok) { setState(response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const data = (await response.json() as GroupEnvelope).data;
      groupRef.current = data; setGroup(data); setState("ready");
    } catch { setState(groupRef.current ? "stale" : "error"); }
  }, [groupId]);
  useEffect(() => { const timer = globalThis.setTimeout(() => void refresh(), 0); return () => globalThis.clearTimeout(timer); }, [refresh]);

  async function transition(action: "shadow" | "reject", id: string, version: number): Promise<void> {
    const identity = `${action}:${id}:${version}`;
    const key = keys.current.get(identity) ?? `proposal-${action}-${globalThis.crypto?.randomUUID?.() ?? Date.now().toString(36)}`;
    keys.current.set(identity, key);
    const response = await fetch(`/api/v1/optimization-proposals/${encodeURIComponent(id)}/${action}`, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": key, "if-match": `"${version}"` }, body: JSON.stringify({ version }) });
    if (!response.ok) {
      const code = await safeErrorCode(response);
      if (response.status < 500) keys.current.delete(identity);
      throw new Error(code);
    }
    const envelope = await response.json() as LifecycleEnvelope;
    keys.current.delete(identity);
    setGroup((current) => {
      const next = current && current.proposal_group_id === envelope.data.proposal_group_id ? { ...current, status: envelope.data.status, version: envelope.data.version } : current;
      groupRef.current = next; return next;
    });
  }
  return <ProposalGroupPanel state={state} group={group} onRefresh={() => void refresh()} onStartShadow={(id, version) => transition("shadow", id, version)} onReject={(id, version) => transition("reject", id, version)} />;
}

export function ProposalGroupPanel({ state, group, onRefresh, onStartShadow, onReject }: Props): React.ReactElement {
  const hasData = group !== null && ["ready", "partial", "stale"].includes(state);
  const actionsEnabled = state === "ready";
  const minimumChange = group?.proposals.find((proposal) => proposal.variant === "minimum_change");
  return <section className="page-stack proposal-page" aria-label="三方案比较页面" data-state={state}>
    <header className="page-heading"><div><p className="eyebrow">PROPOSAL / SAME BASELINE</p><h1>三方案比较</h1><p>固定顺序展示保持现状、最小调整与策略目标；未知值不参与优劣排序。</p></div><div className="page-actions"><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />重新加载方案</button></div></header>
    {!hasData ? <DataState state={state} title={state === "empty" ? "方案组不存在" : "方案组暂时不可用"} detail="不会从 URL 或浏览器状态构造方案。" /> : <>
      <ProposalComparison group={group} actionsEnabled={actionsEnabled} onStartShadow={onStartShadow} onReject={onReject} />
      {actionsEnabled && minimumChange && minimumChange.positions.length <= 30 ? <nav className="proposal-context-links" aria-label="方案上下文动作"><a className="button button--quiet" href={`/what-if?proposal_group_id=${group.proposal_group_id}&proposal_id=${minimumChange.proposal_id}`}>试算最小调整权重</a></nav> : null}
      {actionsEnabled && minimumChange && minimumChange.positions.length > 30 ? <p className="inline-notice">方案可继续只读比较；情景试算最多支持 30 个标的。</p> : null}
    </>}
  </section>;
}

async function safeErrorCode(response: Response): Promise<string> { try { return ((await response.json()) as { error?: { code?: string } }).error?.code ?? "REQUEST_FAILED"; } catch { return "REQUEST_FAILED"; } }
