"use client";

import { Layers3, RefreshCw } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { DecisionPath } from "../../components/decision-path";
import { DiagnosticList, OptimizationNeedPanel } from "../../components/diagnostic-list";
import type { components } from "../../lib/generated/schema";

type Overview = components["schemas"]["OverviewResponse"];
type OverviewEnvelope = components["schemas"]["OverviewEnvelope"];
type Admission = components["schemas"]["ProposalAdmissionResponse"];
type JobEnvelope = components["schemas"]["OptimizationJobEnvelope"];
type Notice = { kind: "success" | "error"; text: string } | null;
type Props = { state: DataStateKind; overview: Overview | null; creating: boolean; notice: Notice; onCreate: (admission: Admission) => Promise<void>; onRefresh: () => void };

export default function OptimizePage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [overview, setOverview] = useState<Overview | null>(null);
  const [creating, setCreating] = useState(false);
  const [notice, setNotice] = useState<Notice>(null);
  const pendingRef = useRef<{ body: string; key: string } | null>(null);
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

  async function create(admission: Admission): Promise<void> {
    const body = JSON.stringify(admission);
    const pending = pendingRef.current?.body === body ? pendingRef.current : { body, key: idempotencyKey("proposal") };
    pendingRef.current = pending; setCreating(true); setNotice(null);
    try {
      const response = await fetch("/api/v1/optimization-proposals", { method: "POST", headers: { "content-type": "application/json", "idempotency-key": pending.key }, body });
      if (!response.ok) {
        const code = await safeErrorCode(response);
        if (response.status < 500) pendingRef.current = null;
        throw new Error(code);
      }
      const envelope = await response.json() as JobEnvelope;
      pendingRef.current = null;
      setNotice({ kind: "success", text: `三方案任务已入队：${envelope.data.job_id}。未创建订单。` });
    } catch (error) {
      setNotice({ kind: "error", text: String(error).includes("IDEMPOTENCY_CONFLICT") ? "幂等键与请求不一致；未创建新任务。" : "提案结果尚未确定；重试将沿用同一幂等键。" });
      throw error;
    } finally { setCreating(false); }
  }
  return <OptimizePanel state={state} overview={overview} creating={creating} notice={notice} onCreate={create} onRefresh={() => void refresh()} />;
}

export function OptimizePanel({ state, overview, creating, notice, onCreate, onRefresh }: Props): React.ReactElement {
  const diagnosticOverview = overview?.diagnostic_report ? overview : null;
  const hasData = diagnosticOverview !== null && ["ready", "partial", "stale"].includes(state);
  const admission = diagnosticOverview?.proposal_admission ?? null;
  return (
    <section className="page-stack optimize-page" aria-label="组合优化页面" data-state={state}>
      <header className="page-heading"><div><p className="eyebrow">组合决策诊断</p><h1>组合诊断与优化</h1><p>先确认问题和准入边界，再比较同口径三方案。风险和权重始终由后端确定性计算。</p></div><div className="page-actions"><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />刷新诊断</button></div></header>
      <DecisionPath current="diagnose" />
      {!hasData || diagnosticOverview === null ? <DataState state={state} title={state === "loading" ? "正在读取诊断" : "诊断暂时不可用"} detail="诊断数据缺失或不可用，暂不能判断是否需要调整；请核查数据后刷新。" /> : <>
        <OptimizationNeedPanel report={diagnosticOverview.diagnostic_report!} />
        <section className="proposal-admission" aria-labelledby="proposal-admission-title"><div><p className="eyebrow">服务器准入边界</p><h2 id="proposal-admission-title">三方案准入</h2><p>{admission ? "服务器已绑定快照、数据版本与成本策略，可以开始比较。"
          : diagnosticOverview.proposal_admission_reason === "OPTIMIZATION_AUTHORITY_MISSING"
            ? "当前不可创建：缺少有效的三方案准入记录，请先核验所需的快照、数据版本与成本策略。"
            : "当前不可创建：服务器未提供可用准入信息，请核查准入状态后刷新。"}</p>
          {!admission && diagnosticOverview.proposal_admission_reason ? <details><summary>查看原因代码</summary><code>{diagnosticOverview.proposal_admission_reason}</code></details> : null}</div>
          {diagnosticOverview.proposal_group ? <a className="button button--quiet" href={`/optimize/${diagnosticOverview.proposal_group.proposal_group_id}`}>打开已有三方案</a> : admission && state === "ready" ? <button className="button button--primary" type="button" disabled={creating} onClick={() => void onCreate(admission).catch(() => undefined)}><Layers3 aria-hidden="true" size={17} />{creating ? "正在创建" : "创建三方案比较"}</button> : null}
        </section>
        {notice ? <p className={`inline-notice inline-notice--${notice.kind}`} role="status">{notice.text}</p> : null}
        <DiagnosticList report={diagnosticOverview.diagnostic_report!} />
      </>}
    </section>
  );
}

function readOverviewQuery(): string { if (typeof location === "undefined") return ""; const source = new URLSearchParams(location.search); const output = new URLSearchParams(); for (const key of ["portfolio_id", "session_date"]) { const value = source.get(key); if (value) output.set(key, value); } return output.toString(); }
function idempotencyKey(prefix: string): string { return `${prefix}-${globalThis.crypto?.randomUUID?.() ?? Date.now().toString(36)}`; }
async function safeErrorCode(response: Response): Promise<string> { try { return ((await response.json()) as { error?: { code?: string } }).error?.code ?? "REQUEST_FAILED"; } catch { return "REQUEST_FAILED"; } }
