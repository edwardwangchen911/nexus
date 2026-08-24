"use client";

import { RefreshCw, ScanSearch } from "lucide-react";
import { useParams, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../../components/data-state";
import { DecisionCard } from "../../../components/decision-card";
import type { components } from "../../../lib/generated/schema";

type Research = components["schemas"]["ResearchRunResponse"];
type ResearchEnvelope = components["schemas"]["ResearchRunEnvelope"];
type Notice = { kind: "error" | "success"; text: string } | null;

type Props = {
  state: DataStateKind;
  research: Research | null;
  submitting: boolean;
  notice: Notice;
  onRefresh: () => void;
  onRun: () => void;
};

export default function ResearchPage(): React.ReactElement {
  const params = useParams<{ instrumentId: string }>();
  const search = useSearchParams();
  const instrumentId = params?.instrumentId ?? "";
  const asOf = search.get("as_of") ?? "";
  const [state, setState] = useState<DataStateKind>(instrumentId && asOf ? "loading" : "empty");
  const [research, setResearch] = useState<Research | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [notice, setNotice] = useState<Notice>(null);
  const researchRef = useRef<Research | null>(null);
  const idempotencyKeys = useRef(new Map<string, string>());

  const refresh = useCallback(async () => {
    if (!instrumentId || !asOf) { setResearch(null); setState("empty"); return; }
    setNotice(null);
    if (!researchRef.current) setState("loading");
    try {
      const response = await fetch(`/api/v1/instruments/${encodeURIComponent(instrumentId)}/research?as_of=${encodeURIComponent(asOf)}`, { headers: { accept: "application/json" } });
      if (response.status === 403) { researchRef.current = null; setResearch(null); setState("forbidden"); return; }
      if (response.status === 404) { researchRef.current = null; setResearch(null); setState("empty"); return; }
      if (!response.ok) { setState(researchRef.current ? "stale" : response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const data = (await response.json() as ResearchEnvelope).data;
      researchRef.current = data;
      setResearch(data);
      setState(classifyResearch(data));
    } catch {
      setState(researchRef.current ? "stale" : "error");
      setNotice({ kind: "error", text: "研究读取失败；如有最后已知证据会继续保留，但不能创建策略目标。" });
    }
  }, [asOf, instrumentId]);

  useEffect(() => { const timer = globalThis.setTimeout(() => void refresh(), 0); return () => globalThis.clearTimeout(timer); }, [refresh]);

  async function runFrozenResearch(): Promise<void> {
    if (state !== "ready" || research === null) return;
    const body = JSON.stringify({ instrument_id: research.instrument_id, data_snapshot_id: research.data_snapshot_id, decision_time: research.decision_time });
    const key = idempotencyKeys.current.get(body) ?? `research-${globalThis.crypto?.randomUUID?.() ?? Date.now().toString(36)}`;
    idempotencyKeys.current.set(body, key);
    setSubmitting(true);
    setNotice(null);
    try {
      const response = await fetch("/api/v1/research-runs", { method: "POST", headers: { "content-type": "application/json", "idempotency-key": key }, body });
      if (!response.ok) {
        if (response.status < 500) idempotencyKeys.current.delete(body);
        setNotice({ kind: "error", text: response.status === 429 || response.status === 409 ? "研究未创建：预算或时点证据不满足要求。" : "研究结果尚未确定；再次尝试会复用同一幂等键。" });
        return;
      }
      idempotencyKeys.current.delete(body);
      const envelope = await response.json() as ResearchEnvelope;
      researchRef.current = envelope.data;
      setResearch(envelope.data);
      setState(classifyResearch(envelope.data));
      setNotice({ kind: "success", text: "冻结研究已登记；页面只展示可追溯的研究状态，不触发交易。" });
    } catch {
      setNotice({ kind: "error", text: "研究结果尚未确定；再次尝试会复用同一幂等键。" });
    } finally { setSubmitting(false); }
  }

  return <ResearchPanel state={state} research={research} submitting={submitting} notice={notice} onRefresh={() => void refresh()} onRun={() => void runFrozenResearch()} />;
}

export function ResearchPanel({ state, research, submitting, notice, onRefresh, onRun }: Props): React.ReactElement {
  const hasData = research !== null && ["ready", "partial", "stale"].includes(state);
  return (
    <section className="page-stack research-page" aria-label="个股研究证据页" data-state={state}>
      <header className="page-heading">
        <div><p className="eyebrow">RESEARCH / POINT-IN-TIME</p><h1>个股研究证据页</h1><p>把委员会结论拆成论点、反证、失效条件和不可变证据；没有分数，也不提供交易动作。</p></div>
        <div className="page-actions">
          <span className="state-chip" data-state={state}>{dataStateLabels[state]}</span>
          <button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />重新读取</button>
          {state === "ready" && research?.status === "completed" ? <button className="button button--primary" type="button" onClick={onRun} disabled={submitting}><ScanSearch aria-hidden="true" size={17} />{submitting ? "正在登记研究" : "重新运行冻结研究"}</button> : null}
        </div>
      </header>
      {notice ? <p className="inline-notice" data-kind={notice.kind} role="status" aria-live="polite">{notice.text}</p> : null}
      {!hasData ? <DataState state={state} title={researchStateTitle(state)} detail={researchStateDetail(state)} /> : <>
        {state !== "ready" ? <p className="research-page__gate" role="note">当前数据为 {dataStateLabels[state]}；保留最后证据供核验，但禁止创建或重跑研究。</p> : null}
        <DecisionCard research={research} />
      </>}
    </section>
  );
}

function classifyResearch(research: Research): DataStateKind {
  return research.status === "completed" && research.intent !== null && research.quality_status === "complete" ? "ready" : "partial";
}

function researchStateTitle(state: DataStateKind): string {
  if (state === "empty") return "没有该时点的研究记录";
  if (state === "forbidden") return "无权读取研究记录";
  if (state === "loading") return "正在装订证据卷宗";
  return "研究记录暂时不可用";
}

function researchStateDetail(state: DataStateKind): string {
  return state === "empty" ? "请从市场或每日简报进入，并保留 URL 中的 as_of。" : "系统不会从浏览器状态补造证据、结论或交易动作。";
}
