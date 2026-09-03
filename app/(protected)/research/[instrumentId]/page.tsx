"use client";

import { RefreshCw, ScanSearch } from "lucide-react";
import { useParams, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../../components/data-state";
import { DecisionCard } from "../../../components/decision-card";
import { DecisionPath } from "../../../components/decision-path";
import { ResearchInputs } from "../../../components/research-inputs";
import { Badge } from "../../../components/ui/badge";
import { Button } from "../../../components/ui/button";
import type { components } from "../../../lib/generated/schema";

type Research = components["schemas"]["ResearchRunResponse"];
type ResearchEnvelope = components["schemas"]["ResearchRunEnvelope"];
type ResearchAdmission = components["schemas"]["ResearchAdmissionResponse"];
type ResearchAdmissionEnvelope = components["schemas"]["ResearchAdmissionEnvelope"];
type Notice = { kind: "error" | "success"; text: string } | null;

type Props = {
  state: DataStateKind;
  research: Research | null;
  admission: ResearchAdmission | null;
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
  const [admission, setAdmission] = useState<ResearchAdmission | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [notice, setNotice] = useState<Notice>(null);
  const researchRef = useRef<Research | null>(null);
  const idempotencyKeys = useRef(new Map<string, string>());

  const refresh = useCallback(async () => {
    if (!instrumentId || !asOf) { setResearch(null); setAdmission(null); setState("empty"); return; }
    setNotice(null);
    if (!researchRef.current) setState("loading");
    try {
      const suffix = `?as_of=${encodeURIComponent(asOf)}`;
      const [response, admissionResponse] = await Promise.all([
        fetch(`/api/v1/instruments/${encodeURIComponent(instrumentId)}/research${suffix}`, { headers: { accept: "application/json" } }),
        fetch(`/api/v1/instruments/${encodeURIComponent(instrumentId)}/research-admission${suffix}`, { headers: { accept: "application/json" } }),
      ]);
      if (response.status === 403 || admissionResponse.status === 403) { researchRef.current = null; setResearch(null); setAdmission(null); setState("forbidden"); return; }
      const nextAdmission = admissionResponse.ok ? (await admissionResponse.json() as ResearchAdmissionEnvelope).data : null;
      setAdmission(nextAdmission);
      if (response.status === 404) { researchRef.current = null; setResearch(null); setState(admissionResponse.ok ? "empty" : "error"); return; }
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
    const canRun = admission?.eligible === true && admission.data_snapshot_id !== null
      && (state === "empty" || state === "ready" && research?.status === "completed");
    if (!canRun) return;
    const body = JSON.stringify({ instrument_id: admission.instrument_id, data_snapshot_id: admission.data_snapshot_id, decision_time: admission.decision_time });
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

  return <ResearchPanel state={state} research={research} admission={admission} submitting={submitting} notice={notice} onRefresh={() => void refresh()} onRun={() => void runFrozenResearch()} />;
}

export function ResearchPanel({ state, research, admission, submitting, notice, onRefresh, onRun }: Props): React.ReactElement {
  const hasData = research !== null && ["ready", "partial", "stale"].includes(state);
  const inputs = state !== "forbidden" && state !== "loading" ? admission?.inputs : null;
  const differentInputSnapshot = hasData && inputs && admission?.data_snapshot_id
    && admission.data_snapshot_id.toLowerCase() !== research.data_snapshot_id.toLowerCase();
  const hasFundObservations = (inputs?.issuer_fund_observations ?? []).length > 0;
  const hasNavHistory = (inputs?.issuer_nav_histories ?? []).length > 0;
  const issuerKinds = [hasFundObservations, hasNavHistory, (inputs?.issuer_distributions ?? []).length > 0].filter(Boolean).length;
  const hasIssuerInputs = issuerKinds > 0;
  const issuerInputsLabel = inputs && (inputs.filings.length > 0 || issuerKinds > 1)
    ? "研究资料" : hasNavHistory ? "发行商净值历史资料" : hasFundObservations ? "发行商分类与持仓资料" : "发行商现金分红资料";
  const canRun = admission?.eligible === true && admission.data_snapshot_id !== null
    && (state === "empty" || state === "ready" && research?.status === "completed");
  return (
    <section className="page-stack research-page" aria-label="个股研究证据页" data-state={state}>
      <header className="page-heading">
        <div><p className="eyebrow">时点冻结研究</p><h1>个股研究证据页</h1><p>把委员会结论拆成论点、反证、失效条件和不可变证据；没有分数，也不提供交易动作。</p></div>
        <div className="page-actions">
          <Badge data-state={state}>{inputs && state === "empty" ? (hasIssuerInputs ? "资料已冻结 · 研究待生成" : "财报已冻结 · 研究待生成") : dataStateLabels[state]}</Badge>
          <Button variant="outline" onClick={onRefresh}><RefreshCw aria-hidden="true" size={16} />重新读取</Button>
          {canRun ? <Button onClick={onRun} disabled={submitting} aria-busy={submitting}><ScanSearch aria-hidden="true" size={16} />{submitting ? "正在登记研究" : state === "empty" ? "运行冻结研究" : "重新运行冻结研究"}</Button> : null}
        </div>
      </header>
      <DecisionPath current="research" />
      {notice ? <p className="inline-notice" data-kind={notice.kind} role="status" aria-live="polite">{notice.text}</p> : null}
      {admission?.eligible === false ? <p className="research-page__gate" role="note">{researchAdmissionReason(admission.reason)}</p> : null}
      {inputs && admission ? <ResearchInputs inputs={inputs} asOf={admission.decision_time} /> : null}
      {differentInputSnapshot ? <p className="research-page__gate" role="note">当前资料与下方研究使用不同冻结快照，不能把当前资料视为下方研究已经采用的完整输入；请分别核对两处截止时间与证据版本。</p> : null}
      {!hasData ? (inputs && state === "empty"
        ? <p className="research-page__gate" role="note">{hasIssuerInputs ? `${issuerInputsLabel}已就绪，AI 研究尚未生成。可先核验数值、日期与来源；不代表具备历史回测或执行资格。` : admission?.inputs?.filings.some((filing) => ["6-K", "497K"].includes(filing.form)) ? "披露资料已就绪，AI 研究尚未生成。可先核验正文、时间与来源；文本不等于财务指标。" : "财报资料已就绪，AI 研究尚未生成。可先核验数值、期间与来源；模型配置可稍后补充。"}</p>
        : <DataState state={state} title={researchStateTitle(state)} detail={researchStateDetail(state)} />) : <>
        {state !== "ready" ? <p className="research-page__gate" role="note">当前数据为 {dataStateLabels[state]}；保留最后证据供核验，但禁止创建或重跑研究。</p> : null}
        <DecisionCard research={research} />
      </>}
    </section>
  );
}

function researchAdmissionReason(reason: ResearchAdmission["reason"]): string {
  if (reason === "instrument_not_found") return "研究准入失败：该标的不存在于服务端权威目录。";
  if (reason === "decision_time_in_future") return "研究准入失败：决策时点仍在未来。";
  if (reason === "data_snapshot_unavailable") return "研究准入失败：该时点没有可冻结的数据快照。";
  if (reason === "prompt_authority_unavailable") return "研究准入失败：委员会提示词版本尚未登记。";
  if (reason === "model_pricing_unavailable") return "研究准入失败：模型定价凭据已缺失或过期。";
  return "研究准入失败：服务端没有提供完整权威依据。";
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
