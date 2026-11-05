"use client";

import { EconomicContextBoundary } from "../../../components/paper/economic-context";

import { ArrowLeft, RefreshCw } from "lucide-react";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../../components/data-state";
import { isDecisionCycle } from "../../../components/decisions/decision-list";
import { DecisionTimeline, isPaperOrder } from "../../../components/decisions/decision-timeline";
import { PortfolioDecisionDetail } from "../../../components/decisions/portfolio-decision-detail";
import { cycleDecisionPresentation, executionPresentation } from "../../../lib/product-status";
import { formatDisplayTime } from "../../../lib/display-format";
import type { components } from "../../../lib/generated/schema";

type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];

export default function DecisionDetailPage(): React.ReactElement {
  const params = useParams<{ cycleId: string }>();
  return <EconomicContextBoundary view="detail" cycleId={params?.cycleId}><DefaultDecisionDetailPage /></EconomicContextBoundary>;
}

function DefaultDecisionDetailPage(): React.ReactElement {
  const params = useParams<{ cycleId: string }>();
  const cycleId = params?.cycleId ?? "";
  const [state, setState] = useState<DataStateKind>(cycleId ? "loading" : "empty");
  const [detail, setDetail] = useState<Detail | null>(null);
  const detailRef = useRef<Detail | null>(null);
  const refresh = useCallback(async (): Promise<void> => {
    if (!cycleId) { detailRef.current = null; setDetail(null); setState("empty"); return; }
    if (!detailRef.current) setState("loading");
    try {
      const response = await fetch(`/api/v1/paper/management/cycles/${encodeURIComponent(cycleId)}`, { cache: "no-store", signal: AbortSignal.timeout(10_000) });
      if (response.status === 401 || response.status === 403) { detailRef.current = null; setDetail(null); setState("forbidden"); return; }
      if (response.status === 404) { detailRef.current = null; setDetail(null); setState("empty"); return; }
      if (!response.ok) { setState(detailRef.current ? "stale" : response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const envelope: unknown = await response.json();
      if (!isDetailEnvelope(envelope) || envelope.data.cycle.cycle_id !== cycleId) throw new Error("invalid cycle detail");
      detailRef.current = envelope.data;
      setDetail(envelope.data);
      setState("ready");
    } catch { setState(detailRef.current ? "stale" : "error"); }
  }, [cycleId]);
  useEffect(() => { const timer = globalThis.setTimeout(() => void refresh(), 0); return () => globalThis.clearTimeout(timer); }, [refresh]);
  return <DecisionDetailPanel state={state} detail={detail} onRefresh={() => void refresh()} />;
}

export function DecisionDetailPanel({ state, detail, onRefresh }: { state: DataStateKind; detail: Detail | null; onRefresh: () => void }): React.ReactElement {
  const hasFacts = detail !== null && ["ready", "partial", "stale"].includes(state);
  const title = ({ loading: "正在读取决策详情", empty: "没有这条决策记录", ready: "决策详情已就绪", partial: "决策详情不完整", stale: "显示上次决策详情", error: "决策详情加载失败", forbidden: "无权读取决策详情" } as const)[state];
  const decision = detail ? cycleDecisionPresentation(detail.cycle) : null;
  const execution = detail ? executionPresentation(detail.cycle, detail.orders) : null;
  const primary = decision?.candidates[0] ?? null;
  const resultUnknown = execution?.key === "result_unknown" || execution?.key === "unknown";
  const management = detail && Boolean(detail.candidate_batch || detail.proposal || detail.blocker || detail.synthesis || detail.activation_result || detail.plan_summary?.target_version_id || detail.blocking_plan);
  return <section className="decision-detail-page" data-state={state} aria-label="决策详情页面">
    {/* vinext client routes use native anchors here; next/link currently duplicates React at runtime. */}
    {/* eslint-disable-next-line @next/next/no-html-link-for-pages */}
    <a className="decision-detail-page__back" href="/decisions"><ArrowLeft aria-hidden="true" size={16} />返回决策记录</a>
    <header className="decision-detail-hero">
      <div><p className="eyebrow">DECISION EVIDENCE LEDGER</p><h1>{management ? "组合决策详情" : primary ? `${primary.label} · ${primary.symbol}` : decision?.summary.label ?? "决策详情"}</h1><p>{management ? "核对本轮组合取舍、确定性配置与目标生效依据。" : primary?.detail ?? decision?.summary.detail ?? "读取已保存的周期事实。"}</p></div>
      <div className="page-actions"><span className="mode-badge">内部模拟</span><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />刷新记录</button></div>
      {detail ? <dl><div><dt>决策时间</dt><dd className="numeric-data">{formatDisplayTime(detail.cycle.decision_time)}</dd></div><div><dt>周期 ID</dt><dd className="numeric-data">{detail.cycle.cycle_id}</dd></div><div><dt>执行结论</dt><dd>{resultUnknown ? "结果未知" : execution?.label}</dd></div></dl> : null}
    </header>
    {hasFacts && detail?.cycle.status === "unavailable" && detail.cycle.result_code === "RESEARCH_ADMISSION_UNAVAILABLE" && detail.cycle.admission_failure ? <AdmissionFailureNotice cycle={detail.cycle} /> : null}
    {hasFacts ? management ? <PortfolioDecisionDetail detail={detail} /> : <DecisionTimeline detail={detail} /> : <DataState state={state} title={title} detail="详情只读取该周期及其精确关联订单，不从其他周期补填。" announce={false} />}
  </section>;
}

function AdmissionFailureNotice({ cycle }: { cycle: Detail["cycle"] }): React.ReactElement | null {
  const failure = cycle.admission_failure;
  if (!failure) return null;
  const phases = {
    pool_capacity: "整池容量检查", profile_validation: "模型档案复核", prompt_selection: "研究提示选择",
    snapshot_preparation: "研究快照准备", research_admission: "研究与预算准入", prompt_binding: "研究提示绑定",
    stage_registration: "模型阶段登记", reservation_commit: "研究预留提交",
  };
  const reasons: Record<string, string> = {
    SINGLE_SOL_NATURAL_BUDGET_PROTECTED: "原自然研究额度仍受保护",
    DAILY_RESEARCH_BUDGET_EXHAUSTED: "当日共享研究额度不足",
    RESEARCH_PRICING_STALE: "研究价档已过期",
    CODEX_CAPACITY_PRECLAIM_TIMEOUT: "准备检查达到原截止时间",
    CODEX_CAPACITY_POOL_WIRE_EXCEEDED: "研究输入超过容量限制",
    CODEX_CAPACITY_POOL_INPUT_EXCEEDED: "研究输入超过容量限制",
    PLATFORM_NATIVE_INPUT_CHANGED: "阶段输入与冻结证据不一致",
    UNKNOWN: "原准入错误未能安全归类",
  };
  const symbol = cycle.candidates.find(candidate => candidate.instrument_id === failure.instrument_id)?.symbol;
  return <aside className="inline-notice" role="note" aria-label="研究准入诊断">
    <strong>{symbol ? `${symbol} · ` : ""}{phases[failure.phase]}</strong>
    <p>{reasons[failure.code] ?? "准入校验未通过，具体安全原因见记录代码。"} · <code>{failure.code}</code></p>
    <p>此记录不授权重试或解除预算占用。</p>
  </aside>;
}

function isDetailEnvelope(value: unknown): value is components["schemas"]["AgentPaperManagementCycleEnvelope"] {
  return isPlain(value) && isPlain(value.data) && isDecisionCycle(value.data.cycle)
    && Array.isArray(value.data.orders) && value.data.orders.every(isPaperOrder)
    && isPlain(value.meta) && typeof value.meta.as_of === "string";
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
