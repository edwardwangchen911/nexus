"use client";

import { RefreshCw } from "lucide-react";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../../components/data-state";
import { DecisionPath } from "../../../components/decision-path";
import { ProposalComparison } from "../../../components/proposal-comparison";
import { ShadowPerformance } from "../../../components/shadow-performance";
import type { components } from "../../../lib/generated/schema";

type Group = components["schemas"]["OptimizationProposalGroupResponse"];
type GroupEnvelope = components["schemas"]["OptimizationProposalGroupEnvelope"];
type LifecycleEnvelope = components["schemas"]["OptimizationLifecycleEnvelope"];
type Shadow = components["schemas"]["ShadowGroupResponse"];
type ShadowEnvelope = components["schemas"]["ShadowGroupEnvelope"];
type ShadowStartEnvelope = components["schemas"]["ShadowStartEnvelope"];
export type ShadowLoadState = "loading" | "not-started" | "ready" | "partial" | "stale" | "error";

type Props = {
  groupState: DataStateKind;
  shadowState: ShadowLoadState;
  group: Group | null;
  shadow: Shadow | null;
  shadowRequestId: string | null;
  onRefresh: () => void;
  onStartShadow: (groupId: string, version: number) => Promise<void>;
  onReject: (groupId: string, version: number) => Promise<void>;
};

export default function ProposalGroupPage(): React.ReactElement {
  const params = useParams<{ proposalGroupId: string }>();
  const groupId = params?.proposalGroupId ?? "";
  const [groupState, setGroupState] = useState<DataStateKind>("loading");
  const [shadowState, setShadowState] = useState<ShadowLoadState>("loading");
  const [group, setGroup] = useState<Group | null>(null);
  const [shadow, setShadow] = useState<Shadow | null>(null);
  const [shadowRequestId, setShadowRequestId] = useState<string | null>(null);
  const keys = useRef(new Map<string, string>());
  const groupRef = useRef<Group | null>(null);
  const shadowRef = useRef<Shadow | null>(null);

  const applyShadowResponse = useCallback(async (response: Response): Promise<void> => {
    if (response.status === 404) {
      shadowRef.current = null;
      setShadow(null);
      setShadowRequestId(null);
      setShadowState("not-started");
      return;
    }
    if (!response.ok) {
      setShadowRequestId(await safeRequestId(response));
      if (response.status === 409 || response.status === 422) {
        setShadowState("partial");
      } else {
        setShadowState(shadowRef.current ? "stale" : "error");
      }
      return;
    }
    const envelope = await response.json() as ShadowEnvelope;
    shadowRef.current = envelope.data;
    setShadow(envelope.data);
    setShadowRequestId(envelope.meta.request_id);
    setShadowState(envelope.data.freshness.status === "current" ? "ready" : "stale");
  }, []);

  const refreshShadow = useCallback(async (): Promise<void> => {
    if (!groupId) return;
    if (!shadowRef.current) setShadowState("loading");
    try {
      const response = await fetch(
        `/api/v1/optimization-proposals/${encodeURIComponent(groupId)}/shadow`,
        { headers: { accept: "application/json" } },
      );
      await applyShadowResponse(response);
    } catch {
      setShadowState(shadowRef.current ? "stale" : "error");
    }
  }, [applyShadowResponse, groupId]);

  const refresh = useCallback(async (): Promise<void> => {
    if (!groupId) {
      setGroupState("empty");
      setShadowState("not-started");
      return;
    }
    setGroupState("loading");
    if (!shadowRef.current) setShadowState("loading");
    const groupUrl = `/api/v1/optimization-proposals/${encodeURIComponent(groupId)}`;
    const shadowUrl = `${groupUrl}/shadow`;
    const [groupResult, shadowResult] = await Promise.allSettled([
      fetch(groupUrl, { headers: { accept: "application/json" } }),
      fetch(shadowUrl, { headers: { accept: "application/json" } }),
    ]);

    if (groupResult.status === "rejected") {
      setGroupState(groupRef.current ? "stale" : "error");
    } else {
      const response = groupResult.value;
      if (response.status === 403) {
        groupRef.current = null;
        setGroup(null);
        setGroupState("forbidden");
      } else if (response.status === 404) {
        groupRef.current = null;
        setGroup(null);
        setGroupState("empty");
      } else if (!response.ok) {
        setGroupState(response.status === 409 || response.status === 422
          ? "partial"
          : groupRef.current ? "stale" : "error");
      } else {
        const envelope = await response.json() as GroupEnvelope;
        groupRef.current = envelope.data;
        setGroup(envelope.data);
        setGroupState("ready");
      }
    }

    if (shadowResult.status === "rejected") {
      setShadowState(shadowRef.current ? "stale" : "error");
    } else {
      await applyShadowResponse(shadowResult.value);
    }
  }, [applyShadowResponse, groupId]);

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);

  async function startShadow(id: string, version: number): Promise<void> {
    const identity = `shadow:${id}:${version}`;
    const key = keys.current.get(identity) ?? newIdempotencyKey("shadow");
    keys.current.set(identity, key);
    const response = await fetch(`/api/v1/optimization-proposals/${encodeURIComponent(id)}/shadow`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": key,
        "if-match": `"${version}"`,
      },
      body: JSON.stringify({ version }),
    });
    if (!response.ok) {
      const code = await safeErrorCode(response);
      if (response.status < 500) keys.current.delete(identity);
      throw new Error(code);
    }
    const envelope = await response.json() as ShadowStartEnvelope;
    keys.current.delete(identity);
    updateGroupLifecycle(envelope.data);
    await refreshShadow();
  }

  async function reject(id: string, version: number): Promise<void> {
    const identity = `reject:${id}:${version}`;
    const key = keys.current.get(identity) ?? newIdempotencyKey("reject");
    keys.current.set(identity, key);
    const response = await fetch(`/api/v1/optimization-proposals/${encodeURIComponent(id)}/reject`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": key,
        "if-match": `"${version}"`,
      },
      body: JSON.stringify({ version }),
    });
    if (!response.ok) {
      const code = await safeErrorCode(response);
      if (response.status < 500) keys.current.delete(identity);
      throw new Error(code);
    }
    const envelope = await response.json() as LifecycleEnvelope;
    keys.current.delete(identity);
    updateGroupLifecycle(envelope.data);
  }

  function updateGroupLifecycle(data: { proposal_group_id: string; status: Group["status"]; version: number }): void {
    setGroup((current) => {
      const next = current && current.proposal_group_id === data.proposal_group_id
        ? { ...current, status: data.status, version: data.version }
        : current;
      groupRef.current = next;
      return next;
    });
  }

  return (
    <ProposalGroupPanel
      groupState={groupState}
      shadowState={shadowState}
      group={group}
      shadow={shadow}
      shadowRequestId={shadowRequestId}
      onRefresh={() => void refresh()}
      onStartShadow={startShadow}
      onReject={reject}
    />
  );
}

export function ProposalGroupPanel({
  groupState,
  shadowState,
  group,
  shadow,
  shadowRequestId,
  onRefresh,
  onStartShadow,
  onReject,
}: Props): React.ReactElement {
  const hasGroup = group !== null && ["ready", "partial", "stale"].includes(groupState);
  const healthyGroup = groupState === "ready";
  const canStartShadow = healthyGroup && group?.status === "compared" && shadowState === "not-started";
  const canReject = healthyGroup && group !== null && ["draft", "compared", "shadowing"].includes(group.status);
  const minimumChange = group?.proposals.find((proposal) => proposal.variant === "minimum_change");

  return (
    <section className="page-stack proposal-page" aria-label="三方案比较页面" data-state={groupState}>
      <header className="page-heading">
        <div>
          <p className="eyebrow">同一基线 · 三种方案</p>
          <h1>三方案比较</h1>
          <p>固定顺序展示保持现状、最小调整与策略目标；未知值不参与优劣排序。</p>
        </div>
        <div className="page-actions">
          <span className="state-chip" data-state={groupState}>方案：{dataStateLabels[groupState]}</span>
          <span className="state-chip" data-state={shadowState === "not-started" ? "empty" : shadowState}>
            Shadow：{shadowStateLabel(shadowState)}
          </span>
          <button className="button button--quiet" type="button" onClick={onRefresh}>
            <RefreshCw aria-hidden="true" size={17} />重新加载方案
          </button>
        </div>
      </header>
      <DecisionPath current="diagnose" />

      {!hasGroup ? (
        <DataState
          state={groupState}
          title={groupState === "empty" ? "方案组不存在" : "方案组暂时不可用"}
          detail="不会从 URL 或浏览器状态构造方案。"
        />
      ) : (
        <>
          <ProposalComparison
            group={group}
            startShadowEnabled={canStartShadow}
            rejectEnabled={canReject}
            onStartShadow={onStartShadow}
            onReject={onReject}
          />
          {healthyGroup && minimumChange && minimumChange.positions.length <= 30 ? (
            <nav className="proposal-context-links" aria-label="方案上下文动作">
              <a className="button button--quiet" href={`/what-if?proposal_group_id=${group.proposal_group_id}&proposal_id=${minimumChange.proposal_id}`}>
                试算最小调整权重
              </a>
            </nav>
          ) : null}
          {healthyGroup && minimumChange && minimumChange.positions.length > 30 ? (
            <p className="inline-notice">方案可继续只读比较；情景试算最多支持 30 个标的。</p>
          ) : null}
          <ShadowPanel state={shadowState} shadow={shadow} requestId={shadowRequestId} />
        </>
      )}
    </section>
  );
}

function ShadowPanel({ state, shadow, requestId }: { state: ShadowLoadState; shadow: Shadow | null; requestId: string | null }): React.ReactElement {
  if (state === "not-started") {
    return (
      <section className="shadow-state-empty" aria-labelledby="shadow-not-started-title">
        <h2 id="shadow-not-started-title">尚未启动影子验证</h2>
        <p>满足资格并经人工确认后，可启动纯观察的 63 会话前向比较；不会创建订单。</p>
      </section>
    );
  }
  if (shadow && (state === "ready" || state === "partial" || state === "stale")) {
    return (
      <>
        {state === "ready" ? null : (
          <p className="shadow-state-notice" role="status">
            保留最近一次已验证曲线；当前刷新{state === "partial" ? "只返回部分事实" : "失败或数据已滞后"}，不可据此启动新的生命周期操作。
          </p>
        )}
        <ShadowPerformance shadow={shadow} />
      </>
    );
  }
  const dataState = state === "loading" ? "loading" : state === "partial" ? "partial" : "error";
  return (
    <DataState
      state={dataState}
      title={state === "loading" ? "正在读取影子绩效" : "影子绩效暂时不可用"}
      detail={`${state === "partial" ? "后端未返回可验证的完整曲线。" : "未使用浏览器缓存构造曲线。"}${requestId ? ` 请求 ID：${requestId}` : ""}`}
      announce={false}
    />
  );
}

function shadowStateLabel(state: ShadowLoadState): string {
  if (state === "not-started") return "尚未启动";
  return dataStateLabels[state];
}

function newIdempotencyKey(action: string): string {
  return `proposal-${action}-${globalThis.crypto?.randomUUID?.() ?? Date.now().toString(36)}`;
}

async function safeErrorCode(response: Response): Promise<string> {
  try {
    return ((await response.json()) as { error?: { code?: string } }).error?.code ?? "REQUEST_FAILED";
  } catch {
    return "REQUEST_FAILED";
  }
}

async function safeRequestId(response: Response): Promise<string | null> {
  try {
    return ((await response.json()) as { error?: { request_id?: string } }).error?.request_id ?? null;
  } catch {
    return null;
  }
}
