"use client";

import { AlertTriangle, CheckCircle2, RefreshCw, ShieldAlert, X } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { CapabilityBanner } from "../../components/broker-simulation/capability-banner";
import { IntentSummary } from "../../components/broker-simulation/intent-summary";
import { OrderBatch } from "../../components/broker-simulation/order-batch";
import { BrokerOrderTimeline } from "../../components/broker-simulation/order-timeline";
import { ReconciliationPanel } from "../../components/broker-simulation/reconciliation-panel";
import { DecisionPath } from "../../components/decision-path";
import type { components } from "../../lib/generated/schema";

export type BrokerSimulation = components["schemas"]["BrokerSimulationResponse"];
export type BrokerWorkbenchState =
  | "loading" | "empty" | "ready" | "partial" | "stale" | "error" | "forbidden"
  | "reconciling" | "unknown";
type Notice = { kind: "success" | "error"; text: string } | null;
type PendingAction = { action: "approve" | "reject"; idempotencyKey: string };

const LOAD_TIMEOUT_MS = 10_000;
const ACTION_TIMEOUT_MS = 30_000;
const stateCopy: Record<BrokerWorkbenchState, { label: string; detail: string }> = {
  loading: { label: "正在加载", detail: "正在读取服务端权威批次，不触发任何动作。" },
  empty: { label: "暂无待处理批次", detail: "尚未生成可审阅的服务端订单批次。" },
  ready: { label: "证据已就绪", detail: "可审阅并在本地批准已封印批次；批准不会派发订单。" },
  partial: { label: "权威事实不完整", detail: "保留最后已知事实，所有可变动作关闭。" },
  stale: { label: "权威事实已过期", detail: "批次、报价或能力已过期，需重建权威后再审阅。" },
  error: { label: "加载失败", detail: "无法建立权威视图；未触发任何订单动作。" },
  forbidden: { label: "无权访问", detail: "当前会话不能读取模拟盘权威事实。" },
  reconciling: { label: "正在对账", detail: "等待券商回执与内部记录对齐，账户保持锁定。" },
  unknown: { label: "提交结果未知", detail: "未收到确定结果；请继续核对原操作，不会重复提交。" },
};

export default function BrokerSimulationPage(): React.ReactElement {
  const [state, setState] = useState<BrokerWorkbenchState>("loading");
  const [workbench, setWorkbench] = useState<BrokerSimulation | null>(null);
  const [notice, setNotice] = useState<Notice>(null);
  const [busy, setBusy] = useState(false);
  const [confirmApproval, setConfirmApproval] = useState(false);
  const [unknownAction, setUnknownAction] = useState<PendingAction | null>(null);
  const workbenchRef = useRef<BrokerSimulation | null>(null);
  const readVersion = useRef(0);
  const returnFocus = useRef<HTMLElement | null>(null);

  const clearFacts = useCallback(() => {
    workbenchRef.current = null;
    setWorkbench(null);
  }, []);

  const refresh = useCallback(async (): Promise<boolean> => {
    const version = readVersion.current + 1;
    readVersion.current = version;
    if (!workbenchRef.current) setState("loading");
    try {
      const response = await fetch("/api/v1/broker-simulation", { signal: AbortSignal.timeout(LOAD_TIMEOUT_MS) });
      if (version !== readVersion.current) return false;
      if (response.status === 401 || response.status === 403) {
        clearFacts(); setState("forbidden"); return false;
      }
      if (response.status === 404) { clearFacts(); setState("empty"); return true; }
      if (!response.ok) {
        setState(workbenchRef.current ? "stale" : "error");
        setNotice({ kind: "error", text: "无法更新模拟盘权威事实；所有动作保持关闭。" });
        return false;
      }
      const payload = await response.json() as { data?: unknown; meta?: { as_of?: unknown } };
      if (!isBrokerSimulation(payload.data) || typeof payload.meta?.as_of !== "string") throw new Error("invalid broker workbench");
      workbenchRef.current = payload.data;
      setWorkbench(payload.data);
      setState(deriveState(payload.data, payload.meta.as_of));
      return true;
    } catch {
      if (version !== readVersion.current) return false;
      setState(workbenchRef.current ? "stale" : "error");
      setNotice({ kind: "error", text: "模拟盘读取失败；未触发任何订单动作。" });
      return false;
    }
  }, [clearFacts]);

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);

  function openApproval(trigger?: HTMLElement): void {
    returnFocus.current = trigger ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    setConfirmApproval(true);
  }

  function closeApproval(): void {
    if (busy) return;
    setConfirmApproval(false);
    globalThis.setTimeout(() => returnFocus.current?.focus(), 0);
  }

  async function submitAction(action: "approve" | "reject", pending?: PendingAction): Promise<void> {
    const intent = workbenchRef.current?.intent;
    if (!intent || intent.status !== "prepared" || busy) return;
    const command = pending ?? { action, idempotencyKey: `broker-${action}-${crypto.randomUUID()}` };
    setBusy(true); setNotice(null); setUnknownAction(null);
    try {
      const response = await fetch(`/api/v1/broker-simulation/intents/${encodeURIComponent(intent.intent_id)}/${action}`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": command.idempotencyKey,
          "if-match": action === "approve" ? intent.body_hash : `"${intent.version}"`,
        },
        body: JSON.stringify(action === "approve"
          ? { version: intent.version }
          : { version: intent.version, reason: "user_rejected" }),
        signal: AbortSignal.timeout(ACTION_TIMEOUT_MS),
      });
      let payload: { data?: { resource_id?: string; action?: string; dispatch_created?: boolean }; error?: { code?: string } } = {};
      try { payload = await response.json() as typeof payload; } catch { /* unknown below */ }
      if (response.ok && payload.data?.resource_id === intent.intent_id
        && payload.data.action === action && payload.data.dispatch_created === false) {
        setConfirmApproval(false);
        const refreshed = await refresh();
        setNotice(refreshed
          ? { kind: "success", text: action === "approve"
            ? "离线批次已批准；未创建派发任务。"
            : "离线批次已拒绝；未创建派发任务。" }
          : { kind: "error", text: "本地决策已记录，但权威事实重读失败；动作保持关闭。" });
        globalThis.setTimeout(() => returnFocus.current?.focus(), 0);
        return;
      }
      if (response.status >= 500 || response.ok) throw new Error("unknown result");
      setConfirmApproval(false);
      await refresh();
      setNotice({ kind: "error", text: response.status === 409
        ? "批次权威已变化；已重新读取，请重新审阅。"
        : "离线批次未变更；请检查权威事实。" });
    } catch {
      setConfirmApproval(false);
      setUnknownAction(command);
      setState("unknown");
      setNotice({ kind: "error", text: "提交结果未知；已锁定当前批次，请继续确认原操作结果。" });
    } finally { setBusy(false); }
  }

  return <>
    <BrokerSimulationPanel
      state={state}
      workbench={workbench}
      notice={notice}
      busy={busy}
      onRefresh={() => void refresh()}
      onApprove={() => openApproval()}
      onReject={() => void submitAction("reject")}
      onRetry={unknownAction ? () => void submitAction(unknownAction.action, unknownAction) : undefined}
    />
    {confirmApproval && workbench?.intent ? (
      <ApprovalDialog busy={busy} onDismiss={closeApproval} onConfirm={() => void submitAction("approve")} />
    ) : null}
  </>;
}

export function BrokerSimulationPanel({
  state, workbench, notice, busy, onRefresh, onApprove, onReject, onRetry,
}: {
  state: BrokerWorkbenchState;
  workbench: BrokerSimulation | null;
  notice: Notice;
  busy: boolean;
  onRefresh: () => void;
  onApprove: () => void;
  onReject: () => void;
  onRetry?: () => void;
}): React.ReactElement {
  const copy = stateCopy[state];
  const mutable = state === "ready";
  return (
    <section className="broker-workbench" data-state={state} aria-label="复星模拟盘审核工作台">
      <header className="broker-hero">
        <div>
          <p className="eyebrow">离线模拟审核 · 证据账簿</p>
          <h1>复星模拟盘验证</h1>
          <p>把策略意图、确定性风控、人工批准与券商回执拆成可追溯的权威节点。</p>
        </div>
        <div className="broker-hero__actions">
          <span className="broker-live-lock"><ShieldAlert aria-hidden="true" size={17} />实盘关闭</span>
          <button className="button button--quiet" type="button" disabled={busy} onClick={onRefresh} aria-label="刷新模拟盘权威事实">
            <RefreshCw aria-hidden="true" size={17} />刷新事实
          </button>
        </div>
      </header>

      <DecisionPath current="review" />

      <div className="broker-state" data-state={state}>
        <div role="status" aria-label="模拟盘工作台状态" aria-live="polite" aria-atomic="true">
          <strong>{copy.label}</strong><span>{copy.detail}</span>
        </div>
        {notice ? <p className={`broker-notice broker-notice--${notice.kind}`} role={notice.kind === "error" ? "alert" : undefined}>{notice.text}</p> : null}
        {state === "unknown" && onRetry ? <button className="button button--danger" type="button" disabled={busy} onClick={onRetry}>继续确认原操作结果</button> : null}
      </div>

      <ExecutionCircuit />

      {workbench ? <>
        <CapabilityBanner workbench={workbench} />
        <IntentSummary workbench={workbench} mutationsAllowed={mutable} busy={busy} onApprove={onApprove} onReject={onReject} />
        {workbench.intent ? <OrderBatch intent={workbench.intent} /> : null}
        <div className="broker-ledger-grid">
          <BrokerOrderTimeline events={workbench.events} />
          <ReconciliationPanel reconciliation={workbench.reconciliation} />
        </div>
      </> : <section className="broker-empty" aria-label="模拟盘空状态">
        <AlertTriangle aria-hidden="true" size={22} />
        <p>{copy.detail}</p>
      </section>}
    </section>
  );
}

function ExecutionCircuit(): React.ReactElement {
  return (
    <section className="broker-circuit" aria-label="执行电路">
      <p className="eyebrow">失败关闭执行电路</p>
      <ol>
        {["策略提案", "确定性风控", "人工批准", "外部激活", "派发与对账"].map((step, index) => (
          <li key={step} data-locked={index >= 3 ? "true" : undefined}>
            <span className="numeric-data">0{index + 1}</span><strong>{step}</strong>
          </li>
        ))}
      </ol>
      <p>当前仅支持本地审阅与离线回放，外部订单发送未启用。</p>
    </section>
  );
}

function ApprovalDialog({ busy, onDismiss, onConfirm }: { busy: boolean; onDismiss: () => void; onConfirm: () => void }): React.ReactElement {
  const closeRef = useRef<HTMLButtonElement>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);
  const confirmRef = useRef<HTMLButtonElement>(null);
  useEffect(() => { cancelRef.current?.focus(); }, []);
  function onKeyDown(event: React.KeyboardEvent<HTMLDialogElement>): void {
    if (event.key === "Escape") { event.preventDefault(); onDismiss(); return; }
    if (event.key !== "Tab") return;
    if (event.shiftKey && document.activeElement === closeRef.current) {
      event.preventDefault(); confirmRef.current?.focus();
    } else if (!event.shiftKey && document.activeElement === confirmRef.current) {
      event.preventDefault(); closeRef.current?.focus();
    }
  }
  return (
    <div className="dialog-backdrop" role="presentation">
      <dialog className="dialog" open aria-modal="true" aria-labelledby="broker-approval-title" onKeyDown={onKeyDown}>
        <button className="dialog__close" type="button" onClick={onDismiss} disabled={busy} aria-label="关闭批准对话框" ref={closeRef}><X aria-hidden="true" size={18} /></button>
        <p className="eyebrow">仅记录本地批准</p>
        <h2 id="broker-approval-title">确认批准离线模拟批次</h2>
        <p>批准不会派发任何订单，也不会开启复星模拟写入或实盘。它只记录你对当前封印批次的本地批准。</p>
        <div className="dialog__actions">
          <button className="button button--quiet" type="button" onClick={onDismiss} disabled={busy} ref={cancelRef}>返回审阅</button>
          <button className="button button--primary" type="button" onClick={onConfirm} disabled={busy} ref={confirmRef}>
            <CheckCircle2 aria-hidden="true" size={17} />{busy ? "正在记录…" : "确认离线批准"}
          </button>
        </div>
      </dialog>
    </div>
  );
}

function deriveState(workbench: BrokerSimulation, asOf: string): BrokerWorkbenchState {
  if (!workbench.account && !workbench.intent && !workbench.reconciliation && workbench.events.length === 0) return "empty";
  if (workbench.intent?.status === "submission_unknown") return "unknown";
  if (workbench.intent?.status === "reconciling" || workbench.reconciliation?.status === "pending" || workbench.reconciliation?.status === "running") return "reconciling";
  const authorityTime = Date.parse(asOf);
  if (workbench.intent?.status === "expired"
    || workbench.intent && Date.parse(workbench.intent.expires_at) <= authorityTime
    || workbench.account && Date.parse(workbench.account.capability_expires_at) <= authorityTime) return "stale";
  if (workbench.account?.locked || workbench.reconciliation?.status === "conflict") return "partial";
  return "ready";
}

function isBrokerSimulation(value: unknown): value is BrokerSimulation {
  if (!isPlain(value) || !isPlain(value.capability) || !isPlain(value.actions)
    || value.capability.live_trading !== false
    || value.capability.external_activation !== "external_activation_blocked"
    || value.capability.simulation_write_enabled !== false
    || value.actions.can_dispatch !== false || value.actions.can_modify !== false || value.actions.can_cancel !== false
    || !Array.isArray(value.events) || value.events.length > 200) return false;
  if (value.account !== null && !isPlain(value.account)) return false;
  if (value.intent !== null && (!isPlain(value.intent) || !Array.isArray(value.intent.orders))) return false;
  return value.reconciliation === null || isPlain(value.reconciliation);
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
