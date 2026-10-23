"use client";

import { EconomicContextBoundary } from "../../components/paper/economic-context";

import { RefreshCw } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { ActivitySummary, validActivity } from "../../components/workbench/activity-summary";
import { ManualPaperStart } from "../../components/workbench/manual-paper-start";
import { CurrentCash } from "../../components/workbench/current-cash";
import { AccountSummary } from "../../components/workbench/account-summary";
import { ManagementSummaryCard } from "../../components/paper/management-summary";
import { AttentionList } from "../../components/workbench/attention-list";
import { LatestDecision } from "../../components/workbench/latest-decision";
import { ModeStrip } from "../../components/workbench/mode-strip";
import { ReadinessSummary } from "../../components/workbench/readiness-summary";
import { OrderSummary } from "../../components/workbench/order-summary";
import { DataState } from "../../components/data-state";
import type { components } from "../../lib/generated/schema";

export type WorkbenchPayload = components["schemas"]["WorkbenchEnvelope"];
export type WorkbenchPageState = "loading" | "empty" | "ready" | "partial" | "stale" | "error" | "forbidden";

const LOAD_TIMEOUT_MS = 10_000;
const stateCopy: Record<WorkbenchPageState, { label: string; title: string; detail: string }> = {
  loading: { label: "正在加载", title: "正在读取工作台", detail: "只读取已保存事实，不会触发 Agent、模型或交易。" },
  empty: { label: "暂无工作台事实", title: "尚无决策与账户事实", detail: "先通过现有运行入口生成并保存周期；刷新本页不会启动运行。" },
  ready: { label: "工作台已更新", title: "工作台已更新", detail: "当前显示服务端返回的最新已保存事实。" },
  partial: { label: "部分事实不可用", title: "工作台事实不完整", detail: "可用事实按原样显示；缺失内容不会从其他来源推断。" },
  stale: { label: "显示上次数据", title: "无法更新工作台", detail: "保留最后一次有效事实；请核对服务状态后再刷新。" },
  error: { label: "工作台加载失败", title: "无法建立工作台视图", detail: "响应缺失或格式无效，页面没有展示推测数据。" },
  forbidden: { label: "无权访问工作台", title: "当前会话不能读取工作台", detail: "已清除先前读取的工作台事实。" },
};

export default function WorkspacePage(): React.ReactElement {
  return <EconomicContextBoundary view="workspace"><DefaultWorkspacePage /></EconomicContextBoundary>;
}

function DefaultWorkspacePage(): React.ReactElement {
  const [state, setState] = useState<WorkbenchPageState>("loading");
  const [payload, setPayload] = useState<WorkbenchPayload | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const payloadRef = useRef<WorkbenchPayload | null>(null);
  const readVersion = useRef(0);

  const clearFacts = useCallback(() => {
    payloadRef.current = null;
    setPayload(null);
  }, []);

  const refresh = useCallback(async (): Promise<void> => {
    const version = readVersion.current + 1;
    readVersion.current = version;
    if (payloadRef.current) setRefreshing(true);
    else setState("loading");
    try {
      const response = await fetch("/api/v1/workbench", {
        cache: "no-store",
        signal: AbortSignal.timeout(LOAD_TIMEOUT_MS),
      });
      if (version !== readVersion.current) return;
      if (response.status === 401 || response.status === 403) {
        clearFacts();
        setState("forbidden");
        return;
      }
      if (response.status === 404) {
        clearFacts();
        setState("empty");
        return;
      }
      if (!response.ok) {
        setState(payloadRef.current ? "stale" : response.status === 409 || response.status === 422 ? "partial" : "error");
        return;
      }
      const value: unknown = await response.json();
      if (!isWorkbenchPayload(value)) throw new Error("invalid workbench response");
      payloadRef.current = value;
      setPayload(value);
      setState(deriveState(value.data));
    } catch {
      if (version !== readVersion.current) return;
      setState(payloadRef.current ? "stale" : "error");
    } finally {
      if (version === readVersion.current) setRefreshing(false);
    }
  }, [clearFacts]);

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);

  return <WorkbenchPanel state={state} payload={payload} refreshing={refreshing} onRefresh={() => void refresh()} />;
}

export function WorkbenchPanel({
  state, payload, refreshing, onRefresh,
}: {
  state: WorkbenchPageState;
  payload: WorkbenchPayload | null;
  refreshing: boolean;
  onRefresh: () => void;
}): React.ReactElement {
  const copy = stateCopy[state];
  return (
    <section className="workbench-page" data-state={state} aria-label="交易工作台页面">
      <header className="workbench-page__heading">
        <div><p className="eyebrow">AGENT PAPER WORKBENCH</p><h1>交易工作台</h1><p>查看 Agent 决策、确定性风控、内部模拟执行与账户结果。</p></div>
        <button className="button button--quiet" type="button" onClick={onRefresh} disabled={refreshing} aria-label="刷新工作台">
          <RefreshCw aria-hidden="true" size={17} />{refreshing ? "正在刷新…" : "刷新"}
        </button>
      </header>

      <p className="workbench-announcement" role="status" aria-label="工作台状态" aria-live="polite" aria-atomic="true">
        <strong>{refreshing ? "正在刷新" : copy.label}</strong><span>{refreshing ? "保留当前事实，等待只读刷新结果。" : copy.detail}</span>
      </p>

      {state !== "forbidden" && state !== "loading" && <ManualPaperStart />}

      {payload ? <>
        <ModeStrip workbench={payload.data} />
        <ActivitySummary activity={"activity" in payload.data ? payload.data.activity : undefined} />
        <ManagementSummaryCard />
        <ReadinessSummary workbench={payload.data} />
        <div className="workbench-primary-grid">
          <LatestDecision cycle={payload.data.latest_cycle} orders={payload.data.active_orders} />
          <AttentionList items={payload.data.attention} />
        </div>
        <OrderSummary orders={payload.data.active_orders} latestCycle={payload.data.latest_cycle} />
        <CurrentCash readVersion={payload} />
        <AccountSummary account={payload.data.account} />
      </> : <DataState state={state} title={copy.title} detail={copy.detail} announce={false} />}
    </section>
  );
}

function deriveState(workbench: WorkbenchPayload["data"]): WorkbenchPageState {
  const activity = "activity" in workbench ? workbench.activity : undefined;
  if (activity != null && (!validActivity(activity) || activity.read_status === "partial")) return "partial";
  const hasActivity = validActivity(activity) && activity.scopes.some((scope) => scope.selection !== "none" || (scope.today_registered ?? 0) > 0 || (scope.today_started ?? 0) > 0);
  if (!hasActivity && !workbench.latest_cycle && !workbench.account && workbench.active_orders.length === 0 && workbench.attention.length === 0) return "empty";
  if (workbench.account?.data_state === "STALE") return "stale";
  if (workbench.account?.data_state === "PARTIAL") return "partial";
  return "ready";
}

function isWorkbenchPayload(value: unknown): value is WorkbenchPayload {
  if (!isPlain(value) || !isPlain(value.data) || !isPlain(value.meta)) return false;
  const data = value.data;
  const loopReasons = [null, "HEARTBEAT_MISSING", "HEARTBEAT_STALE", "HEARTBEAT_INCOMPATIBLE", "HEARTBEAT_INVALID"];
  if (data.mode !== "internal_paper" || data.execution_locked !== true
    || !isPlain(data.loop) || !["online", "offline", "unknown"].includes(String(data.loop.status))
    || !loopReasons.includes(data.loop.reason_code as null | string)
    || (data.loop.status === "online" && data.loop.reason_code !== null)
    || (data.loop.status === "unknown" && !["HEARTBEAT_MISSING", "HEARTBEAT_INVALID"].includes(String(data.loop.reason_code)))
    || (data.loop.status === "offline" && !["HEARTBEAT_STALE", "HEARTBEAT_INCOMPATIBLE"].includes(String(data.loop.reason_code)))
    || data.loop.stale_after_seconds !== 30
    || !isNullableString(data.loop.started_at) || !isNullableString(data.loop.last_seen_at)
    || !isPlain(data.session) || data.session.execution_locked !== true
    || !Array.isArray(data.active_orders) || !Array.isArray(data.attention)
    || typeof value.meta.as_of !== "string" || typeof value.meta.request_id !== "string"
    || typeof value.meta.schema_version !== "string") return false;
  if (!validReadiness(data.readiness) || !validCompatibility(data.compatibility)) return false;
  if (!validSession(data.session)) return false;
  if (data.latest_cycle !== null && !validCycle(data.latest_cycle)) return false;
  if (!data.active_orders.every(validOrder) || !data.attention.every(validAttention)) return false;
  return data.account === null || validAccount(data.account);
}

function validSession(value: Record<string, unknown>): boolean {
  return isNullableString(value.decision_session)
    && ["calendar_unavailable", "not_started", "ready", "in_progress", "completed", "stopped", "closed", "historical"].includes(String(value.status))
    && Number.isInteger(value.cycle_count) && Number.isInteger(value.completed_count)
    && isNullableString(value.last_completed_decision_at) && isNullableString(value.next_eligible_at)
    && (value.latest_result_code === undefined || isNullableString(value.latest_result_code));
}

function validCycle(value: unknown): boolean {
  if (!isPlain(value) || value.execution_locked !== true || typeof value.cycle_id !== "string"
    || typeof value.decision_time !== "string" || !Array.isArray(value.candidates) || !Array.isArray(value.order_ids)
    || !value.candidates.every((candidate) => isPlain(candidate) && typeof candidate.instrument_id === "string" && typeof candidate.symbol === "string")
    || !value.order_ids.every((id) => typeof id === "string")) return false;
  return value.decision === null || isPlain(value.decision);
}

function validOrder(value: unknown): boolean {
  return isPlain(value) && typeof value.order_id === "string" && typeof value.symbol === "string"
    && ["buy", "sell"].includes(String(value.side)) && typeof value.quantity === "string"
    && typeof value.filled_quantity === "string" && typeof value.remaining_quantity === "string"
    && typeof value.status === "string" && Array.isArray(value.events) && Array.isArray(value.fills);
}

function validAttention(value: unknown): boolean {
  return isPlain(value) && ["data", "model", "risk", "execution", "service"].includes(String(value.kind))
    && ["info", "warning", "critical"].includes(String(value.severity))
    && typeof value.title_code === "string" && typeof value.reason_code === "string" && typeof value.href === "string"
    && ["view_evidence", "view_decision", "verify_original_result", "wait"].includes(String(value.safe_action));
}

function validAccount(value: unknown): boolean {
  return isPlain(value) && typeof value.portfolio_id === "string" && value.currency === "USD"
    && typeof value.initial_capital === "string" && ["EMPTY", "READY", "PARTIAL", "STALE"].includes(String(value.data_state))
    && ["as_of", "nav", "available_cash", "reserved_cash", "market_value", "realized_pnl", "unrealized_pnl"]
      .every((key) => isNullableString(value[key]));
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNullableString(value: unknown): boolean {
  return value === null || typeof value === "string";
}

function validReadiness(value: unknown): boolean {
  if (value === undefined || value === null) return true;
  return isPlain(value)
    && ["window", "data", "agent", "risk", "execution", "recovery", "completed", "unknown"].includes(String(value.stage))
    && ["none", "data_missing", "stale", "unbound", "research_not_actionable", "risk_rejected", "result_unknown", "incompatible", "unknown"].includes(String(value.blocker))
    && typeof value.reason_code === "string" && isNullableString(value.previous_cycle_id)
    && (value.previous_result_code === undefined || isNullableString(value.previous_result_code))
    && Array.isArray(value.changes) && value.changes.every((key) => ["stage", "blocker", "result", "risk", "orders"].includes(key));
}

function validCompatibility(value: unknown): boolean {
  if (value === undefined || value === null) return true;
  return isPlain(value) && ["compatible", "incompatible", "unknown"].includes(String(value.status))
    && ["matching", "mismatching", "unknown"].includes(String(value.release_status))
    && typeof value.expected_process_version === "string" && typeof value.expected_migration_head === "string"
    && isNullableRevision(value.expected_release_revision) && isNullableRevision(value.reported_release_revision)
    && isNullableString(value.reported_process_version) && isNullableString(value.reported_migration_head)
    && Array.isArray(value.database_migration_heads) && value.database_migration_heads.every((head) => typeof head === "string");
}

function isNullableRevision(value: unknown): boolean {
  return value === null || (typeof value === "string" && /^[0-9a-f]{40}$/.test(value));
}
