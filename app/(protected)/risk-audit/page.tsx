"use client";

import {
  AlertTriangle,
  Archive,
  Download,
  RefreshCw,
  RotateCcw,
  ShieldAlert,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import type { components } from "../../lib/generated/schema";

export type Readiness = components["schemas"]["ReadinessResponse"];
export type RiskAuditData = components["schemas"]["RiskAuditPageResponse"];
type AuditEvent = components["schemas"]["RiskAuditEventResponse"];
type RetryableJob = components["schemas"]["RetryableJobSummary"];
type AuditKind = "" | AuditEvent["kind"];
type OutcomeFilter = "" | "attention" | "recovered" | "healthy";
type Notice = { kind: "success" | "error"; text: string } | null;
type Filters = { kind: AuditKind; status: OutcomeFilter; from: string; to: string };
type PendingAction =
  | { kind: "retry"; job: RetryableJob; idempotencyKey: string; unknown: boolean }
  | { kind: "acknowledge"; eventSeq: number; idempotencyKey: string; unknown: boolean };
type RiskAuditPanelProps = {
  state: DataStateKind;
  readiness: Readiness | null;
  data: RiskAuditData | null;
  kindFilter: AuditKind;
  statusFilter: OutcomeFilter;
  fromDate: string;
  toDate: string;
  loadingMore: boolean;
  notice: Notice;
  onRefresh: () => void;
  onFilters: (filters: Filters) => void;
  onLoadMore: () => void;
  onRetry: (job: RetryableJob, trigger?: HTMLButtonElement) => void;
  onAcknowledge: (eventSeq: number, trigger?: HTMLButtonElement) => void;
  onExport: () => void;
};

const READ_TIMEOUT_MS = 10_000;
const WRITE_TIMEOUT_MS = 30_000;
const PAGE_SIZE = 50;
const MAX_EVENTS = 500;
const auditKinds: Array<[AuditKind, string]> = [
  ["", "全部类型"], ["quality", "数据质量"], ["risk", "确定性风控"],
  ["agent_cost", "Agent 成本"], ["job", "后台 Job"],
  ["reconciliation", "对账"], ["audit", "其他审计"],
];
const outcomeFilters: Array<[OutcomeFilter, string]> = [
  ["", "全部结果"], ["attention", "需要关注"], ["recovered", "已恢复"], ["healthy", "正常"],
];

export default function RiskAuditPage(): React.ReactElement {
  const initialFilters = useMemo(() => readLocationFilters(), []);
  const [filters, setFilters] = useState(initialFilters);
  const [state, setState] = useState<DataStateKind>("loading");
  const [readiness, setReadiness] = useState<Readiness | null>(null);
  const [data, setData] = useState<RiskAuditData | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [notice, setNotice] = useState<Notice>(null);
  const [pendingAction, setPendingAction] = useState<PendingAction | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const dataRef = useRef<RiskAuditData | null>(null);
  const loadVersion = useRef(0);
  const cursorHistory = useRef(new Set<string>());
  const actionInFlight = useRef(false);
  const returnFocus = useRef<HTMLElement | null>(null);

  const clearFacts = useCallback(() => {
    dataRef.current = null;
    setData(null);
    setReadiness(null);
  }, []);

  const refresh = useCallback(async (): Promise<boolean> => {
    const version = loadVersion.current + 1;
    loadVersion.current = version;
    setNotice(null);
    if (!dataRef.current) setState("loading");
    try {
      const [readinessResponse, auditResponse] = await Promise.all([
        fetchWithTimeout("/api/v1/readiness"),
        fetchWithTimeout(auditPath({
          kind: filters.kind,
          status: "",
          from: filters.from,
          to: filters.to,
        }, null)),
      ]);
      if (version !== loadVersion.current) return false;
      if ([401, 403].includes(readinessResponse.status) || [401, 403].includes(auditResponse.status)) {
        clearFacts();
        setState("forbidden");
        return false;
      }
      if (auditResponse.status === 404) {
        clearFacts();
        setState("empty");
        return true;
      }
      if (!auditResponse.ok) {
        setState(dataRef.current ? "stale" : auditResponse.status === 409 || auditResponse.status === 422 ? "partial" : "error");
        setNotice({ kind: "error", text: "无法读取脱敏审计事实；未触发任何恢复动作。" });
        return false;
      }
      const auditPayload = await auditResponse.json() as { data?: unknown };
      const nextData = validAuditPage(auditPayload.data);
      if (!nextData) throw new Error("risk-audit page invalid");
      dataRef.current = nextData;
      setData(nextData);
      cursorHistory.current = new Set(nextData.next_cursor ? [nextData.next_cursor] : []);

      let nextReadiness: Readiness | null = null;
      if (readinessResponse.status === 200 || readinessResponse.status === 503) {
        const readinessPayload = await readinessResponse.json() as { data?: unknown };
        nextReadiness = validReadiness(readinessPayload.data);
      }
      if (!nextReadiness) {
        setReadiness(null);
        setState("stale");
        setNotice({ kind: "error", text: "无法验证依赖就绪状态；保留脱敏事实，恢复动作保持关闭。" });
        return false;
      }
      setReadiness(nextReadiness);
      if (readinessResponse.status === 503 || nextReadiness.status !== "ready") {
        setState("stale");
        setNotice({ kind: "error", text: "依赖未就绪；保留最后读取的脱敏事实，恢复动作保持关闭。" });
        return false;
      }
      setState(nextData.summary.data_quality === "PASS" ? "ready" : "partial");
      return true;
    } catch (error) {
      if (version !== loadVersion.current) return false;
      setState(dataRef.current ? "stale" : "error");
      setNotice({
        kind: "error",
        text: error instanceof DOMException && error.name === "AbortError"
          ? "风险审计读取超时；保留最后事实，恢复动作保持关闭。"
          : "风险审计读取失败；未触发任何恢复动作。",
      });
      return false;
    }
  }, [clearFacts, filters.from, filters.kind, filters.to]);

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);

  function changeFilters(next: Filters): void {
    const sanitized = sanitizeFilters(next);
    setFilters(sanitized);
    replaceLocationFilters(sanitized);
  }

  async function loadMore(): Promise<void> {
    const current = dataRef.current;
    const cursor = current?.next_cursor;
    if (!current || !cursor || loadingMore || current.items.length >= MAX_EVENTS) return;
    const version = loadVersion.current;
    setLoadingMore(true);
    setNotice(null);
    try {
      const response = await fetchWithTimeout(auditPath(filters, cursor));
      if (version !== loadVersion.current) return;
      if ([401, 403].includes(response.status)) { clearFacts(); setState("forbidden"); return; }
      if (!response.ok) throw new Error("risk-audit page failed");
      const payload = await response.json() as { data?: unknown };
      const page = validAuditPage(payload.data);
      if (!page || current.items.length + page.items.length > MAX_EVENTS) throw new Error("risk-audit page invalid");
      const sequences = new Set(current.items.map((event) => event.event_seq));
      if (page.items.some((event) => sequences.has(event.event_seq))) throw new Error("duplicate audit event");
      if (page.next_cursor && cursorHistory.current.has(page.next_cursor)) throw new Error("audit cursor cycle");
      const next = { ...page, summary: current.summary, items: [...current.items, ...page.items] };
      dataRef.current = next;
      setData(next);
      if (page.next_cursor) cursorHistory.current.add(page.next_cursor);
    } catch {
      setNotice({ kind: "error", text: "无法加载更多审计事件；已保留当前有界列表。" });
    } finally {
      setLoadingMore(false);
    }
  }

  function openRetry(job: RetryableJob, trigger?: HTMLButtonElement): void {
    if (state !== "ready" || actionInFlight.current) return;
    returnFocus.current = trigger ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    setPendingAction({
      kind: "retry",
      job,
      idempotencyKey: `risk-retry-${crypto.randomUUID()}`,
      unknown: false,
    });
  }

  function openAcknowledge(eventSeq: number, trigger?: HTMLButtonElement): void {
    if (state !== "ready" || actionInFlight.current) return;
    returnFocus.current = trigger ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    setPendingAction({
      kind: "acknowledge",
      eventSeq,
      idempotencyKey: `drawdown-ack-${crypto.randomUUID()}`,
      unknown: false,
    });
  }

  function closeAction(): void {
    if (actionInFlight.current) return;
    setPendingAction(null);
    const target = returnFocus.current;
    globalThis.setTimeout(() => target?.focus(), 0);
  }

  async function confirmAction(): Promise<void> {
    const pending = pendingAction;
    if (!pending || actionInFlight.current || state !== "ready" && !pending.unknown) return;
    actionInFlight.current = true;
    setSubmitting(true);
    setNotice(null);
    try {
      const isRetry = pending.kind === "retry";
      const expectedVersion = isRetry ? pending.job.version : pending.eventSeq;
      const response = await fetch(isRetry
        ? `/api/v1/jobs/${encodeURIComponent(pending.job.job_id)}/retry`
        : "/api/v1/risk/drawdown-lock/acknowledge", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": pending.idempotencyKey,
          "if-match": `"${expectedVersion}"`,
        },
        body: isRetry ? "{}" : JSON.stringify({ expected_event_seq: expectedVersion }),
        signal: AbortSignal.timeout(WRITE_TIMEOUT_MS),
      });
      let payload: { data?: Record<string, unknown>; error?: { code?: string } } = {};
      try { payload = await response.json() as typeof payload; } catch { /* unknown result below */ }
      const valid = isRetry
        ? response.ok && payload.data?.job_id === pending.job.job_id
          && payload.data.status === "queued" && Number(payload.data.version) > pending.job.version
        : response.ok && payload.data?.state === "unlocked"
          && Number(payload.data.event_seq) > pending.eventSeq;
      if (valid) {
        setPendingAction(null);
        const refreshed = await refresh();
        setNotice(refreshed
          ? { kind: "success", text: isRetry ? "Job 已重新排队；已刷新权威事实。" : "回撤锁已确认解除；已刷新权威事实。" }
          : { kind: "error", text: "恢复动作已接受，但最新事实读取失败；后续恢复动作保持关闭。" });
        return;
      }
      if (response.status >= 500 || response.ok) {
        setPendingAction({ ...pending, unknown: true });
        setState("stale");
        setNotice({ kind: "error", text: "恢复结果未知。保持相同幂等键并关闭其他恢复动作。" });
        return;
      }
      setPendingAction(null);
      const refreshed = response.status === 409 ? await refresh() : false;
      setNotice({ kind: "error", text: refreshed
        ? "权威版本已变化；已刷新事实，请重新评估。"
        : "恢复动作未执行；请刷新权威事实后重试。" });
    } catch {
      setPendingAction({ ...pending, unknown: true });
      setState("stale");
      setNotice({ kind: "error", text: "恢复结果未知。保持相同幂等键并关闭其他恢复动作。" });
    } finally {
      actionInFlight.current = false;
      setSubmitting(false);
    }
  }

  async function exportAudit(): Promise<void> {
    setNotice(null);
    try {
      const response = await fetch(exportPath(filters), { signal: AbortSignal.timeout(READ_TIMEOUT_MS) });
      if (!response.ok
        || response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/x-ndjson") {
        throw new Error("audit export invalid");
      }
      const blob = await response.blob();
      const href = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = href;
      anchor.download = "nexus-risk-audit.ndjson.gz";
      anchor.click();
      URL.revokeObjectURL(href);
      setNotice({ kind: "success", text: "脱敏审计导出已生成。" });
    } catch {
      setNotice({ kind: "error", text: "无法生成脱敏审计导出；未下载任何文件。" });
    }
  }

  return <>
    <RiskAuditPanel
      state={state}
      readiness={readiness}
      data={data}
      kindFilter={filters.kind}
      statusFilter={filters.status}
      fromDate={filters.from}
      toDate={filters.to}
      loadingMore={loadingMore}
      notice={notice}
      onRefresh={() => void refresh()}
      onFilters={changeFilters}
      onLoadMore={() => void loadMore()}
      onRetry={openRetry}
      onAcknowledge={openAcknowledge}
      onExport={() => void exportAudit()}
    />
    {pendingAction ? <RecoveryDialog
      pending={pendingAction}
      submitting={submitting}
      onDismiss={closeAction}
      onConfirm={() => void confirmAction()}
    /> : null}
  </>;
}

export function RiskAuditPanel({ state, readiness, data, kindFilter, statusFilter, fromDate, toDate, loadingMore, notice, onRefresh, onFilters, onLoadMore, onRetry, onAcknowledge, onExport }: RiskAuditPanelProps): React.ReactElement {
  const hasFacts = data !== null && ["ready", "partial", "stale"].includes(state);
  const filteredEvents = data ? data.items.filter((event) => matchesOutcome(event, statusFilter)) : [];
  const recoveryEnabled = state === "ready" && readiness?.status === "ready";
  const filters: Filters = { kind: kindFilter, status: statusFilter, from: fromDate, to: toDate };
  return <section className="page-stack risk-audit-page" aria-label="风险与审计控制台" data-state={state}>
    <header className="page-heading">
      <div>
        <p className="eyebrow">DETERMINISTIC RISK / REDACTED AUDIT</p>
        <h1>风险与审计</h1>
        <p>验证依赖、数据质量、回撤锁和后台 Job。这里只展示脱敏事实；恢复动作不创建订单，也不连接真实券商写接口。</p>
      </div>
      <div className="page-actions">
        <span className="state-chip" data-state={state}>{dataStateLabels[state]}</span>
        <button className="button button--quiet" type="button" onClick={onRefresh}>
          <RefreshCw aria-hidden="true" size={17} />刷新事实
        </button>
      </div>
    </header>
    <div role="status" aria-label="风险审计操作状态" aria-live="polite" aria-atomic="true">
      {notice ? <p className={`inline-notice inline-notice--${notice.kind}`}>{notice.text}</p> : null}
    </div>
    {!hasFacts ? <DataState state={state} title="风险与审计事实" detail={stateDetail(state)} announce={false} /> : <>
      {state !== "ready" ? <p className="risk-audit-warning"><AlertTriangle aria-hidden="true" size={18} />{stateDetail(state)} 恢复写动作已关闭。</p> : null}
      <ReadinessBand readiness={readiness} />
      <AuditSummary data={data} recoveryEnabled={recoveryEnabled} onRetry={onRetry} onAcknowledge={onAcknowledge} />
      <section className="risk-audit-ledger" aria-labelledby="risk-audit-ledger-title">
        <header>
          <div><p className="eyebrow">APPEND-ONLY / REDACTED</p><h2 id="risk-audit-ledger-title">脱敏审计事件</h2><p>只显示固定投影；原始事件内容、账户金额和密钥不会进入浏览器。</p></div>
          <button className="button button--quiet" type="button" onClick={onExport} disabled={!validDateRange(fromDate, toDate)}>
            <Download aria-hidden="true" size={17} />导出 31 天内 NDJSON.gz
          </button>
        </header>
        <div className="risk-audit-filters" aria-label="审计事件筛选">
          <label>事件类型<select aria-label="事件类型" value={kindFilter} onChange={(event) => onFilters({ ...filters, kind: event.target.value as AuditKind })}>{auditKinds.map(([value, label]) => <option key={value || "all"} value={value}>{label}</option>)}</select></label>
          <label>结果状态<select aria-label="结果状态" value={statusFilter} onChange={(event) => onFilters({ ...filters, status: event.target.value as OutcomeFilter })}>{outcomeFilters.map(([value, label]) => <option key={value || "all"} value={value}>{label}</option>)}</select></label>
          <label>开始日期<input aria-label="开始日期" type="date" value={fromDate} max={toDate} onChange={(event) => onFilters({ ...filters, from: event.target.value })} /></label>
          <label>结束日期<input aria-label="结束日期" type="date" value={toDate} min={fromDate} onChange={(event) => onFilters({ ...filters, to: event.target.value })} /></label>
        </div>
        {filteredEvents.length === 0 ? <p className="risk-audit-empty">当前筛选下没有脱敏事件。</p> : <AuditTable events={filteredEvents} />}
        {data.next_cursor ? <button className="button button--quiet risk-audit-more" type="button" onClick={onLoadMore} disabled={loadingMore}>{loadingMore ? "正在加载" : "加载更多事件"}</button> : null}
      </section>
    </>}
  </section>;
}

function ReadinessBand({ readiness }: { readiness: Readiness | null }): React.ReactElement {
  const components = readiness?.components;
  return <section className="risk-readiness" aria-labelledby="risk-readiness-title">
    <header><div><p className="eyebrow">PROCESS READINESS</p><h2 id="risk-readiness-title">依赖就绪</h2></div><span className="state-chip" data-state={readiness?.status === "ready" ? "ready" : "stale"}>{readiness?.status === "ready" ? "全部就绪" : "未就绪"}</span></header>
    <dl>{(["database", "migration", "object_store", "worker"] as const).map((key) => <div key={key}><dt>{componentLabel(key)}</dt><dd data-state={components?.[key] ?? "unavailable"}>{componentStateLabel(components?.[key] ?? "unavailable")}</dd></div>)}</dl>
  </section>;
}

function AuditSummary({ data, recoveryEnabled, onRetry, onAcknowledge }: { data: RiskAuditData; recoveryEnabled: boolean; onRetry: RiskAuditPanelProps["onRetry"]; onAcknowledge: RiskAuditPanelProps["onAcknowledge"] }): React.ReactElement {
  const lock = data.summary.drawdown_lock;
  return <section className="risk-summary" aria-label="风险审计摘要">
    <article data-tone={data.summary.data_quality === "PASS" ? "ready" : "attention"}><Archive aria-hidden="true" size={22} /><div><p className="eyebrow">DATA QUALITY</p><h2>{data.summary.data_quality}</h2><p>质量门结果来自确定性日流水线。</p></div></article>
    <article data-tone={lock.state === "unlocked" ? "ready" : "attention"}><ShieldAlert aria-hidden="true" size={22} /><div><p className="eyebrow">DRAWDOWN LOCK</p><h2>{drawdownLabel(lock.state)}</h2><p>{lock.event_seq ? `事件版本 ${lock.event_seq}` : "尚无可确认版本"}</p>{lock.acknowledge_allowed && lock.event_seq ? <button className="button button--danger" type="button" disabled={!recoveryEnabled} aria-label="确认解除回撤锁" onClick={(event) => onAcknowledge(lock.event_seq!, event.currentTarget)}>确认解除回撤锁</button> : null}</div></article>
    <article data-tone={data.summary.failed_jobs.count === 0 ? "ready" : "attention"}><RotateCcw aria-hidden="true" size={22} /><div><p className="eyebrow">FAILED JOBS</p><h2>{data.summary.failed_jobs.count}</h2><p>仅 manual_safe 且未超过两次的 Job 可逐个重试。</p></div></article>
    {data.summary.failed_jobs.retryable.map((job) => <article className="retryable-job" data-tone="attention" key={job.job_id}><AlertTriangle aria-hidden="true" size={22} /><div><p className="eyebrow">MANUAL SAFE / {job.status}</p><h2>{job.job_type}</h2><p>版本 {job.version} · 已手动重试 {job.manual_retry_count}/2 · {formatTime(job.finished_at)}</p><p>{job.last_error_code ?? "未公开错误代码"} · Job {shortId(job.job_id)}</p><button className="button button--quiet" type="button" disabled={!recoveryEnabled} aria-label={`重试 ${job.job_type}`} onClick={(event) => onRetry(job, event.currentTarget)}>安全重试</button></div></article>)}
  </section>;
}

function AuditTable({ events }: { events: AuditEvent[] }): React.ReactElement {
  // Bounded horizontal data regions must remain keyboard-scrollable on narrow screens.
  // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
  return <div className="table-scroll" role="region" aria-label="脱敏审计事件表，可横向滚动" tabIndex={0}><table className="risk-audit-table" aria-label="脱敏审计事件"><caption>事件序号、类型、动作和脱敏 outcome；哈希链由后端验证。</caption><thead><tr><th scope="col">序号</th><th scope="col">发生时间</th><th scope="col">类型</th><th scope="col">动作</th><th scope="col">Actor</th><th scope="col">脱敏结果</th><th scope="col">Schema</th></tr></thead><tbody>{events.map((event) => <tr key={event.event_seq}><td className="numeric-data">#{event.event_seq}</td><td className="numeric-data">{formatTime(event.occurred_at)}</td><td>{kindLabel(event.kind)}</td><th scope="row">{event.action}</th><td>{event.actor_kind}</td><td>{outcomeText(event.outcome)}</td><td className="numeric-data">v{event.schema_version}</td></tr>)}</tbody></table></div>;
}

function RecoveryDialog({ pending, submitting, onDismiss, onConfirm }: { pending: PendingAction; submitting: boolean; onDismiss: () => void; onConfirm: () => void }): React.ReactElement {
  const retry = pending.kind === "retry";
  const title = retry ? `确认重试 ${pending.job.job_type}` : "确认解除回撤锁";
  const backRef = useRef<HTMLButtonElement>(null);
  useEffect(() => { backRef.current?.focus(); }, []);
  return <div className="confirmation-dialog" role="presentation" onKeyDown={(event) => { if (event.key === "Escape" && !submitting) onDismiss(); }}>
    <div role="dialog" aria-modal="true" aria-label={title}>
      <p className="eyebrow">DETERMINISTIC RECOVERY</p><h3>{title}</h3>
      <p>{retry
        ? `只重新排队这个 manual_safe Job（版本 ${pending.job.version}），不改变 payload、不创建订单。`
        : `只确认事件版本 ${pending.eventSeq} 的回撤锁恢复资格；不会创建或提交订单。`}</p>
      {pending.unknown ? <p className="inline-notice inline-notice--error">上次结果未知；继续会复用同一幂等键。</p> : null}
      <div className="confirmation-dialog__actions"><button ref={backRef} className="button button--quiet" type="button" onClick={onDismiss} disabled={submitting}>返回审计</button><button className="button button--danger" type="button" onClick={onConfirm} disabled={submitting}>{pending.unknown ? "使用同一幂等键继续确认" : retry ? "确认安全重试" : "确认解除"}</button></div>
    </div>
  </div>;
}

function auditPath(filters: Filters, cursor: string | null): string {
  const query = new URLSearchParams();
  if (filters.from) query.set("from", `${filters.from}T00:00:00Z`);
  if (filters.kind) query.set("kind", filters.kind);
  query.set("limit", String(PAGE_SIZE));
  if (filters.to) query.set("to", nextUtcDay(filters.to));
  if (cursor) query.set("cursor", cursor);
  query.sort();
  return `/api/v1/risk-audit?${query.toString()}`;
}

function exportPath(filters: Filters): string {
  const query = new URLSearchParams();
  query.set("from", `${filters.from}T00:00:00Z`);
  if (filters.kind) query.set("kind", filters.kind);
  query.set("to", nextUtcDay(filters.to));
  query.sort();
  return `/api/v1/risk-audit/export?${query.toString()}`;
}

function readLocationFilters(): Filters {
  const defaults = defaultDates();
  if (typeof window === "undefined") return { kind: "", status: "", ...defaults };
  const search = new URLSearchParams(window.location.search);
  return sanitizeFilters({
    kind: (auditKinds.some(([value]) => value === search.get("kind")) ? search.get("kind") : "") as AuditKind,
    status: (outcomeFilters.some(([value]) => value === search.get("status")) ? search.get("status") : "") as OutcomeFilter,
    from: search.get("from") ?? defaults.from,
    to: search.get("to") ?? defaults.to,
  });
}

function replaceLocationFilters(filters: Filters): void {
  const query = new URLSearchParams();
  if (filters.kind) query.set("kind", filters.kind);
  query.set("from", filters.from);
  query.set("to", filters.to);
  if (filters.status) query.set("status", filters.status);
  window.history.replaceState({}, "", `${window.location.pathname}?${query.toString()}`);
}

function sanitizeFilters(filters: Filters): Filters {
  const defaults = defaultDates();
  const from = isIsoDate(filters.from) ? filters.from : defaults.from;
  const to = isIsoDate(filters.to) ? filters.to : defaults.to;
  return {
    kind: auditKinds.some(([value]) => value === filters.kind) ? filters.kind : "",
    status: outcomeFilters.some(([value]) => value === filters.status) ? filters.status : "",
    from: from <= to ? from : defaults.from,
    to: from <= to ? to : defaults.to,
  };
}

function validAuditPage(value: unknown): RiskAuditData | null {
  if (!isPlain(value) || !isPlain(value.summary) || !Array.isArray(value.items)
    || value.items.length > 100 || !isPlain(value.summary.failed_jobs)
    || !Array.isArray(value.summary.failed_jobs.retryable)
    || value.summary.failed_jobs.retryable.length > 20
    || value.next_cursor !== null && (typeof value.next_cursor !== "string" || value.next_cursor.length > 2048)) return null;
  return value as unknown as RiskAuditData;
}

function validReadiness(value: unknown): Readiness | null {
  if (!isPlain(value) || !["ready", "unready"].includes(String(value.status)) || !isPlain(value.components)) return null;
  const components = value.components;
  const states = new Set(["ready", "unavailable", "incompatible", "stale"]);
  const keys = ["database", "migration", "object_store", "worker"];
  if (Object.keys(value).length !== 2 || Object.keys(components).length !== 4
    || !keys.every((key) => states.has(String(components[key])))) return null;
  return value as unknown as Readiness;
}

function matchesOutcome(event: AuditEvent, filter: OutcomeFilter): boolean {
  if (!filter) return true;
  const values = Object.values(event.outcome).map((value) => String(value).toLowerCase());
  if (filter === "attention") return values.some((value) => ["failed", "dead_letter", "locked", "rejected", "error", "fail"].includes(value));
  if (filter === "recovered") return values.some((value) => ["queued", "unlocked", "recovered", "acknowledged"].includes(value));
  return values.some((value) => ["pass", "ready", "success", "succeeded", "ok"].includes(value));
}

async function fetchWithTimeout(path: string): Promise<Response> {
  return fetch(path, { signal: AbortSignal.timeout(READ_TIMEOUT_MS) });
}

function defaultDates(): { from: string; to: string } {
  const to = new Date();
  const from = new Date(to);
  from.setUTCDate(from.getUTCDate() - 7);
  return { from: from.toISOString().slice(0, 10), to: to.toISOString().slice(0, 10) };
}

function nextUtcDay(date: string): string {
  const value = new Date(`${date}T00:00:00Z`);
  value.setUTCDate(value.getUTCDate() + 1);
  return value.toISOString().replace(".000Z", "Z");
}

function validDateRange(from: string, to: string): boolean {
  if (!isIsoDate(from) || !isIsoDate(to) || from > to) return false;
  return Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`) < 31 * 24 * 60 * 60 * 1000;
}

function isIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 10) === value;
}

function outcomeText(outcome: AuditEvent["outcome"]): string {
  const entries = Object.entries(outcome).sort(([left], [right]) => left.localeCompare(right));
  return entries.length ? entries.map(([key, value]) => `${key}: ${String(value)}`).join(" · ") : "无公开结果字段";
}

function stateDetail(state: DataStateKind): string {
  if (state === "loading") return "正在读取就绪状态与脱敏审计事实。";
  if (state === "empty") return "当前没有可显示的审计事实。";
  if (state === "partial") return "数据质量不是 PASS；事实可读，但恢复动作需等待质量门恢复。";
  if (state === "stale") return "依赖状态或审计事实已过期。";
  if (state === "forbidden") return "当前身份不能读取风险审计事实。";
  if (state === "error") return "无法读取风险审计事实，系统保持观望。";
  return "依赖、数据质量与审计事实均已验证。";
}

function formatTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? "未知时点" : `${date.toISOString().slice(0, 19).replace("T", " ")} UTC`;
}

function shortId(value: string): string { return `${value.slice(0, 8)}…${value.slice(-4)}`; }
function componentLabel(value: string): string { return ({ database: "数据库", migration: "迁移版本", object_store: "对象存储", worker: "后台 Worker" } as Record<string, string>)[value] ?? value; }
function componentStateLabel(value: string): string { return ({ ready: "就绪", unavailable: "不可用", incompatible: "不兼容", stale: "过期" } as Record<string, string>)[value] ?? "不可用"; }
function drawdownLabel(value: string): string { return ({ unlocked: "未锁定", locked: "已锁定", recovery_eligible: "可确认恢复", unknown: "未知" } as Record<string, string>)[value] ?? "未知"; }
function kindLabel(value: AuditEvent["kind"]): string { return auditKinds.find(([kind]) => kind === value)?.[1] ?? value; }
function isPlain(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
