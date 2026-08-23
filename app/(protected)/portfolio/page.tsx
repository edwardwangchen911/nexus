"use client";

import { RefreshCw, ShieldCheck } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { PortfolioSummary } from "../../components/portfolio-summary";
import type { components } from "../../lib/generated/schema";

export type PortfolioItem = components["schemas"]["PortfolioListItemResponse"];
export type PortfolioSnapshot = components["schemas"]["PortfolioSnapshotResponse"];
export type BrokerSyncRun = components["schemas"]["BrokerSyncRunResponse"];

type Notice = { kind: "error" | "success"; text: string } | null;
type PendingSync = { portfolioId: string; idempotencyKey: string; body: string };
type TrackedSync = { location: string; run: BrokerSyncRun };
type PortfolioPanelProps = {
  state: DataStateKind;
  portfolios: PortfolioItem[];
  snapshot: PortfolioSnapshot | null;
  selectedPortfolioId: string | null;
  syncing: boolean;
  unresolvedSync: boolean;
  notice: Notice;
  syncRun?: BrokerSyncRun | null;
  syncRefreshing?: boolean;
  onSelect: (portfolioId: string) => void;
  onRefresh: () => void;
  onSync: () => void;
  onResolveSync: () => void;
  onRefreshSync?: () => void;
};

const LOAD_TIMEOUT_MS = 10_000;
const SYNC_CONFIRM_TIMEOUT_MS = 35_000;
const MAX_PORTFOLIO_PAGES = 10;
const SYNC_POLL_MS = 5_000;

export default function PortfolioPage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [portfolios, setPortfolios] = useState<PortfolioItem[]>([]);
  const [snapshot, setSnapshot] = useState<PortfolioSnapshot | null>(null);
  const [selectedPortfolioId, setSelectedPortfolioId] = useState<string | null>(null);
  const [syncing, setSyncing] = useState(false);
  const [unresolvedSync, setUnresolvedSync] = useState<PendingSync | null>(null);
  const [trackedSync, setTrackedSync] = useState<TrackedSync | null>(null);
  const [syncRefreshing, setSyncRefreshing] = useState(false);
  const [notice, setNotice] = useState<Notice>(null);
  const loadVersion = useRef(0);
  const syncInFlight = useRef(false);
  const portfoliosRef = useRef<PortfolioItem[]>([]);
  const snapshotRef = useRef<PortfolioSnapshot | null>(null);
  const selectedPortfolioIdRef = useRef<string | null>(null);
  const trackedSyncRef = useRef<TrackedSync | null>(null);

  const loadSnapshot = useCallback(async (portfolioId: string, version: number) => {
    const response = await fetchWithTimeout(`/api/v1/portfolios/${encodeURIComponent(portfolioId)}/snapshots/latest`);
    if (version !== loadVersion.current) return;
    if (response.status === 401 || response.status === 403) {
      snapshotRef.current = null; setSnapshot(null); setState("forbidden"); return;
    }
    if (response.status === 404) {
      snapshotRef.current = null; setSnapshot(null); setState("empty"); return;
    }
    if (!response.ok) throw new Error("snapshot read failed");
    const envelope = await response.json() as { data: PortfolioSnapshot };
    if (version !== loadVersion.current) return;
    if (envelope.data.portfolio_id !== portfolioId) throw new Error("snapshot portfolio mismatch");
    snapshotRef.current = envelope.data; setSnapshot(envelope.data);
    setState(snapshotState(envelope.data.status));
  }, []);

  const refresh = useCallback(async () => {
    const version = loadVersion.current + 1;
    loadVersion.current = version;
    setNotice(null);
    if (portfoliosRef.current.length === 0) setState("loading");
    try {
      const items: PortfolioItem[] = [];
      const portfolioIds = new Set<string>();
      const cursors = new Set<string>();
      let cursor: string | null = null;
      for (let page = 0; page < MAX_PORTFOLIO_PAGES; page += 1) {
        const path = `/api/v1/portfolios?kind=real_read_only&limit=50${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`;
        const response = await fetchWithTimeout(path);
        if (version !== loadVersion.current) return;
        if (response.status === 401 || response.status === 403) {
          portfoliosRef.current = []; snapshotRef.current = null;
          setPortfolios([]); setSnapshot(null); setState("forbidden"); return;
        }
        if (!response.ok) throw new Error("portfolio read failed");
        const envelope = await response.json() as { data?: { items?: PortfolioItem[]; next_cursor?: string | null } };
        if (version !== loadVersion.current) return;
        if (!envelope.data || !Array.isArray(envelope.data.items)) throw new Error("portfolio page invalid");
        for (const item of envelope.data.items) {
          if (portfolioIds.has(item.portfolio_id)) throw new Error("duplicate portfolio");
          portfolioIds.add(item.portfolio_id); items.push(item);
        }
        const next = envelope.data.next_cursor;
        if (next === null) { cursor = null; break; }
        if (typeof next !== "string" || next.length === 0 || next.length > 2048 || cursors.has(next)) throw new Error("portfolio cursor invalid");
        if (page === MAX_PORTFOLIO_PAGES - 1) throw new Error("portfolio page bound exceeded");
        cursors.add(next); cursor = next;
      }
      if (version !== loadVersion.current) return;
      portfoliosRef.current = items; setPortfolios(items);
      if (items.length === 0) {
        selectedPortfolioIdRef.current = null; snapshotRef.current = null;
        setSelectedPortfolioId(null); setSnapshot(null); setState("empty"); return;
      }
      const selected = items.some((item) => item.portfolio_id === selectedPortfolioIdRef.current)
        ? selectedPortfolioIdRef.current!
        : items[0].portfolio_id;
      if (selected !== selectedPortfolioIdRef.current) {
        snapshotRef.current = null;
        setSnapshot(null);
        setState("loading");
      }
      selectedPortfolioIdRef.current = selected; setSelectedPortfolioId(selected);
      await loadSnapshot(selected, version);
    } catch (error) {
      if (version !== loadVersion.current) return;
      setState(snapshotRef.current ? "stale" : "error");
      setNotice({ kind: "error", text: error instanceof DOMException && error.name === "AbortError"
        ? "组合读取超时。保留最后已知快照，不触发任何交易动作。"
        : "无法读取组合事实。当前状态保持只读。" });
    }
  }, [loadSnapshot]);

  const refreshSyncRun = useCallback(async () => {
    const tracked = trackedSyncRef.current;
    if (!tracked || tracked.run.portfolio_id !== selectedPortfolioIdRef.current) return;
    setSyncRefreshing(true);
    try {
      const response = await fetchWithTimeout(tracked.location);
      if (!response.ok) throw new Error("sync lookup failed");
      const payload = await response.json() as { data?: BrokerSyncRun };
      const updated = validateSyncRun(payload.data, tracked);
      if (!updated || tracked.run.portfolio_id !== selectedPortfolioIdRef.current) throw new Error("sync lookup mismatch");
      trackedSyncRef.current = updated; setTrackedSync(updated);
    } catch {
      setNotice({ kind: "error", text: "无法刷新同步详情。冻结组合快照保持不变。" });
    } finally { setSyncRefreshing(false); }
  }, []);

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);

  useEffect(() => {
    if (trackedSync?.run.status !== "ACTIVE") return;
    const timer = globalThis.setTimeout(() => void refreshSyncRun(), SYNC_POLL_MS);
    return () => globalThis.clearTimeout(timer);
  }, [refreshSyncRun, trackedSync]);

  async function selectPortfolio(portfolioId: string): Promise<void> {
    const version = loadVersion.current + 1;
    loadVersion.current = version;
    selectedPortfolioIdRef.current = portfolioId; snapshotRef.current = null;
    if (trackedSyncRef.current?.run.portfolio_id !== portfolioId) {
      trackedSyncRef.current = null; setTrackedSync(null);
    }
    setSelectedPortfolioId(portfolioId); setSnapshot(null); setState("loading"); setNotice(null);
    try { await loadSnapshot(portfolioId, version); }
    catch { if (version === loadVersion.current) { setState("error"); setNotice({ kind: "error", text: "无法读取该组合的冻结快照。" }); } }
  }

  async function syncSelected(): Promise<void> {
    if (!selectedPortfolioId || syncing || syncInFlight.current || unresolvedSync || trackedSync?.run.status === "ACTIVE") return;
    await confirmSync({
      portfolioId: selectedPortfolioId,
      idempotencyKey: crypto.randomUUID(),
      body: JSON.stringify({ portfolio_id: selectedPortfolioId }),
    });
  }

  async function confirmSync(pending: PendingSync): Promise<void> {
    if (syncInFlight.current) return;
    syncInFlight.current = true;
    setSyncing(true); setNotice(null);
    try {
      for (let attempt = 0; attempt < 2; attempt += 1) {
        try {
        const response = await fetch("/api/v1/broker-sync-runs", {
          method: "POST",
          headers: { "content-type": "application/json", "idempotency-key": pending.idempotencyKey },
          body: pending.body,
          signal: AbortSignal.timeout(SYNC_CONFIRM_TIMEOUT_MS),
        });
        const payload = await response.json();
        if (!response.ok) {
          if (payload?.error?.code === "INTERNAL_ERROR") {
            if (attempt === 0) continue;
            setUnresolvedSync(pending);
            setNotice({ kind: "error", text: "同步请求结果仍未知。当前组合的新同步保持锁定；请使用同一幂等键继续确认。" });
            return;
          }
          setUnresolvedSync(null);
          setNotice({ kind: "error", text: syncErrorMessage(payload?.error?.code, response.status, response.headers.get("retry-after")) });
          return;
        }
        const accepted = acceptedSync(payload, response.headers.get("location"), pending);
        if (!accepted) {
          setUnresolvedSync(pending);
          setNotice({ kind: "error", text: "同步请求结果仍未知。当前组合的新同步保持锁定；请使用同一幂等键继续确认。" });
          return;
        }
        setUnresolvedSync(null);
        trackedSyncRef.current = accepted; setTrackedSync(accepted);
        setNotice({ kind: "success", text: attempt === 0
          ? "复星只读同步任务已登记。当前快照不会被静默覆盖。"
          : "首次结果未知；已用同一幂等键确认同步任务。" });
        return;
        } catch {
          if (attempt === 0) continue;
          setUnresolvedSync(pending);
          setNotice({ kind: "error", text: "同步请求结果仍未知。当前组合的新同步保持锁定；请使用同一幂等键继续确认。" });
        }
      }
    } finally {
      syncInFlight.current = false;
      setSyncing(false);
    }
  }

  return <PortfolioPanel state={state} portfolios={portfolios} snapshot={snapshot}
    selectedPortfolioId={selectedPortfolioId} syncing={syncing} unresolvedSync={unresolvedSync !== null} notice={notice}
    syncRun={trackedSync?.run ?? null} syncRefreshing={syncRefreshing}
    onSelect={(id) => void selectPortfolio(id)} onRefresh={() => void refresh()}
    onSync={() => void syncSelected()}
    onResolveSync={() => { if (unresolvedSync) void confirmSync(unresolvedSync); }}
    onRefreshSync={() => void refreshSyncRun()} />;
}

export function PortfolioPanel({ state, portfolios, snapshot, selectedPortfolioId, syncing, unresolvedSync, notice, syncRun = null, syncRefreshing = false, onSelect, onRefresh, onSync, onResolveSync, onRefreshSync }: PortfolioPanelProps): React.ReactElement {
  const hasData = ["ready", "partial", "stale"].includes(state) && snapshot !== null;
  const selected = portfolios.find((item) => item.portfolio_id === selectedPortfolioId) ?? null;
  const currencies = selected && snapshot ? [...new Set([
    ...selected.original_currencies,
    ...snapshot.cash.map((item) => item.currency),
    ...snapshot.positions.map((item) => item.currency),
    ...snapshot.open_orders.map((item) => item.currency),
  ])].sort() : [];
  return (
    <section className="page-stack portfolio-page" aria-label="组合事实页面" data-state={state}>
      <header className="page-heading">
        <div><p className="eyebrow">PORTFOLIO / FROZEN FACTS</p><h1>组合事实</h1><p>复星真实持仓与内部模拟盘分账展示。未有合格 FX 证据时，不合并资产净值。</p></div>
        <div className="portfolio-page__actions"><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />刷新快照</button>{unresolvedSync ? <button className="button button--primary" type="button" onClick={onResolveSync} disabled={syncing}><ShieldCheck aria-hidden="true" size={17} />{syncing ? "正在继续确认" : "使用同一幂等键继续确认"}</button> : selected?.kind === "real_read_only" && syncRun?.status !== "ACTIVE" ? <button className="button button--primary" type="button" onClick={onSync} disabled={syncing}><ShieldCheck aria-hidden="true" size={17} />{syncing ? "正在确认同步任务" : "同步复星只读事实"}</button> : null}</div>
      </header>
      <div role="status" aria-label="组合操作状态" aria-live="polite" aria-atomic="true">
        {notice ? <p className={`inline-notice inline-notice--${notice.kind}`}>{notice.text}</p> : null}
      </div>
      {!selected ? <DataState state={state} title="组合快照" detail={stateDetail(state)} announce={false} /> : (
        <div className="portfolio-workspace">
          <nav className="portfolio-rail" aria-label="组合选择"><ul>{portfolios.map((item) => <li key={item.portfolio_id}><button type="button" aria-pressed={item.portfolio_id === selectedPortfolioId} onClick={() => onSelect(item.portfolio_id)}><strong>{item.name}</strong><span>{kindLabel(item.kind)} · {item.coverage_scope}</span><span>{item.original_currencies.join(" / ") || "币种未知"}</span></button></li>)}</ul></nav>
          <div className="portfolio-detail">
            {!hasData || !snapshot ? <DataState state={state} title={`${selected.name} 快照`} detail={stateDetail(state)} announce={false} /> : <>
              <div className="portfolio-snapshot-band"><div><p className="eyebrow">FROZEN SNAPSHOT</p><h2>{selected.name}</h2></div><dl><div><dt>状态</dt><dd>{snapshot.status}</dd></div><div><dt>覆盖范围</dt><dd>{snapshot.coverage_scope}</dd></div><div><dt>快照时点</dt><dd>{formatTime(snapshot.as_of)}</dd></div></dl></div>
              {snapshot.status === "RECONCILIATION_REQUIRED" ? <p className="inline-notice inline-notice--error">双读对账未通过。快照仅供展示，不能用作优化基线。</p> : null}
              {currencies.map((currency) => <PortfolioSummary key={currency} portfolio={selected} snapshot={snapshot} currency={currency} />)}
            </>}
            {syncRun?.portfolio_id === selected.portfolio_id ? <SyncRunDetails run={syncRun} refreshing={syncRefreshing} onRefresh={onRefreshSync} /> : null}
          </div>
        </div>
      )}
    </section>
  );
}

function SyncRunDetails({ run, refreshing, onRefresh }: { run: BrokerSyncRun; refreshing: boolean; onRefresh?: () => void }): React.ReactElement {
  return (
    <section className="portfolio-sync-run" aria-labelledby="portfolio-sync-run-heading">
      <header><div><p className="eyebrow">READ-ONLY SYNC RUN</p><h2 id="portfolio-sync-run-heading">同步详情</h2></div><button className="button button--quiet" type="button" onClick={onRefresh} disabled={refreshing}><RefreshCw aria-hidden="true" size={17} />{refreshing ? "正在刷新" : "刷新同步状态"}</button></header>
      <dl><div><dt>状态</dt><dd>{run.status}</dd></div><div><dt>尝试次数</dt><dd>{run.attempt_count}</dd></div><div><dt>现金流质量</dt><dd>{run.cash_flow_quality}</dd></div><div><dt>接受时间</dt><dd>{formatTime(run.accepted_at)}</dd></div><div><dt>完成时间</dt><dd>{formatTime(run.completed_at)}</dd></div></dl>
      {run.failure_category ? <p className="inline-notice inline-notice--error">同步失败类别：{run.failure_category}。Provider 原始原因不在页面展示。</p> : null}
    </section>
  );
}

function acceptedSync(payload: unknown, location: string | null, pending: PendingSync): TrackedSync | null {
  if (!isRecord(payload) || !isRecord(payload.data)) return null;
  const run = payload.data as BrokerSyncRun;
  const expected = typeof run.broker_sync_run_id === "string" ? `/api/v1/broker-sync-runs/${run.broker_sync_run_id}` : "";
  if (location !== expected || run.portfolio_id !== pending.portfolioId) return null;
  return { location, run };
}

function validateSyncRun(value: BrokerSyncRun | undefined, tracked: TrackedSync): TrackedSync | null {
  if (!value || value.broker_sync_run_id !== tracked.run.broker_sync_run_id || value.portfolio_id !== tracked.run.portfolio_id) return null;
  return { location: tracked.location, run: value };
}

function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
async function fetchWithTimeout(path: string): Promise<Response> { return fetch(path, { signal: AbortSignal.timeout(LOAD_TIMEOUT_MS) }); }
function snapshotState(status: PortfolioSnapshot["status"]): DataStateKind { return status === "READY" ? "ready" : status === "STALE" ? "stale" : "partial"; }
function kindLabel(kind: PortfolioItem["kind"]): string { return ({ real_read_only: "真实只读", internal_paper: "内部模拟", shadow: "影子组合" })[kind]; }
function formatTime(value: string | null): string { if (!value) return "Provider 时点缺失"; const date = new Date(value); return Number.isNaN(date.valueOf()) ? "无效时间" : date.toISOString().replace("T", " ").replace(".000Z", " UTC"); }
function syncErrorMessage(code: unknown, status: number, retryAfter: string | null): string { if (code === "IDEMPOTENCY_CONFLICT") return "幂等键已被不同请求使用；未创建新任务。"; if (status === 429) return `只读同步冷却中${retryAfter && /^\d+$/.test(retryAfter) ? `，请等待 ${retryAfter} 秒` : ""}。当前快照保持不变。`; if (code === "VALIDATION_ERROR") return "同步请求不合法；未创建新任务。"; return "同步任务未登记。当前快照保持不变。"; }
function stateDetail(state: DataStateKind): string { return ({ loading: "正在读取脱敏组合和冻结快照。", empty: "尚无可展示的冻结组合快照。", ready: "组合快照已就绪。", partial: "快照仅供研究，不可作为优化基线。", stale: "显示最后已知快照，不触发业务动作。", error: "读取失败；未开启任何交易能力。", forbidden: "当前身份无权读取组合事实。" })[state]; }
