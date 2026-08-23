"use client";

import { RefreshCw, ShieldCheck } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { PortfolioSummary } from "../../components/portfolio-summary";

export type PortfolioItem = {
  portfolio_id: string;
  name: string;
  kind: "real_read_only" | "internal_paper" | "shadow";
  coverage_scope: "broker_account" | "internal_paper" | "shadow";
  provider: string;
  latest_snapshot_status: PortfolioSnapshot["status"] | null;
  latest_snapshot_as_of: string | null;
  original_currencies: string[];
  last_sync_run_status: string | null;
};

export type PortfolioSnapshot = {
  portfolio_snapshot_id: string;
  portfolio_id: string;
  coverage_scope: "broker_account" | "internal_paper" | "shadow";
  status: "READY" | "PARTIAL" | "STALE" | "RECONCILIATION_REQUIRED";
  cash: Array<{ currency: string; cash_balance: string; available_cash: string; provider_as_of: string | null }>;
  positions: Array<{ instrument_id: string; symbol: string | null; exchange: string; currency: string; quantity: string; available_quantity: string; frozen_quantity: string; avg_cost: string | null; diluted_cost: string | null; current_price: string | null; provider_as_of: string | null; read_only: boolean }>;
  open_orders: Array<{ broker_order_snapshot_id: string; instrument_id: string; symbol: string | null; side: string; quantity: string; filled_quantity: string; price: string | null; status: string; updated_at: string | null; read_only: boolean }>;
  converted_view: unknown | null;
  as_of: string | null;
};

type Notice = { kind: "error" | "success"; text: string } | null;
type PortfolioPanelProps = {
  state: DataStateKind;
  portfolios: PortfolioItem[];
  snapshot: PortfolioSnapshot | null;
  selectedPortfolioId: string | null;
  syncing: boolean;
  notice: Notice;
  onSelect: (portfolioId: string) => void;
  onRefresh: () => void;
  onSync: () => void;
};

const LOAD_TIMEOUT_MS = 10_000;

export default function PortfolioPage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [portfolios, setPortfolios] = useState<PortfolioItem[]>([]);
  const [snapshot, setSnapshot] = useState<PortfolioSnapshot | null>(null);
  const [selectedPortfolioId, setSelectedPortfolioId] = useState<string | null>(null);
  const [syncing, setSyncing] = useState(false);
  const [notice, setNotice] = useState<Notice>(null);
  const loadVersion = useRef(0);
  const portfoliosRef = useRef<PortfolioItem[]>([]);
  const snapshotRef = useRef<PortfolioSnapshot | null>(null);
  const selectedPortfolioIdRef = useRef<string | null>(null);

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
      const response = await fetchWithTimeout("/api/v1/portfolios?kind=real_read_only&limit=50");
      if (version !== loadVersion.current) return;
      if (response.status === 401 || response.status === 403) {
        portfoliosRef.current = []; snapshotRef.current = null;
        setPortfolios([]); setSnapshot(null); setState("forbidden"); return;
      }
      if (!response.ok) throw new Error("portfolio read failed");
      const envelope = await response.json() as { data: { items: PortfolioItem[]; next_cursor: string | null } };
      const items = envelope.data.items;
      portfoliosRef.current = items; setPortfolios(items);
      if (items.length === 0) {
        selectedPortfolioIdRef.current = null; snapshotRef.current = null;
        setSelectedPortfolioId(null); setSnapshot(null); setState("empty"); return;
      }
      const selected = items.some((item) => item.portfolio_id === selectedPortfolioIdRef.current)
        ? selectedPortfolioIdRef.current!
        : items[0].portfolio_id;
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

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);

  async function selectPortfolio(portfolioId: string): Promise<void> {
    const version = loadVersion.current + 1;
    loadVersion.current = version;
    selectedPortfolioIdRef.current = portfolioId; snapshotRef.current = null;
    setSelectedPortfolioId(portfolioId); setSnapshot(null); setState("loading"); setNotice(null);
    try { await loadSnapshot(portfolioId, version); }
    catch { if (version === loadVersion.current) { setState("error"); setNotice({ kind: "error", text: "无法读取该组合的冻结快照。" }); } }
  }

  async function syncSelected(): Promise<void> {
    if (!selectedPortfolioId || syncing) return;
    const idempotencyKey = crypto.randomUUID();
    const body = JSON.stringify({ portfolio_id: selectedPortfolioId });
    setSyncing(true); setNotice(null);
    try {
      for (let attempt = 0; attempt < 2; attempt += 1) {
        try {
        const response = await fetch("/api/v1/broker-sync-runs", {
          method: "POST",
          headers: { "content-type": "application/json", "idempotency-key": idempotencyKey },
          body,
          signal: AbortSignal.timeout(30_000),
        });
        const payload = await response.json();
        if (!response.ok) {
          if (payload?.error?.code === "INTERNAL_ERROR" && attempt === 0) continue;
          setNotice({ kind: "error", text: syncErrorMessage(payload?.error?.code) });
          return;
        }
        setNotice({ kind: "success", text: attempt === 0
          ? "复星只读同步任务已登记。当前快照不会被静默覆盖。"
          : "首次结果未知；已用同一幂等键确认同步任务。" });
        return;
        } catch {
          if (attempt === 0) continue;
          setNotice({ kind: "error", text: "同步请求结果仍未知。请先刷新权威状态，不要反复触发。" });
        }
      }
    } finally {
      setSyncing(false);
    }
  }

  return <PortfolioPanel state={state} portfolios={portfolios} snapshot={snapshot}
    selectedPortfolioId={selectedPortfolioId} syncing={syncing} notice={notice}
    onSelect={(id) => void selectPortfolio(id)} onRefresh={() => void refresh()}
    onSync={() => void syncSelected()} />;
}

export function PortfolioPanel({ state, portfolios, snapshot, selectedPortfolioId, syncing, notice, onSelect, onRefresh, onSync }: PortfolioPanelProps): React.ReactElement {
  const hasData = ["ready", "partial", "stale"].includes(state) && snapshot !== null;
  const selected = portfolios.find((item) => item.portfolio_id === selectedPortfolioId) ?? null;
  const currencies = selected && snapshot ? [...new Set([...selected.original_currencies, ...snapshot.cash.map((item) => item.currency), ...snapshot.positions.map((item) => item.currency)])].sort() : [];
  return (
    <section className="page-stack portfolio-page" aria-label="组合事实页面" data-state={state}>
      <header className="page-heading">
        <div><p className="eyebrow">PORTFOLIO / FROZEN FACTS</p><h1>组合事实</h1><p>复星真实持仓与内部模拟盘分账展示。未有合格 FX 证据时，不合并资产净值。</p></div>
        <div className="portfolio-page__actions"><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />刷新快照</button>{selected?.kind === "real_read_only" ? <button className="button button--primary" type="button" onClick={onSync} disabled={syncing}><ShieldCheck aria-hidden="true" size={17} />{syncing ? "正在确认同步任务" : "同步复星只读事实"}</button> : null}</div>
      </header>
      {notice ? <p className={`inline-notice inline-notice--${notice.kind}`}>{notice.text}</p> : null}
      {!hasData || !selected ? <DataState state={state} title="组合快照" detail={stateDetail(state)} announce={false} /> : (
        <div className="portfolio-workspace">
          <nav className="portfolio-rail" aria-label="组合选择"><ul>{portfolios.map((item) => <li key={item.portfolio_id}><button type="button" aria-pressed={item.portfolio_id === selectedPortfolioId} onClick={() => onSelect(item.portfolio_id)}><strong>{item.name}</strong><span>{kindLabel(item.kind)} · {item.coverage_scope}</span><span>{item.original_currencies.join(" / ") || "币种未知"}</span></button></li>)}</ul></nav>
          <div className="portfolio-detail">
            <div className="portfolio-snapshot-band"><div><p className="eyebrow">FROZEN SNAPSHOT</p><h2>{selected.name}</h2></div><dl><div><dt>状态</dt><dd>{snapshot.status}</dd></div><div><dt>覆盖范围</dt><dd>{snapshot.coverage_scope}</dd></div><div><dt>快照时点</dt><dd>{formatTime(snapshot.as_of)}</dd></div></dl></div>
            {snapshot.status === "RECONCILIATION_REQUIRED" ? <p className="inline-notice inline-notice--error">双读对账未通过。快照仅供展示，不能用作优化基线。</p> : null}
            {currencies.map((currency) => <PortfolioSummary key={currency} portfolio={selected} snapshot={snapshot} currency={currency} />)}
          </div>
        </div>
      )}
    </section>
  );
}

async function fetchWithTimeout(path: string): Promise<Response> { return fetch(path, { signal: AbortSignal.timeout(LOAD_TIMEOUT_MS) }); }
function snapshotState(status: PortfolioSnapshot["status"]): DataStateKind { return status === "READY" ? "ready" : status === "STALE" ? "stale" : "partial"; }
function kindLabel(kind: PortfolioItem["kind"]): string { return ({ real_read_only: "真实只读", internal_paper: "内部模拟", shadow: "影子组合" })[kind]; }
function formatTime(value: string | null): string { if (!value) return "Provider 时点缺失"; const date = new Date(value); return Number.isNaN(date.valueOf()) ? "无效时间" : date.toISOString().replace("T", " ").replace(".000Z", " UTC"); }
function syncErrorMessage(code: unknown): string { if (code === "IDEMPOTENCY_CONFLICT") return "幂等键已被不同请求使用；未创建新任务。"; if (code === "VALIDATION_ERROR") return "60 秒同步冷却中，或请求不合法。"; return "同步任务未登记。当前快照保持不变。"; }
function stateDetail(state: DataStateKind): string { return ({ loading: "正在读取脱敏组合和冻结快照。", empty: "尚无可展示的冻结组合快照。", ready: "组合快照已就绪。", partial: "快照仅供研究，不可作为优化基线。", stale: "显示最后已知快照，不触发业务动作。", error: "读取失败；未开启任何交易能力。", forbidden: "当前身份无权读取组合事实。" })[state]; }
