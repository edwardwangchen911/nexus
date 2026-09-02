"use client";

import { ChevronDown, RefreshCw, TerminalSquare } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { AuthorityLedger } from "../../components/authority-ledger";
import { IntelligenceHeader } from "../../components/intelligence-header";
import { PortfolioDiagnostic } from "../../components/portfolio-diagnostic";
import { PortfolioSummary, PortfolioValuationSummary } from "../../components/portfolio-summary";
import type { components } from "../../lib/generated/schema";

export type PortfolioItem = components["schemas"]["PortfolioListItemResponse"];
export type PortfolioSnapshot = components["schemas"]["PortfolioSnapshotResponse"];

type Notice = { kind: "error" | "success"; text: string } | null;
type PortfolioPanelProps = {
  state: DataStateKind;
  portfolios: PortfolioItem[];
  snapshot: PortfolioSnapshot | null;
  selectedPortfolioId: string | null;
  notice: Notice;
  onSelect: (portfolioId: string) => void;
  onRefresh: () => void;
};

const LOAD_TIMEOUT_MS = 10_000;
const MAX_PORTFOLIO_PAGES = 10;
const MAX_PORTFOLIOS_PER_PAGE = 50;
const MAX_PORTFOLIOS = MAX_PORTFOLIO_PAGES * MAX_PORTFOLIOS_PER_PAGE;

export default function PortfolioPage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [portfolios, setPortfolios] = useState<PortfolioItem[]>([]);
  const [snapshot, setSnapshot] = useState<PortfolioSnapshot | null>(null);
  const [selectedPortfolioId, setSelectedPortfolioId] = useState<string | null>(null);
  const [notice, setNotice] = useState<Notice>(null);
  const [diagnosticVersion, setDiagnosticVersion] = useState(0);
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
    if (version !== loadVersion.current) return;
    if (envelope.data.portfolio_id !== portfolioId) throw new Error("snapshot portfolio mismatch");
    snapshotRef.current = envelope.data; setSnapshot(envelope.data);
    setDiagnosticVersion((current) => current + 1);
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
        if (envelope.data.items.length > MAX_PORTFOLIOS_PER_PAGE || items.length + envelope.data.items.length > MAX_PORTFOLIOS) throw new Error("portfolio item bound exceeded");
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

  return <><PortfolioPanel state={state} portfolios={portfolios} snapshot={snapshot}
    selectedPortfolioId={selectedPortfolioId} notice={notice}
    onSelect={(id) => void selectPortfolio(id)} onRefresh={() => void refresh()} />
    {snapshot && ["ready", "partial", "stale"].includes(state) ? <PortfolioDiagnostic
      key={`${snapshot.portfolio_id}:${snapshot.portfolio_snapshot_id}:${diagnosticVersion}`}
      portfolioId={snapshot.portfolio_id} snapshotId={snapshot.portfolio_snapshot_id}
    /> : null}</>;
}

export function PortfolioPanel({ state, portfolios, snapshot, selectedPortfolioId, notice, onSelect, onRefresh }: PortfolioPanelProps): React.ReactElement {
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
      <IntelligenceHeader
        eyebrow="资产与持仓"
        title="组合事实"
        description="复星真实持仓与内部模拟盘分账展示。缺少合格汇率证据时，各币种独立呈现，不虚构合并净值。"
        marker="ASSET LEDGER"
        actions={<><span className="state-chip" data-state={state}>快照展示：{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />刷新页面数据</button></>}
      />
      <div role="status" aria-label="组合操作状态" aria-live="polite" aria-atomic="true">
        {notice ? <p className={`inline-notice inline-notice--${notice.kind}`}>{notice.text}</p> : null}
      </div>
      {!selected ? <DataState state={state} title="组合快照" detail={stateDetail(state)} announce={false} /> : (
        <div className="portfolio-workspace" data-single={portfolios.length === 1}>
          <nav className="portfolio-rail" aria-label="组合选择"><p className="portfolio-rail__label">组合账户</p><ul>{portfolios.map((item) => <li key={item.portfolio_id}><button type="button" aria-pressed={item.portfolio_id === selectedPortfolioId} onClick={() => onSelect(item.portfolio_id)}><strong>{item.name}</strong><span>{kindLabel(item.kind)} · {coverageLabel(item.coverage_scope)}</span><span>{item.original_currencies.join(" / ") || "币种未知"}</span></button></li>)}</ul></nav>
          <div className="portfolio-detail">
            {!hasData || !snapshot ? <DataState state={state} title={`${selected.name} 快照`} detail={stateDetail(state)} announce={false} /> : <>
              {state === "stale" ? <p className="inline-notice inline-notice--error" role="status">{stateDetail(state)}</p> : null}
              {snapshot.decision_readiness === "RECONCILIATION_PENDING" ? <p className="inline-notice">资产事实可展示；订单或现金流尚未完成对账，不能作为优化基线。</p> : null}
              {snapshot.status === "RECONCILIATION_REQUIRED" ? <p className="inline-notice inline-notice--error">双读对账未通过。快照仅供展示，不能用作优化基线。</p> : null}
              <div className="portfolio-assets" aria-label="原币种资产总览">
                {currencies.map((currency) => <PortfolioValuationSummary key={currency} provider={selected.provider} snapshot={snapshot} currency={currency} compact />)}
              </div>
              <AuthorityLedger snapshotStatus={snapshot.status} displayReadiness={snapshot.display_readiness} decisionReadiness={snapshot.decision_readiness} />
              <details className="portfolio-disclosure portfolio-source">
                <summary>快照来源与覆盖范围 · {selected.provider === "fosun" ? "复星（只读）" : selected.provider} · {formatTime(snapshot.as_of)}</summary>
              <div className="portfolio-snapshot-band"><div><p className="eyebrow">冻结快照</p><h2>{selected.name}</h2><p>只读账户事实，不含跨币种推算。</p></div><dl><div><dt>数据来源</dt><dd>{selected.provider === "fosun" ? "复星（只读）" : selected.provider}</dd></div><div><dt>现金流对账</dt><dd>{cashFlowQualityLabel(snapshot.cash_flow_quality)} · {snapshot.cash_flow_count} 条</dd></div><div><dt>执行能力</dt><dd>{snapshot.execution_locked ? "已锁定" : "异常"}</dd></div><div><dt>覆盖范围</dt><dd>{coverageLabel(snapshot.coverage_scope)}</dd></div><div><dt>币种</dt><dd>{currencies.length}</dd></div><div><dt>持仓</dt><dd>{snapshot.positions.length}</dd></div><div><dt>未结订单</dt><dd>{snapshot.open_orders.length}</dd></div><div><dt>{snapshot.time_basis === "gateway_observed" ? "读取时间" : "更新时间"}</dt><dd>{formatTime(snapshot.as_of)}</dd></div></dl></div>
              {snapshot.time_basis === "gateway_observed" ? <p className="inline-notice">此快照使用网关读取时间；券商未提供逐条更新时间。双读对账只证明读取窗口内一致，不代表逐笔成交回放。</p> : null}
              </details>
              {currencies.map((currency) => <PortfolioSummary key={currency} portfolio={selected} snapshot={snapshot} currency={currency} showValuation={false} />)}
            </>}
            {selected.kind === "real_read_only" ? <LocalRefreshGuidance status={selected.last_sync_run_status} /> : null}
          </div>
        </div>
      )}
    </section>
  );
}

function LocalRefreshGuidance({ status }: { status: PortfolioItem["last_sync_run_status"] }): React.ReactElement {
  return (
    <details className="portfolio-local-refresh" aria-label="真实刷新仅限本机终端" role="region" open={status === "ACTIVE" || undefined}>
      <summary>
        <span className="portfolio-local-refresh__icon"><TerminalSquare aria-hidden="true" size={17} /></span>
        <span><small>LOCAL READ-ONLY REFRESH</small><strong>数据刷新与来源</strong></span>
        <span className="portfolio-local-refresh__state"><span>最近本地刷新状态</span><strong>{status ?? "未登记"}</strong></span>
        <ChevronDown aria-hidden="true" size={17} />
      </summary>
      <div className="portfolio-local-refresh__body">
        <p>浏览器只重新读取已持久化数据，不会登记、执行或重试 Provider 同步。</p>
        <p>通过既有安全 launcher 加载本地配置，并确认 Gateway 处于 <code>local_read_only</code> 后，在仓库根目录运行：</p>
        <pre className="portfolio-local-refresh__command"><code>uv run --package nexus-engine nexus-fosun-local refresh</code></pre>
        {status === "ACTIVE" ? <p className="inline-notice inline-notice--error">存在历史 ACTIVE 登记；当前页面不会执行、轮询或自动重试。请保留记录并在本机核对 launcher。</p> : null}
      </div>
    </details>
  );
}
async function fetchWithTimeout(path: string): Promise<Response> { return fetch(path, { signal: AbortSignal.timeout(LOAD_TIMEOUT_MS) }); }
function snapshotState(status: PortfolioSnapshot["status"]): DataStateKind { return status === "READY" ? "ready" : status === "STALE" ? "stale" : "partial"; }
function kindLabel(kind: PortfolioItem["kind"]): string { return ({ real_read_only: "真实只读", internal_paper: "内部模拟", shadow: "影子组合" })[kind]; }
function coverageLabel(scope: string): string { return scope === "broker_account" ? "券商账户" : scope; }
function cashFlowQualityLabel(value: PortfolioSnapshot["cash_flow_quality"]): string { return ({ PASS: "已通过", PARTIAL: "不完整", NOT_RUN: "未运行" })[value]; }
function formatTime(value: string | null): string { if (!value) return "Provider 时点缺失"; const date = new Date(value); return Number.isNaN(date.valueOf()) ? "无效时间" : date.toISOString().replace("T", " ").replace(".000Z", " UTC"); }
function stateDetail(state: DataStateKind): string { return ({ loading: "正在读取脱敏组合和冻结快照。", empty: "尚无可展示的冻结组合快照。", ready: "组合快照已就绪。", partial: "资产事实可展示；订单或现金流尚未完成对账，不能作为优化基线。", stale: "显示最后已知快照，不触发业务动作。", error: "读取失败；未开启任何交易能力。", forbidden: "当前身份无权读取组合事实。" })[state]; }
