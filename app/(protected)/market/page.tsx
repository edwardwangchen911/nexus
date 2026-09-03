"use client";

import { formatDisplayTime } from "../../lib/display-format";

import { formatDisplayDecimal } from "../../lib/display-decimal";
import { DatabaseZap, RefreshCw, Star, StarOff, TriangleAlert } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { IntradayMonitor, type IntradayMonitorData } from "../../components/intraday-monitor";
import { MarketQualityReasons } from "../../components/market-quality-reasons";
import { PriceChart } from "../../components/price-chart";
import type { components } from "../../lib/generated/schema";
import {
  isReceiptOnlyQuote,
  marketAdjustmentLabel,
  marketDeliveryModeLabel,
  marketSourceLabel,
} from "../../lib/market-evidence-labels";

export type MarketItem = components["schemas"]["MarketItemResponse"];
export type SelectedMarketSeries = components["schemas"]["SelectedMarketSeriesResponse"];
export type MarketQuality = MarketItem["quality"];
type MarketData = components["schemas"]["MarketPageResponse"];
type MarketEnvelope = components["schemas"]["MarketPageEnvelope"];
type WatchlistEnvelope = components["schemas"]["WatchlistMembershipEnvelope"];

type Notice = { kind: "error" | "success"; text: string } | null;

type MarketPanelProps = {
  state: DataStateKind;
  researchAsOf?: string | null;
  lastReadAt?: string | null;
  items: MarketItem[];
  selectedSeries: SelectedMarketSeries | null;
  intradayMonitor?: IntradayMonitorData | null;
  selectedInstrumentId: string | null;
  watchlistOnly: boolean;
  nextCursor: string | null;
  refreshing: boolean;
  pendingInstrumentId: string | null;
  notice: Notice;
  onRefresh: () => void;
  onSelect: (item: MarketItem) => void;
  onToggleWatchlist: (item: MarketItem) => void;
  onToggleWatchlistOnly: () => void;
  onNextPage: () => void;
};

const LOAD_TIMEOUT_MS = 10_000;

export default function MarketPage(): React.ReactElement {
  const initialLocation = useMemo(() => readMarketLocation(), []);
  const [state, setState] = useState<DataStateKind>("loading");
  const [researchAsOf, setResearchAsOf] = useState<string | null>(null);
  const [lastReadAt, setLastReadAt] = useState<string | null>(null);
  const [items, setItems] = useState<MarketItem[]>([]);
  const [selectedSeries, setSelectedSeries] = useState<SelectedMarketSeries | null>(null);
  const [intradayMonitor, setIntradayMonitor] = useState<IntradayMonitorData | null>(null);
  const [selectedInstrumentId, setSelectedInstrumentId] = useState<string | null>(initialLocation.selectedInstrumentId);
  const [watchlistOnly, setWatchlistOnly] = useState(initialLocation.watchlistOnly);
  const [cursor, setCursor] = useState<string | null>(initialLocation.cursor);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [pendingInstrumentId, setPendingInstrumentId] = useState<string | null>(null);
  const [notice, setNotice] = useState<Notice>(null);
  const itemsRef = useRef<MarketItem[]>([]);
  const refreshVersionRef = useRef(0);
  const locationVersionRef = useRef(0);
  const watchlistMutationVersionRef = useRef(0);
  const scrollRestoreRef = useRef<number | null | undefined>(undefined);

  useEffect(() => {
    scrollRestoreRef.current = readMarketScrollY();
    const saveScrollPosition = () => saveMarketScrollY(globalThis.scrollY);
    globalThis.addEventListener("scroll", saveScrollPosition, { passive: true });
    return () => {
      globalThis.removeEventListener("scroll", saveScrollPosition);
    };
  }, []);

  useEffect(() => {
    if (
      scrollRestoreRef.current === undefined
      || scrollRestoreRef.current === null
      || items.length === 0
      || !["ready", "partial", "stale"].includes(state)
    ) return;
    const savedScrollY = scrollRestoreRef.current;
    scrollRestoreRef.current = null;
    const restoreTimer = globalThis.setTimeout(() => {
      globalThis.scrollTo({ top: savedScrollY, left: 0, behavior: "auto" });
    }, 0);
    return () => globalThis.clearTimeout(restoreTimer);
  }, [items.length, state]);

  const refresh = useCallback(async () => {
    const refreshVersion = refreshVersionRef.current + 1;
    refreshVersionRef.current = refreshVersion;
    setRefreshing(true);
    setNotice(null);
    const previousItems = itemsRef.current;
    try {
      const result = await loadMarket({ cursor, selectedInstrumentId, watchlistOnly });
      if (refreshVersion !== refreshVersionRef.current) return;
      if (result.kind === "forbidden") {
        setState("forbidden");
        setLastReadAt(null);
        itemsRef.current = [];
        setItems([]);
        setSelectedSeries(null);
        setIntradayMonitor(null);
        return;
      }
      const data = result.envelope.data;
      setResearchAsOf(result.envelope.meta.as_of);
      const nextSelectedInstrumentId = selectedInstrumentId !== null
        && data.items.some((item) => item.instrument_id === selectedInstrumentId)
        ? selectedInstrumentId
        : data.items[0]?.instrument_id ?? null;
      itemsRef.current = data.items;
      setItems(data.items);
      setSelectedSeries(
        data.selected_series?.instrument_id === nextSelectedInstrumentId
          ? data.selected_series
          : null,
      );
      setIntradayMonitor(
        data.intraday_monitor?.instrument_id === nextSelectedInstrumentId
          ? data.intraday_monitor
          : null,
      );
      setNextCursor(data.next_cursor);
      setState(classifyMarketState(data));
      setLastReadAt(new Date().toISOString());
      if (nextSelectedInstrumentId !== selectedInstrumentId) {
        setSelectedInstrumentId(nextSelectedInstrumentId);
        replaceMarketQuery({
          selectedInstrumentId: nextSelectedInstrumentId,
          watchlistOnly,
          cursor,
        });
      }
    } catch (error) {
      if (refreshVersion !== refreshVersionRef.current) return;
      const timedOut = error instanceof DOMException && error.name === "AbortError";
      setState(previousItems.length > 0 ? "stale" : "error");
      setNotice({
        kind: "error",
        text: timedOut
          ? "行情刷新超时。保留最后已知值，不触发任何业务动作。"
          : "无法读取行情。保留安全默认值，请重试。",
      });
    } finally {
      if (refreshVersion === refreshVersionRef.current) setRefreshing(false);
    }
  }, [cursor, selectedInstrumentId, watchlistOnly]);

  useEffect(() => {
    const initialRefresh = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(initialRefresh);
  }, [refresh]);

  useEffect(() => {
    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let scheduleGeneration = 0;
    const schedule = () => {
      if (!active) return;
      const generation = scheduleGeneration + 1;
      scheduleGeneration = generation;
      timer = globalThis.setTimeout(async () => {
        await refresh();
        if (!active || generation !== scheduleGeneration) return;
        schedule();
      }, refreshIntervalMs(document.visibilityState));
    };
    schedule();
    const onVisibilityChange = () => {
      if (timer) globalThis.clearTimeout(timer);
      schedule();
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      active = false;
      scheduleGeneration += 1;
      if (timer) globalThis.clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [refresh]);

  function setSelected(instrumentId: string): void {
    refreshVersionRef.current += 1;
    locationVersionRef.current += 1;
    setSelectedInstrumentId(instrumentId);
    setSelectedSeries(null);
    setIntradayMonitor(null);
    replaceMarketQuery({ selectedInstrumentId: instrumentId, watchlistOnly, cursor });
  }

  async function toggleWatchlist(item: MarketItem): Promise<void> {
    refreshVersionRef.current += 1;
    const locationVersion = locationVersionRef.current;
    const mutationVersion = watchlistMutationVersionRef.current + 1;
    watchlistMutationVersionRef.current = mutationVersion;
    setRefreshing(false);
    setPendingInstrumentId(item.instrument_id);
    setNotice(null);
    try {
      const response = await fetch(`/api/v1/watchlist/${encodeURIComponent(item.instrument_id)}`, {
        method: item.watchlisted ? "DELETE" : "PUT",
      });
      if (!response.ok) throw new Error("watchlist mutation failed");
      const envelope = await response.json() as WatchlistEnvelope;
      if (envelope.data.instrument_id !== item.instrument_id) {
        throw new Error("watchlist membership mismatch");
      }
      if (
        mutationVersion !== watchlistMutationVersionRef.current
        || locationVersion !== locationVersionRef.current
      ) return;
      refreshVersionRef.current += 1;
      const finalWatchlisted = envelope.data.watchlisted;
      if (watchlistOnly && !finalWatchlisted) {
        await refresh();
      } else {
        setItems((current) => {
          const updated = current.map((candidate) => candidate.instrument_id === item.instrument_id
            ? { ...candidate, watchlisted: finalWatchlisted }
            : candidate);
          itemsRef.current = updated;
          return updated;
        });
      }
      if (
        mutationVersion === watchlistMutationVersionRef.current
        && locationVersion === locationVersionRef.current
      ) {
        setNotice({ kind: "success", text: finalWatchlisted ? `${item.symbol} 已加入观察列表。` : `${item.symbol} 已移出观察列表。` });
      }
    } catch {
      if (
        mutationVersion === watchlistMutationVersionRef.current
        && locationVersion === locationVersionRef.current
      ) {
        setNotice({ kind: "error", text: "观察列表未更新。当前行情和交易状态均未改变，请重试。" });
      }
    } finally {
      if (mutationVersion === watchlistMutationVersionRef.current) {
        setPendingInstrumentId(null);
      }
    }
  }

  return (
    <MarketPanel
      state={state}
      researchAsOf={researchAsOf}
      lastReadAt={lastReadAt}
      items={items}
      selectedSeries={selectedSeries}
      intradayMonitor={intradayMonitor}
      selectedInstrumentId={selectedInstrumentId}
      watchlistOnly={watchlistOnly}
      nextCursor={nextCursor}
      refreshing={refreshing}
      pendingInstrumentId={pendingInstrumentId}
      notice={notice}
      onRefresh={() => void refresh()}
      onSelect={(item) => setSelected(item.instrument_id)}
      onToggleWatchlist={(item) => void toggleWatchlist(item)}
      onToggleWatchlistOnly={() => {
        const nextWatchlistOnly = !watchlistOnly;
        refreshVersionRef.current += 1;
        locationVersionRef.current += 1;
        setCursor(null);
        setNextCursor(null);
        setSelectedInstrumentId(null);
        setSelectedSeries(null);
        setIntradayMonitor(null);
        setWatchlistOnly(nextWatchlistOnly);
        replaceMarketQuery({ selectedInstrumentId: null, watchlistOnly: nextWatchlistOnly, cursor: null });
      }}
      onNextPage={() => {
        if (!nextCursor) return;
        refreshVersionRef.current += 1;
        locationVersionRef.current += 1;
        setCursor(nextCursor);
        setSelectedInstrumentId(null);
        setSelectedSeries(null);
        setIntradayMonitor(null);
        replaceMarketQuery({ selectedInstrumentId: null, watchlistOnly, cursor: nextCursor });
      }}
    />
  );
}

export function MarketPanel({
  state,
  researchAsOf = null,
  lastReadAt = null,
  items,
  selectedSeries,
  intradayMonitor = null,
  selectedInstrumentId,
  watchlistOnly,
  nextCursor,
  refreshing,
  pendingInstrumentId,
  notice,
  onRefresh,
  onSelect,
  onToggleWatchlist,
  onToggleWatchlistOnly,
  onNextPage,
}: MarketPanelProps): React.ReactElement {
  const selectedItem = useMemo(
    () => selectedInstrumentId === null
      ? items[0] ?? null
      : items.find((candidate) => candidate.instrument_id === selectedInstrumentId) ?? null,
    [items, selectedInstrumentId],
  );
  const selectedSeriesForItem = selectedSeries?.instrument_id === selectedItem?.instrument_id
    ? selectedSeries
    : null;
  const intradayMonitorForItem = intradayMonitor?.instrument_id === selectedItem?.instrument_id
    ? intradayMonitor
    : null;
  const hasData = ["ready", "partial", "stale"].includes(state) && items.length > 0;
  const hasExplicitQuoteTimes = selectedItem !== null
    && (selectedItem.source_observed_at !== undefined || selectedItem.received_at !== undefined);

  return (
    <section className="page-stack market-page" aria-label="市场监控页面" data-state={state}>
      <header className="page-heading">
        <div>
          <p className="eyebrow">来源可追溯行情</p>
          <h1>市场监控</h1>
          <p>先看报价是否可靠，再决定是否继续研究。价格只作指示性证据，不会触发交易。</p>
        </div>
        <div className="market-page__heading-actions">
          <span className="state-chip" data-state={state}>{dataStateLabels[state]}</span>
          <button className="button button--quiet" type="button" onClick={onRefresh} disabled={refreshing}>
            <RefreshCw aria-hidden="true" size={17} />
            {refreshing ? "正在刷新行情" : "刷新行情"}
          </button>
        </div>
      </header>

      <p>
        <span aria-label="查询快照时点">查询快照时点 {formatUtc(researchAsOf)}</span>
        <br />
        <span aria-label="页面成功读取时间">页面成功读取时间 {formatUtc(lastReadAt)}</span>
        <br />
        <span>页面每 15 秒读取已保存数据，后台页每 60 秒；不代表券商每 15 秒采集，分页可能沿用同一快照。</span>
      </p>

      {!hasData ? (
        <DataState state={state} title={stateTitle(state)} detail={stateDetail(state)} />
      ) : (
        <div className="market-workspace">
          <aside className="market-rail" aria-label="股票池与观察列表">
            <div className="market-rail__heading"><p className="eyebrow">研究标的</p><span>{items.length} 只</span></div>
            <label className="market-filter">
              <input type="checkbox" checked={watchlistOnly} onChange={onToggleWatchlistOnly} />
              只看观察列表
            </label>
            <ul className="market-instrument-list">
              {items.map((marketItem) => (
                <li key={marketItem.instrument_id}>
                  <button
                    className="market-instrument"
                    type="button"
                    data-quality={marketItem.quality}
                    aria-pressed={marketItem.instrument_id === selectedItem?.instrument_id}
                    onClick={() => onSelect(marketItem)}
                  >
                    <span className="market-instrument__symbol">{marketItem.symbol}</span>
                    <span className="numeric-data">{marketItem.price === null ? "无法选定可靠价格" : formatDisplayDecimal(marketItem.price)}</span>
                    <span>{sourceLabel(marketItem.source, marketItem.feed)}</span>
                    <span>{marketQualityLabel(marketItem.quality)} <code>{marketItem.quality}</code></span>
                  </button>
                </li>
              ))}
            </ul>
            {nextCursor ? (
              <button className="button button--quiet" type="button" onClick={onNextPage}>下一页</button>
            ) : null}
          </aside>

          <div className="market-detail">
            {selectedItem ? (
              <>
                <section className="market-evidence-band" aria-label={`${selectedItem.symbol} 报价证据`}>
                  <div>
                    <p className="eyebrow">报价证据</p>
                    <h2>{selectedItem.symbol}</h2>
                  </div>
                  <dl>
                    <EvidenceFact term="最新价" value={selectedItem.price === null ? "无可验证报价" : formatDisplayDecimal(selectedItem.price)} highlight />
                    <EvidenceFact term="报价来源" value={marketSourceLabel(selectedItem.source, selectedItem.feed)} />
                    <EvidenceFact term="行情通道" value={selectedItem.feed?.toUpperCase() ?? "未选定"} />
                    {hasExplicitQuoteTimes ? <>
                      <EvidenceFact term="报价源时间" value={formatUtc(selectedItem.source_observed_at)} />
                      <EvidenceFact term={selectedItem.source === "fosun" ? "本机接收时间（网关响应）" : "本机接收时间"} value={formatUtc(selectedItem.received_at)} />
                    </> : <>
                      <EvidenceFact term={isReceiptOnlyQuote(selectedItem.source, selectedItem.quality_reasons) ? "网关接收时间" : "行情记录时间"} value={formatUtc(selectedItem.as_of)} />
                      {isReceiptOnlyQuote(selectedItem.source, selectedItem.quality_reasons) ? <EvidenceFact term="报价源时间" value="未知（上游未提供）" /> : null}
                    </>}
                    <EvidenceFact term="实时性口径" value={marketDeliveryModeLabel(selectedItem.delivery_mode)} />
                    <EvidenceFact term="报价质量" value={`${marketQualityLabel(selectedItem.quality)} · ${selectedItem.quality}`} />
                  </dl>
                  <div className="market-evidence-actions">
                    {selectedItem.as_of ? <a
                      className="button button--quiet"
                      aria-label={`打开 ${selectedItem.symbol} 个股研究`}
                      href={`/research/${selectedItem.instrument_id}?as_of=${encodeURIComponent(researchAsOf ?? selectedItem.as_of)}`}
                    >打开个股研究</a> : null}
                    <button
                      className="button button--quiet"
                      type="button"
                      aria-label={selectedItem.watchlisted ? `将 ${selectedItem.symbol} 移出观察列表` : `将 ${selectedItem.symbol} 加入观察列表`}
                      onClick={() => onToggleWatchlist(selectedItem)}
                      disabled={pendingInstrumentId === selectedItem.instrument_id}
                    >
                      {selectedItem.watchlisted ? <StarOff aria-hidden="true" size={17} /> : <Star aria-hidden="true" size={17} />}
                      {pendingInstrumentId === selectedItem.instrument_id ? "正在更新" : selectedItem.watchlisted ? "移出观察" : "加入观察"}
                    </button>
                  </div>
                </section>

                {qualityReasons(selectedSeriesForItem, selectedItem).length > 0 ? (
                  <div className="market-reasons" aria-label={`${selectedItem.symbol} 质量原因`}>
                    <TriangleAlert aria-hidden="true" size={18} />
                    <MarketQualityReasons reasons={qualityReasons(selectedSeriesForItem, selectedItem)} />
                  </div>
                ) : null}

                {isSelectedEvidenceStale(selectedSeriesForItem, selectedItem) ? (
                  <p className="inline-notice">显示最后已知行情；截止时间和来源保持可见，不触发任何业务动作。</p>
                ) : null}

                {intradayMonitorForItem ? (
                  <IntradayMonitor
                    key={`intraday:${selectedItem.instrument_id}`}
                    symbol={selectedItem.symbol}
                    monitor={intradayMonitorForItem}
                  />
                ) : null}

                {selectedSeriesForItem?.source && selectedSeriesForItem.feed && selectedSeriesForItem.interval === "1Day" && selectedSeriesForItem.quality !== "FAIL" && selectedSeriesForItem.bars.length > 0 ? (
                  <>
                    <HistoryEvidence symbol={selectedItem.symbol} series={selectedSeriesForItem} />
                    <PriceChart key={selectedItem.instrument_id} series={{
                      symbol: selectedItem.symbol,
                      source: selectedSeriesForItem.source,
                      feed: selectedSeriesForItem.feed,
                      adjustment: selectedSeriesForItem.adjustment,
                      interval: selectedSeriesForItem.interval,
                      bars: selectedSeriesForItem.bars,
                    }} />
                  </>
                ) : (
                  <section className="market-history-empty" aria-label="历史行情不可用">
                    <DatabaseZap aria-hidden="true" size={20} />
                    <div>
                      <h2>历史行情不可用</h2>
                      <p>暂无通过来源核验的历史 K 线数据；最新报价不会替代历史行情。</p>
                    </div>
                  </section>
                )}
              </>
            ) : null}
          </div>
        </div>
      )}

      {notice ? (
        <p className={`inline-notice ${notice.kind === "error" ? "inline-notice--error" : "inline-notice--success"}`} role="status" aria-live="polite" aria-atomic="true">
          {notice.text}
        </p>
      ) : null}
    </section>
  );
}

export function refreshIntervalMs(visibility: DocumentVisibilityState): number {
  return visibility === "hidden" ? 60_000 : 15_000;
}

function classifyMarketState(data: MarketData): DataStateKind {
  if (data.items.length === 0) return "empty";
  const reasons = [...data.items.flatMap((item) => item.quality_reasons), ...(data.selected_series?.quality_reasons ?? [])];
  if (
    reasons.some((reason) => reason === "SELECTED_SOURCE_STALE" || reason === "QUOTE_STALE")
    || data.intraday_monitor?.status === "stale"
  ) return "stale";
  if (
    !data.selected_series
    || data.items.some((item) => item.quality !== "PASS")
    || data.selected_series.quality !== "PASS"
    || data.intraday_monitor?.status === "partial"
    || data.intraday_monitor?.status === "unavailable"
  ) return "partial";
  return "ready";
}

async function loadMarket(input: { cursor: string | null; selectedInstrumentId: string | null; watchlistOnly: boolean }): Promise<{ kind: "ok"; envelope: MarketEnvelope } | { kind: "forbidden" }> {
  const params = buildMarketQuery(input);
  const controller = new AbortController();
  const timeout = globalThis.setTimeout(() => controller.abort(), LOAD_TIMEOUT_MS);
  try {
    const response = await fetch(`/api/v1/market?${params}`, { method: "GET", cache: "no-store", signal: controller.signal });
    if (response.status === 401 || response.status === 403) return { kind: "forbidden" };
    if (!response.ok) throw new Error("market unavailable");
    return { kind: "ok", envelope: await response.json() as MarketEnvelope };
  } finally {
    globalThis.clearTimeout(timeout);
  }
}

export function buildMarketQuery(input: { cursor: string | null; selectedInstrumentId: string | null; watchlistOnly: boolean }): URLSearchParams {
  const params = new URLSearchParams({ limit: "50" });
  if (input.watchlistOnly) params.set("watchlist", "true");
  if (input.cursor) params.set("cursor", input.cursor);
  if (input.selectedInstrumentId) params.set("selected_instrument_id", input.selectedInstrumentId);
  return params;
}

function qualityReasons(series: SelectedMarketSeries | null, item: MarketItem): string[] {
  return [...new Set([...(series?.quality_reasons ?? []), ...item.quality_reasons])];
}

function isSelectedEvidenceStale(series: SelectedMarketSeries | null, item: MarketItem): boolean {
  return qualityReasons(series, item).some((reason) =>
    reason === "SELECTED_SOURCE_STALE" || reason === "QUOTE_STALE",
  );
}

function sourceLabel(source: string | null, feed: string | null): string {
  return marketSourceLabel(source, feed);
}

type MarketLocationState = {
  selectedInstrumentId: string | null;
  watchlistOnly: boolean;
  cursor: string | null;
};

function readMarketLocation(): MarketLocationState {
  if (typeof window === "undefined") {
    return { selectedInstrumentId: null, watchlistOnly: false, cursor: null };
  }
  const params = new URLSearchParams(window.location.search);
  return {
    selectedInstrumentId: params.get("selected"),
    watchlistOnly: params.get("watchlist") === "true",
    cursor: params.get("cursor"),
  };
}

function replaceMarketQuery(state: MarketLocationState): void {
  const url = new URL(globalThis.location.href);
  for (const key of ["selected", "watchlist", "cursor"]) url.searchParams.delete(key);
  if (state.selectedInstrumentId) url.searchParams.set("selected", state.selectedInstrumentId);
  if (state.watchlistOnly) url.searchParams.set("watchlist", "true");
  if (state.cursor) url.searchParams.set("cursor", state.cursor);
  globalThis.history.replaceState(globalThis.history.state, "", url);
}

function readMarketScrollY(): number | null {
  const state: unknown = globalThis.history.state;
  if (typeof state !== "object" || state === null || !("nexusMarketScrollY" in state)) {
    return null;
  }
  const value = (state as { nexusMarketScrollY?: unknown }).nexusMarketScrollY;
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

function saveMarketScrollY(scrollY: number): void {
  const current: unknown = globalThis.history.state;
  const state = typeof current === "object" && current !== null
    ? { ...current, nexusMarketScrollY: scrollY }
    : { nexusMarketScrollY: scrollY };
  globalThis.history.replaceState(state, "", globalThis.location.href);
}

function formatUtc(value: string | null | undefined): string { return formatDisplayTime(value); }

function EvidenceFact({ term, value, highlight = false }: { term: string; value: string; highlight?: boolean }): React.ReactElement {
  return <div className={highlight ? "market-evidence-fact--primary" : undefined}><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>;
}

function HistoryEvidence({ symbol, series }: { symbol: string; series: SelectedMarketSeries }): React.ReactElement {
  const latestBar = series.bars.at(-1);
  return (
    <section className="market-evidence-band market-evidence-band--history" aria-label={`${symbol} OHLCV 证据`}>
      <div>
        <p className="eyebrow">历史序列证据</p>
        <h2>{symbol} 历史序列</h2>
      </div>
      <dl>
        <EvidenceFact term="序列来源" value={marketSourceLabel(series.source, series.feed)} />
        <EvidenceFact term="行情通道" value={series.feed?.toUpperCase() ?? "未选定"} />
        <EvidenceFact term="实时性口径" value={marketDeliveryModeLabel(series.delivery_mode)} />
        <EvidenceFact term="复权口径" value={marketAdjustmentLabel(series.adjustment)} />
        <EvidenceFact term="周期" value={series.interval === "1Day" ? "日线" : series.interval ?? "未选定"} />
        <EvidenceFact term="最新可用时间" value={formatUtc(latestBar?.available_at ?? null)} />
        <EvidenceFact term="最新观测时间" value={formatUtc(latestBar?.observed_at ?? null)} />
        <EvidenceFact term="序列质量" value={`${marketQualityLabel(series.quality)} · ${series.quality}`} />
      </dl>
      {series.adjustment === "raw" ? (
        <p className="market-adjustment-warning">
          <TriangleAlert aria-hidden="true" size={18} />
          未复权价格不可直接用于跨期收益、动量或回撤判断。
        </p>
      ) : null}
    </section>
  );
}

function stateTitle(state: DataStateKind): string {
  const titles: Record<DataStateKind, string> = {
    loading: "正在读取股票池",
    empty: "股票池为空",
    ready: "行情已就绪",
    partial: "部分行情不可用",
    stale: "行情已过期",
    error: "行情读取失败",
    forbidden: "无权访问市场监控",
  };
  return titles[state];
}

function marketQualityLabel(quality: MarketQuality): string {
  return quality === "PASS" ? "质量通过" : quality === "PARTIAL" ? "证据不完整" : "质量未通过";
}

function stateDetail(state: DataStateKind): string {
  const details: Record<DataStateKind, string> = {
    loading: "正在读取来源明确的指示性行情。",
    empty: "当前筛选条件下没有标的。",
    ready: "价格、来源和截止时间均已返回。",
    partial: "保留可验证字段；缺失或冲突部分不会被推断。",
    stale: "显示最后已知值与截止时间，不触发任何业务动作。",
    error: "没有可安全展示的行情，请重试。",
    forbidden: "当前身份不能读取市场数据。",
  };
  return details[state];
}
