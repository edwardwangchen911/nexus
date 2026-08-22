"use client";

import { DatabaseZap, RefreshCw, Star, StarOff, TriangleAlert } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { PriceChart } from "../../components/price-chart";
import type { components } from "../../lib/generated/schema";

export type MarketItem = components["schemas"]["MarketItemResponse"];
export type SelectedMarketSeries = components["schemas"]["SelectedMarketSeriesResponse"];
export type MarketQuality = MarketItem["quality"];
type MarketData = components["schemas"]["MarketPageResponse"];
type MarketEnvelope = components["schemas"]["MarketPageEnvelope"];

type Notice = { kind: "error" | "success"; text: string } | null;

type MarketPanelProps = {
  state: DataStateKind;
  items: MarketItem[];
  selectedSeries: SelectedMarketSeries | null;
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
  const [state, setState] = useState<DataStateKind>("loading");
  const [items, setItems] = useState<MarketItem[]>([]);
  const [selectedSeries, setSelectedSeries] = useState<SelectedMarketSeries | null>(null);
  const [selectedInstrumentId, setSelectedInstrumentId] = useState<string | null>(() =>
    typeof window === "undefined" ? null : new URLSearchParams(window.location.search).get("selected"),
  );
  const [watchlistOnly, setWatchlistOnly] = useState(false);
  const [cursor, setCursor] = useState<string | null>(null);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [pendingInstrumentId, setPendingInstrumentId] = useState<string | null>(null);
  const [notice, setNotice] = useState<Notice>(null);
  const itemsRef = useRef<MarketItem[]>([]);
  const refreshVersionRef = useRef(0);

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
        itemsRef.current = [];
        setItems([]);
        setSelectedSeries(null);
        return;
      }
      const data = result.envelope.data;
      itemsRef.current = data.items;
      setItems(data.items);
      setSelectedSeries(data.selected_series);
      setNextCursor(data.next_cursor);
      setState(classifyMarketState(data));
      if (!selectedInstrumentId && data.items[0]) {
        setSelectedInstrumentId(data.items[0].instrument_id);
        replaceSelectedQuery(data.items[0].instrument_id);
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
    const schedule = () => {
      if (!active) return;
      timer = globalThis.setTimeout(async () => {
        await refresh();
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
      if (timer) globalThis.clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [refresh]);

  function setSelected(instrumentId: string): void {
    setSelectedInstrumentId(instrumentId);
    replaceSelectedQuery(instrumentId);
  }

  async function toggleWatchlist(item: MarketItem): Promise<void> {
    setPendingInstrumentId(item.instrument_id);
    setNotice(null);
    try {
      const response = await fetch(`/api/v1/watchlist/${encodeURIComponent(item.instrument_id)}`, {
        method: item.watchlisted ? "DELETE" : "PUT",
      });
      if (!response.ok) throw new Error("watchlist mutation failed");
      if (watchlistOnly && item.watchlisted) {
        await refresh();
      } else {
        setItems((current) => {
          const updated = current.map((candidate) => candidate.instrument_id === item.instrument_id
            ? { ...candidate, watchlisted: !item.watchlisted }
            : candidate);
          itemsRef.current = updated;
          return updated;
        });
      }
      setNotice({ kind: "success", text: item.watchlisted ? `${item.symbol} 已移出观察列表。` : `${item.symbol} 已加入观察列表。` });
    } catch {
      setNotice({ kind: "error", text: "观察列表未更新。当前行情和交易状态均未改变，请重试。" });
    } finally {
      setPendingInstrumentId(null);
    }
  }

  return (
    <MarketPanel
      state={state}
      items={items}
      selectedSeries={selectedSeries}
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
        setCursor(null);
        setWatchlistOnly((current) => !current);
      }}
      onNextPage={() => { if (nextCursor) setCursor(nextCursor); }}
    />
  );
}

export function MarketPanel({
  state,
  items,
  selectedSeries,
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
  const hasData = ["ready", "partial", "stale"].includes(state) && items.length > 0;

  return (
    <section className="page-stack market-page" aria-label="市场监控页面" data-state={state}>
      <header className="page-heading">
        <div>
          <p className="eyebrow">MARKET / SOURCE-AWARE</p>
          <h1>市场监控</h1>
          <p>价格是指示性证据，不是交易触发器。每个值都保留来源、feed、截止时间与质量原因。</p>
        </div>
        <div className="market-page__heading-actions">
          <span className="state-chip" data-state={state}>{dataStateLabels[state]}</span>
          <button className="button button--quiet" type="button" onClick={onRefresh} disabled={refreshing}>
            <RefreshCw aria-hidden="true" size={17} />
            {refreshing ? "正在刷新行情" : "刷新行情"}
          </button>
        </div>
      </header>

      {!hasData ? (
        <DataState state={state} title={stateTitle(state)} detail={stateDetail(state)} />
      ) : (
        <div className="market-workspace">
          <aside className="market-rail" aria-label="股票池与观察列表">
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
                    <span className="numeric-data">{marketItem.price ?? "无法选定可靠价格"}</span>
                    <span>{sourceLabel(marketItem.source, marketItem.feed)}</span>
                    <span>{marketItem.quality}</span>
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
                <section className="market-evidence-band" aria-label={`${selectedItem.symbol} 来源完整性`}>
                  <div>
                    <p className="eyebrow">SELECTED SOURCE</p>
                    <h2>{selectedItem.symbol}</h2>
                  </div>
                  <dl>
                    <EvidenceFact term="来源" value={selectedSeriesForItem?.source?.toUpperCase() ?? selectedItem.source?.toUpperCase() ?? "未选定"} />
                    <EvidenceFact term="Feed" value={selectedSeriesForItem?.feed?.toUpperCase() ?? selectedItem.feed?.toUpperCase() ?? "未选定"} />
                    <EvidenceFact term="截止时间" value={formatUtc(selectedItem.as_of)} />
                    <EvidenceFact term="质量" value={selectedSeriesForItem?.quality ?? selectedItem.quality} />
                  </dl>
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
                </section>

                {qualityReasons(selectedSeriesForItem, selectedItem).length > 0 ? (
                  <div className="market-reasons" aria-label={`${selectedItem.symbol} 质量原因`}>
                    <TriangleAlert aria-hidden="true" size={18} />
                    <ul>{qualityReasons(selectedSeriesForItem, selectedItem).map((reason) => <li key={reason}>{reason}</li>)}</ul>
                  </div>
                ) : null}

                {state === "stale" ? (
                  <p className="inline-notice">显示最后已知行情；截止时间和来源保持可见，不触发任何业务动作。</p>
                ) : null}

                {selectedSeriesForItem?.source && selectedSeriesForItem.feed && selectedSeriesForItem.quality !== "FAIL" && selectedSeriesForItem.bars.length > 0 ? (
                  <PriceChart key={selectedItem.instrument_id} series={{
                    symbol: selectedItem.symbol,
                    source: selectedSeriesForItem.source,
                    feed: selectedSeriesForItem.feed,
                    bars: selectedSeriesForItem.bars,
                  }} />
                ) : (
                  <section className="market-history-empty" aria-label="历史行情不可用">
                    <DatabaseZap aria-hidden="true" size={20} />
                    <div>
                      <h2>历史行情不可用</h2>
                      <p>没有通过来源策略的 OHLCV；系统不会用最新报价合成 K 线。</p>
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
  if (reasons.some((reason) => reason === "SELECTED_SOURCE_STALE" || reason === "QUOTE_STALE")) return "stale";
  if (!data.selected_series || data.items.some((item) => item.quality !== "PASS") || data.selected_series.quality !== "PASS") return "partial";
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

function sourceLabel(source: string | null, feed: string | null): string {
  return source && feed ? `${source.toUpperCase()} · ${feed.toUpperCase()}` : "来源未选定";
}

function replaceSelectedQuery(instrumentId: string): void {
  const url = new URL(globalThis.location.href);
  url.searchParams.set("selected", instrumentId);
  globalThis.history.replaceState(null, "", url);
}

function formatUtc(value: string | null): string {
  if (!value) return "无";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return "无效时间";
  return parsed.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC");
}

function EvidenceFact({ term, value }: { term: string; value: string }): React.ReactElement {
  return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>;
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
