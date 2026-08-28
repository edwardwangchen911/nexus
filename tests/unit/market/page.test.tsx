import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import MarketPage, {
  buildMarketQuery,
  MarketPanel,
  refreshIntervalMs,
  type MarketItem,
  type SelectedMarketSeries,
} from "../../../app/(protected)/market/page";
import { PriceChart } from "../../../app/components/price-chart";
import type { IntradayMonitorData } from "../../../app/components/intraday-monitor";
import type { DataStateKind } from "../../../app/components/data-state";

const item: MarketItem = {
  instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781",
  symbol: "AAPL",
  price: "227.160000000000",
  source: "alpaca",
  feed: "iex",
  delivery_mode: "real_time",
  as_of: "2026-08-21T20:00:00Z",
  quality: "PASS",
  quality_reasons: [],
  watchlisted: true,
};

const selectedSeries: SelectedMarketSeries = {
  instrument_id: item.instrument_id,
  source: "alpaca",
  feed: "sip",
  delivery_mode: "delayed",
  adjustment: "raw",
  interval: "1Day",
  quality: "PASS",
  quality_reasons: [],
  bars: [
    {
      session: "2026-08-20",
      open: "224.000000000000",
      high: "229.000000000000",
      low: "223.500000000000",
      close: "228.100000000000",
      volume: "52100000.000000000000",
      available_at: "2026-08-21T00:01:00Z",
      observed_at: "2026-08-20T20:00:00Z",
      quality_flags: [],
    },
    {
      session: "2026-08-21",
      open: "228.200000000000",
      high: "229.400000000000",
      low: "225.900000000000",
      close: "227.160000000000",
      volume: "46300000.000000000000",
      available_at: "2026-08-22T00:01:00Z",
      observed_at: "2026-08-21T20:00:00Z",
      quality_flags: [],
    },
  ],
};

const msftItem: MarketItem = {
  ...item,
  instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e782",
  symbol: "MSFT",
  price: "418.790000000000",
  watchlisted: false,
};

const msftSeries: SelectedMarketSeries = {
  ...selectedSeries,
  instrument_id: msftItem.instrument_id,
  bars: selectedSeries.bars.map((bar) => ({ ...bar, close: "418.790000000000" })),
};

const intradayMonitor: IntradayMonitorData = {
  instrument_id: item.instrument_id,
  status: "ready",
  source: "alpaca",
  feed: "iex",
  delivery_mode: "real_time",
  bar_as_of: "2026-08-21T20:00:00Z",
  quote_as_of: "2026-08-21T20:00:01Z",
  trade_as_of: "2026-08-21T20:00:02Z",
  bar_age_seconds: 15,
  quote_age_seconds: 14,
  last: item.price,
  bid: "227.110000000000",
  ask: "227.210000000000",
  return_5m: "0.010000000000",
  return_30m: "-0.025000000000",
  range_30m: "0.031250000000",
  spread_bps: "4.402300000000",
  sample_count: 4,
  quality_reasons: [],
  bars: ["19:57", "19:58", "19:59", "20:00"].map((time, index) => ({
    observed_at: `2026-08-21T${time}:00Z`,
    available_at: `2026-08-21T${time}:00Z`,
    open: `${224 + index}.000000000000`,
    high: `${225 + index}.000000000000`,
    low: `${223 + index}.000000000000`,
    close: `${224 + index}.500000000000`,
    volume: `${1_000_000 + index}.000000000000`,
    quality_flags: [],
  })),
};

const msftIntradayMonitor: IntradayMonitorData = {
  ...intradayMonitor,
  instrument_id: msftItem.instrument_id,
  last: msftItem.price,
  bars: intradayMonitor.bars.map((bar) => ({ ...bar, close: msftItem.price! })),
};

const stateLabels: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无数据"],
  ["ready", "数据已就绪"],
  ["partial", "数据不完整"],
  ["stale", "数据已过期"],
  ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/market");
  Object.defineProperty(window, "scrollY", { configurable: true, value: 0 });
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    value: "visible",
  });
});

function marketEnvelope(overrides: {
  items?: MarketItem[];
  selectedSeries?: SelectedMarketSeries | null;
  intradayMonitor?: IntradayMonitorData | null;
  nextCursor?: string | null;
} = {}) {
  return {
    data: {
      items: overrides.items ?? [item],
      next_cursor: overrides.nextCursor ?? null,
      selected_series: overrides.selectedSeries === undefined ? selectedSeries : overrides.selectedSeries,
      intraday_monitor: overrides.intradayMonitor === undefined ? intradayMonitor : overrides.intradayMonitor,
    },
    meta: {
      request_id: "0198f7a0-1111-7777-8111-111111111115",
      schema_version: "1",
      as_of: "2026-08-22T00:00:00Z",
    },
  };
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

describe("market vertical slice", () => {
  it.each(stateLabels)("renders the visible %s data state", (state, label) => {
    render(
      <MarketPanel
        state={state}
        items={["ready", "partial", "stale"].includes(state) ? [item] : []}
        selectedSeries={["ready", "partial", "stale"].includes(state) ? selectedSeries : null}
        selectedInstrumentId={item.instrument_id}
        watchlistOnly={false}
        nextCursor={null}
        refreshing={false}
        pendingInstrumentId={null}
        notice={null}
        onRefresh={vi.fn()}
        onSelect={vi.fn()}
        onToggleWatchlist={vi.fn()}
        onToggleWatchlistOnly={vi.fn()}
        onNextPage={vi.fn()}
      />,
    );

    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "市场监控页面" })).toHaveAttribute(
      "data-state",
      state,
    );
  });

  it("separates quote evidence from OHLCV evidence and keeps watchlist writes explicit", () => {
    const onToggleWatchlist = vi.fn();
    const historySeries = {
      ...selectedSeries,
      source: "fosun",
      feed: "regular",
      delivery_mode: "real_time" as const,
    };
    render(
      <MarketPanel
        state="ready"
        items={[item]}
        selectedSeries={historySeries}
        selectedInstrumentId={item.instrument_id}
        watchlistOnly={false}
        nextCursor="opaque-next"
        refreshing={false}
        pendingInstrumentId={null}
        notice={null}
        onRefresh={vi.fn()}
        onSelect={vi.fn()}
        onToggleWatchlist={onToggleWatchlist}
        onToggleWatchlistOnly={vi.fn()}
        onNextPage={vi.fn()}
      />,
    );

    expect(screen.getByRole("region", { name: "AAPL 报价证据" })).toHaveTextContent(
      "ALPACA",
    );
    expect(screen.getByRole("region", { name: "AAPL 报价证据" })).toHaveTextContent(
      "IEX",
    );
    expect(screen.getByRole("region", { name: "AAPL 报价证据" })).toHaveTextContent(
      "实时",
    );
    expect(screen.getByRole("region", { name: "AAPL OHLCV 证据" })).toHaveTextContent("FOSUN");
    expect(screen.getByRole("region", { name: "AAPL OHLCV 证据" })).toHaveTextContent("实时");
    expect(screen.getByRole("region", { name: "AAPL OHLCV 证据" })).toHaveTextContent("2026-08-22 00:01:00 UTC");
    expect(screen.getByRole("region", { name: "AAPL 报价证据" })).toHaveTextContent("2026-08-21 20:00:00 UTC");
    expect(screen.getByRole("link", { name: "打开 AAPL 个股研究" })).toHaveAttribute(
      "href",
      `/research/${item.instrument_id}?as_of=${encodeURIComponent(item.as_of!)}`,
    );
    fireEvent.click(screen.getByRole("button", { name: "将 AAPL 移出观察列表" }));
    expect(onToggleWatchlist).toHaveBeenCalledWith(item);
  });

  it("binds the intraday monitor to the selected instrument and ignores mismatched evidence", async () => {
    window.history.replaceState(null, "", `/market?selected=${item.instrument_id}`);
    let mismatch = false;
    vi.stubGlobal("fetch", vi.fn(async () => jsonResponse(marketEnvelope({
      intradayMonitor: mismatch ? msftIntradayMonitor : intradayMonitor,
    }))));

    const view = render(<MarketPage />);
    expect(await screen.findByRole("region", { name: "AAPL 盘中证据" })).toHaveTextContent(
      "实时 IEX（仅 IEX）",
    );

    view.unmount();
    mismatch = true;
    render(<MarketPage />);
    await screen.findByRole("region", { name: "AAPL 报价证据" });
    expect(screen.queryByRole("region", { name: "AAPL 盘中证据" })).not.toBeInTheDocument();
  });

  it("shows a source conflict as a blocked evidence state without inventing a price", () => {
    const conflicted: MarketItem = {
      ...item,
      price: null,
      source: null,
      feed: null,
      delivery_mode: "unknown",
      quality: "FAIL",
      quality_reasons: ["SOURCE_CONFLICT"],
    };
    const onToggleWatchlist = vi.fn();

    render(
      <MarketPanel
        state="partial"
        items={[conflicted]}
        selectedSeries={null}
        selectedInstrumentId={item.instrument_id}
        watchlistOnly={false}
        nextCursor={null}
        refreshing={false}
        pendingInstrumentId={null}
        notice={null}
        onRefresh={vi.fn()}
        onSelect={vi.fn()}
        onToggleWatchlist={onToggleWatchlist}
        onToggleWatchlistOnly={vi.fn()}
        onNextPage={vi.fn()}
      />,
    );

    expect(screen.getAllByText("SOURCE_CONFLICT").length).toBeGreaterThan(0);
    expect(screen.getByText("无法选定可靠价格")).toBeVisible();
    expect(screen.queryByRole("region", { name: "AAPL 价格图表" })).not.toBeInTheDocument();
    expect(onToggleWatchlist).not.toHaveBeenCalled();
  });

  it("keeps stale data visible without causing a business action", () => {
    const onToggleWatchlist = vi.fn();
    render(
      <MarketPanel
        state="stale"
        items={[{ ...item, quality: "PARTIAL", quality_reasons: ["SELECTED_SOURCE_STALE"] }]}
        selectedSeries={{ ...selectedSeries, quality: "PARTIAL", quality_reasons: ["SELECTED_SOURCE_STALE"] }}
        selectedInstrumentId={item.instrument_id}
        watchlistOnly={false}
        nextCursor={null}
        refreshing={false}
        pendingInstrumentId={null}
        notice={null}
        onRefresh={vi.fn()}
        onSelect={vi.fn()}
        onToggleWatchlist={onToggleWatchlist}
        onToggleWatchlistOnly={vi.fn()}
        onNextPage={vi.fn()}
      />,
    );

    expect(screen.getByText(/显示最后已知行情/)).toBeVisible();
    expect(screen.getAllByText("SELECTED_SOURCE_STALE").length).toBeGreaterThan(0);
    expect(onToggleWatchlist).not.toHaveBeenCalled();
  });

  it("does not mark a fresh selected instrument stale because another row is stale", () => {
    const staleItem = {
      ...item,
      instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e782",
      symbol: "MSFT",
      quality: "PARTIAL" as const,
      quality_reasons: ["QUOTE_STALE"],
    };
    render(
      <MarketPanel
        state="stale"
        items={[item, staleItem]}
        selectedSeries={selectedSeries}
        selectedInstrumentId={item.instrument_id}
        watchlistOnly={false}
        nextCursor={null}
        refreshing={false}
        pendingInstrumentId={null}
        notice={null}
        onRefresh={vi.fn()}
        onSelect={vi.fn()}
        onToggleWatchlist={vi.fn()}
        onToggleWatchlistOnly={vi.fn()}
        onNextPage={vi.fn()}
      />,
    );

    expect(screen.queryByText(/显示最后已知行情/)).not.toBeInTheDocument();
  });

  it("renders the same OHLCV as a semantic table and keyboard point controls", () => {
    render(<PriceChart series={{
      symbol: item.symbol,
      source: selectedSeries.source!,
      feed: selectedSeries.feed!,
      interval: "1Day",
      bars: selectedSeries.bars,
    }} />);

    expect(screen.getByRole("region", { name: "AAPL 价格图表" })).toBeVisible();
    expect(screen.getByRole("table", { name: "AAPL OHLCV 数据" })).toHaveTextContent(
      "227.160000000000",
    );
    expect(screen.getByText(/共 2 个交易日/)).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "上一根 K 线" }));
    const activeDetail = screen.getByLabelText("当前 K 线详情");
    expect(activeDetail).toHaveTextContent("2026-08-20");
    expect(activeDetail).not.toHaveAttribute("role", "status");
    expect(activeDetail).not.toHaveAttribute("aria-live");
    expect(screen.getByRole("button", { name: "重置可见范围" })).toHaveClass("button");
  });

  it("refuses to label or chart a minute series as daily history", () => {
    render(
      <MarketPanel
        state="partial"
        items={[item]}
        selectedSeries={{ ...selectedSeries, interval: "1Min" } as unknown as SelectedMarketSeries}
        selectedInstrumentId={item.instrument_id}
        watchlistOnly={false}
        nextCursor={null}
        refreshing={false}
        pendingInstrumentId={null}
        notice={null}
        onRefresh={vi.fn()}
        onSelect={vi.fn()}
        onToggleWatchlist={vi.fn()}
        onToggleWatchlistOnly={vi.fn()}
        onNextPage={vi.fn()}
      />,
    );

    expect(screen.queryByRole("region", { name: "AAPL 价格图表" })).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "历史行情不可用" })).toBeVisible();
  });

  it("uses the approved visible and hidden refresh cadence", () => {
    expect(refreshIntervalMs("visible")).toBe(15_000);
    expect(refreshIntervalMs("hidden")).toBe(60_000);
  });

  it("omits the tri-state watchlist filter unless the UI explicitly asks for watched instruments", () => {
    expect(buildMarketQuery({ cursor: null, selectedInstrumentId: null, watchlistOnly: false }).toString()).toBe("limit=50");
    expect(buildMarketQuery({ cursor: null, selectedInstrumentId: null, watchlistOnly: true }).toString()).toBe("limit=50&watchlist=true");
  });

  it("falls back to the first returned instrument and retains filter and cursor in the URL", async () => {
    const missingId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799";
    window.history.replaceState(null, "", `/market?selected=${missingId}`);
    const queries: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), "https://nexus.test");
      queries.push(url.search);
      return jsonResponse(marketEnvelope({
        selectedSeries: url.searchParams.get("selected_instrument_id") === item.instrument_id
          ? selectedSeries
          : null,
        nextCursor: "opaque-next",
      }));
    }));

    render(<MarketPage />);

    expect(await screen.findByRole("region", { name: "AAPL 价格图表" })).toBeVisible();
    expect(window.location.search).toContain(`selected=${item.instrument_id}`);
    expect(queries.some((query) => query.includes(`selected_instrument_id=${missingId}`))).toBe(true);
    expect(queries.some((query) => query.includes(`selected_instrument_id=${item.instrument_id}`))).toBe(true);

    fireEvent.click(screen.getByRole("checkbox", { name: "只看观察列表" }));
    expect(window.location.search).toBe("?watchlist=true");
    fireEvent.click(await screen.findByRole("button", { name: "下一页" }));
    expect(window.location.search).toBe("?watchlist=true&cursor=opaque-next");
    await waitFor(() => {
      expect(window.location.search).toContain("watchlist=true");
      expect(window.location.search).toContain("cursor=opaque-next");
      expect(window.location.search).toContain(`selected=${item.instrument_id}`);
    });
  });

  it("restores and records the market list scroll position in history state", async () => {
    window.history.replaceState(
      { nexusMarketScrollY: 320 },
      "",
      `/market?selected=${item.instrument_id}`,
    );
    const scrollTo = vi.spyOn(window, "scrollTo").mockImplementation(() => undefined);
    vi.stubGlobal("fetch", vi.fn(async () => jsonResponse(marketEnvelope())));

    const view = render(<MarketPage />);
    await waitFor(() => expect(scrollTo).toHaveBeenCalledWith({
      top: 320,
      left: 0,
      behavior: "auto",
    }));
    Object.defineProperty(window, "scrollY", { configurable: true, value: 640 });
    fireEvent.scroll(window);

    expect(window.history.state).toMatchObject({ nexusMarketScrollY: 640 });
    expect(window.location.search).toContain(`selected=${item.instrument_id}`);
    view.unmount();
  });

  it("uses the server membership and prevents an older refresh from overwriting a watchlist write", async () => {
    window.history.replaceState(null, "", `/market?selected=${item.instrument_id}`);
    let resolveRefresh: ((response: Response) => void) | undefined;
    let marketCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      if (method === "DELETE") {
        return jsonResponse({
          data: { instrument_id: item.instrument_id, watchlisted: false },
          meta: marketEnvelope().meta,
        });
      }
      marketCalls += 1;
      if (marketCalls === 2) {
        return new Promise<Response>((resolve) => { resolveRefresh = resolve; });
      }
      return jsonResponse(marketEnvelope());
    }));

    render(<MarketPage />);
    expect(await screen.findByRole("button", { name: "将 AAPL 移出观察列表" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "刷新行情" }));
    await waitFor(() => expect(resolveRefresh).toBeDefined());
    fireEvent.click(screen.getByRole("button", { name: "将 AAPL 移出观察列表" }));
    expect(await screen.findByRole("button", { name: "将 AAPL 加入观察列表" })).toBeVisible();

    await act(async () => {
      resolveRefresh?.(jsonResponse(marketEnvelope()));
      await Promise.resolve();
    });
    expect(screen.getByRole("button", { name: "将 AAPL 加入观察列表" })).toBeVisible();
  });

  it("does not let a pending watchlist mutation restore an obsolete filter and selection", async () => {
    window.history.replaceState(
      null,
      "",
      `/market?watchlist=true&selected=${item.instrument_id}`,
    );
    let resolveMutation: ((response: Response) => void) | undefined;
    const marketQueries: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if ((init?.method ?? "GET") === "DELETE") {
        return new Promise<Response>((resolve) => { resolveMutation = resolve; });
      }
      const url = new URL(String(input), "https://nexus.test");
      marketQueries.push(url.search);
      if (url.searchParams.get("watchlist") === "true") {
        return jsonResponse(marketEnvelope({ items: [item], selectedSeries }));
      }
      return jsonResponse(marketEnvelope({ items: [msftItem], selectedSeries: msftSeries }));
    }));

    render(<MarketPage />);
    expect(await screen.findByRole("button", { name: "将 AAPL 移出观察列表" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "将 AAPL 移出观察列表" }));
    await waitFor(() => expect(resolveMutation).toBeDefined());
    fireEvent.click(screen.getByRole("checkbox", { name: "只看观察列表" }));
    expect(await screen.findByRole("region", { name: "MSFT 价格图表" })).toBeVisible();

    await act(async () => {
      resolveMutation?.(jsonResponse({
        data: { instrument_id: item.instrument_id, watchlisted: false },
        meta: marketEnvelope().meta,
      }));
      await Promise.resolve();
    });

    expect(window.location.search).toBe(`?selected=${msftItem.instrument_id}`);
    expect(screen.getByRole("region", { name: "MSFT 价格图表" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "AAPL 价格图表" })).not.toBeInTheDocument();
    expect(marketQueries.filter((query) => query.includes("watchlist=true"))).toHaveLength(1);
  });

  it("does not show an obsolete mutation error after the user changes market context", async () => {
    window.history.replaceState(
      null,
      "",
      `/market?watchlist=true&selected=${item.instrument_id}`,
    );
    let rejectMutation: ((reason?: unknown) => void) | undefined;
    vi.stubGlobal("fetch", vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      if ((init?.method ?? "GET") === "DELETE") {
        return new Promise<Response>((_resolve, reject) => { rejectMutation = reject; });
      }
      return jsonResponse(marketEnvelope());
    }));

    render(<MarketPage />);
    expect(await screen.findByRole("button", { name: "将 AAPL 移出观察列表" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "将 AAPL 移出观察列表" }));
    await waitFor(() => expect(rejectMutation).toBeDefined());
    fireEvent.click(screen.getByRole("checkbox", { name: "只看观察列表" }));

    await act(async () => {
      rejectMutation?.(new Error("old mutation failed"));
      await Promise.resolve();
    });

    expect(screen.queryByText("观察列表未更新。当前行情和交易状态均未改变，请重试。")).not.toBeInTheDocument();
    expect(window.location.search).not.toContain("watchlist=true");
  });

  it("keeps a newer selected instrument when two market responses resolve out of order", async () => {
    window.history.replaceState(null, "", `/market?selected=${item.instrument_id}`);
    let resolveOldAapl: ((response: Response) => void) | undefined;
    let resolveNewMsft: ((response: Response) => void) | undefined;
    let marketCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), "https://nexus.test");
      marketCalls += 1;
      if (marketCalls === 1) {
        return jsonResponse(marketEnvelope({ items: [item, msftItem], selectedSeries }));
      }
      if (url.searchParams.get("selected_instrument_id") === msftItem.instrument_id) {
        return new Promise<Response>((resolve) => { resolveNewMsft = resolve; });
      }
      return new Promise<Response>((resolve) => { resolveOldAapl = resolve; });
    }));

    render(<MarketPage />);
    expect(await screen.findByRole("region", { name: "AAPL 价格图表" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "刷新行情" }));
    await waitFor(() => expect(resolveOldAapl).toBeDefined());
    fireEvent.click(screen.getByRole("button", { name: /MSFT/ }));
    await waitFor(() => expect(resolveNewMsft).toBeDefined());

    await act(async () => {
      resolveNewMsft?.(jsonResponse(marketEnvelope({
        items: [item, msftItem],
        selectedSeries: msftSeries,
        intradayMonitor: msftIntradayMonitor,
      })));
      await Promise.resolve();
    });
    expect(await screen.findByRole("region", { name: "MSFT 价格图表" })).toBeVisible();
    expect(screen.getByRole("region", { name: "MSFT 盘中证据" })).toBeVisible();

    await act(async () => {
      resolveOldAapl?.(jsonResponse(marketEnvelope({
        items: [item, msftItem],
        selectedSeries,
        intradayMonitor,
      })));
      await Promise.resolve();
    });
    expect(window.location.search).toBe(`?selected=${msftItem.instrument_id}`);
    expect(screen.getByRole("region", { name: "MSFT 价格图表" })).toBeVisible();
    expect(screen.getByRole("region", { name: "MSFT 盘中证据" })).toBeVisible();
    expect(screen.queryByRole("region", { name: "AAPL 价格图表" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "AAPL 盘中证据" })).not.toBeInTheDocument();
  });

  it("polls stale market data at 15 seconds without issuing business writes", async () => {
    vi.useFakeTimers();
    window.history.replaceState(null, "", `/market?selected=${item.instrument_id}`);
    const methods: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      methods.push(init?.method ?? "GET");
      return jsonResponse(marketEnvelope({
        items: [{ ...item, quality: "PARTIAL", quality_reasons: ["QUOTE_STALE"] }],
        selectedSeries: { ...selectedSeries, quality: "PARTIAL", quality_reasons: ["SELECTED_SOURCE_STALE"] },
      }));
    }));

    const view = render(<MarketPage />);
    await act(async () => { await vi.advanceTimersByTimeAsync(46_000); });

    expect(methods.length).toBeGreaterThanOrEqual(4);
    expect(methods.every((method) => method === "GET")).toBe(true);
    expect(screen.getByText(/显示最后已知行情/)).toBeVisible();
    view.unmount();
    const callsAfterUnmount = methods.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
    expect(methods).toHaveLength(callsAfterUnmount);
  });

  it("reschedules actual polling from 15 to 60 seconds on visibility changes", async () => {
    vi.useFakeTimers();
    window.history.replaceState(null, "", `/market?selected=${item.instrument_id}`);
    let visibility: DocumentVisibilityState = "visible";
    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      get: () => visibility,
    });
    const fetchMock = vi.fn(async () => jsonResponse(marketEnvelope()));
    vi.stubGlobal("fetch", fetchMock);

    const view = render(<MarketPage />);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(15_000); });
    expect(fetchMock).toHaveBeenCalledTimes(2);

    visibility = "hidden";
    fireEvent(document, new Event("visibilitychange"));
    await act(async () => { await vi.advanceTimersByTimeAsync(59_999); });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    view.unmount();
  });

  it("does not duplicate polling when visibility changes during an in-flight refresh", async () => {
    vi.useFakeTimers();
    window.history.replaceState(null, "", `/market?selected=${item.instrument_id}`);
    let visibility: DocumentVisibilityState = "visible";
    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      get: () => visibility,
    });
    let resolveInFlight: ((response: Response) => void) | undefined;
    const fetchMock = vi.fn(async () => {
      if (fetchMock.mock.calls.length === 2) {
        return new Promise<Response>((resolve) => { resolveInFlight = resolve; });
      }
      return jsonResponse(marketEnvelope());
    });
    vi.stubGlobal("fetch", fetchMock);

    const view = render(<MarketPage />);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    act(() => { vi.advanceTimersByTime(15_000); });
    await act(async () => { await Promise.resolve(); });
    expect(resolveInFlight).toBeDefined();

    visibility = "hidden";
    fireEvent(document, new Event("visibilitychange"));
    await act(async () => {
      resolveInFlight?.(jsonResponse(marketEnvelope()));
      await Promise.resolve();
      await vi.advanceTimersByTimeAsync(59_999);
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
    expect(fetchMock).toHaveBeenCalledTimes(4);
    view.unmount();
  });
});
