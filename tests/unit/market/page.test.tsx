import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildMarketQuery,
  MarketPanel,
  refreshIntervalMs,
  type MarketItem,
  type SelectedMarketSeries,
} from "../../../app/(protected)/market/page";
import { PriceChart } from "../../../app/components/price-chart";
import type { DataStateKind } from "../../../app/components/data-state";

const item: MarketItem = {
  instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781",
  symbol: "AAPL",
  price: "227.160000000000",
  source: "alpaca",
  feed: "sip-delayed",
  as_of: "2026-08-21T20:00:00Z",
  quality: "PASS",
  quality_reasons: [],
  watchlisted: true,
};

const selectedSeries: SelectedMarketSeries = {
  instrument_id: item.instrument_id,
  source: "alpaca",
  feed: "sip-delayed",
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
  vi.restoreAllMocks();
});

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

  it("makes source integrity first-class and keeps watchlist writes explicit", () => {
    const onToggleWatchlist = vi.fn();
    render(
      <MarketPanel
        state="ready"
        items={[item]}
        selectedSeries={selectedSeries}
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

    expect(screen.getByRole("region", { name: "AAPL 来源完整性" })).toHaveTextContent(
      "ALPACA",
    );
    expect(screen.getByRole("region", { name: "AAPL 来源完整性" })).toHaveTextContent(
      "SIP-DELAYED",
    );
    expect(screen.getByText("2026-08-21 20:00:00 UTC")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "将 AAPL 移出观察列表" }));
    expect(onToggleWatchlist).toHaveBeenCalledWith(item);
  });

  it("shows a source conflict as a blocked evidence state without inventing a price", () => {
    const conflicted: MarketItem = {
      ...item,
      price: null,
      source: null,
      feed: null,
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

  it("renders the same OHLCV as a semantic table and keyboard point controls", () => {
    render(<PriceChart series={{
      symbol: item.symbol,
      source: selectedSeries.source!,
      feed: selectedSeries.feed!,
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

  it("uses the approved visible and hidden refresh cadence", () => {
    expect(refreshIntervalMs("visible")).toBe(15_000);
    expect(refreshIntervalMs("hidden")).toBe(60_000);
  });

  it("omits the tri-state watchlist filter unless the UI explicitly asks for watched instruments", () => {
    expect(buildMarketQuery({ cursor: null, selectedInstrumentId: null, watchlistOnly: false }).toString()).toBe("limit=50");
    expect(buildMarketQuery({ cursor: null, selectedInstrumentId: null, watchlistOnly: true }).toString()).toBe("limit=50&watchlist=true");
  });
});
