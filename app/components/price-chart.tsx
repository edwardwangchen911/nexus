"use client";

import { ChevronLeft, ChevronRight, RotateCcw } from "lucide-react";
import { useEffect, useId, useRef, useState } from "react";
import type { components } from "../lib/generated/schema";

export type PriceChartBar = components["schemas"]["MarketBarResponse"];

export type PriceChartSeries = {
  symbol: string;
  source: string;
  feed: string;
  bars: PriceChartBar[];
};

type ChartHandle = {
  fitContent: () => void;
};

export function PriceChart({ series }: { series: PriceChartSeries }): React.ReactElement {
  const containerRef = useRef<HTMLDivElement>(null);
  const tableScrollRef = useRef<HTMLDivElement>(null);
  const chartHandleRef = useRef<ChartHandle | null>(null);
  const activeDetailId = useId();
  const [activeSession, setActiveSession] = useState<string | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container || typeof ResizeObserver === "undefined") return;

    let disposed = false;
    let removeChart: () => void = () => undefined;
    let disconnectObserver: () => void = () => undefined;

    void import("lightweight-charts").then(({ CandlestickSeries, ColorType, createChart }) => {
      if (disposed) return;
      const styles = getComputedStyle(document.documentElement);
      const chart = createChart(container, {
        autoSize: true,
        height: 320,
        layout: {
          attributionLogo: false,
          background: {
            type: ColorType.Solid,
            color: token(styles, "--component-chart-background"),
          },
          textColor: token(styles, "--component-chart-text"),
        },
        grid: {
          vertLines: { color: token(styles, "--component-chart-grid") },
          horzLines: { color: token(styles, "--component-chart-grid") },
        },
        rightPriceScale: { borderColor: token(styles, "--component-chart-border") },
        timeScale: { borderColor: token(styles, "--component-chart-border") },
      });
      const candles = chart.addSeries(CandlestickSeries, {
        upColor: "transparent",
        downColor: token(styles, "--component-chart-down"),
        borderUpColor: token(styles, "--component-chart-up"),
        borderDownColor: token(styles, "--component-chart-down"),
        wickUpColor: token(styles, "--component-chart-up"),
        wickDownColor: token(styles, "--component-chart-down"),
      });
      candles.setData(
        series.bars.map((bar) => ({
          time: bar.session,
          open: Number(bar.open),
          high: Number(bar.high),
          low: Number(bar.low),
          close: Number(bar.close),
        })),
      );
      chart.timeScale().fitContent();
      chartHandleRef.current = { fitContent: () => chart.timeScale().fitContent() };

      const observer = new ResizeObserver(() => chart.timeScale().fitContent());
      observer.observe(container);
      disconnectObserver = () => observer.disconnect();
      removeChart = () => chart.remove();
    });

    return () => {
      disposed = true;
      disconnectObserver();
      removeChart();
      chartHandleRef.current = null;
    };
  }, [series.bars]);

  const selectedIndex = activeSession === null
    ? -1
    : series.bars.findIndex((bar) => bar.session === activeSession);
  const activeIndex = selectedIndex >= 0 ? selectedIndex : Math.max(0, series.bars.length - 1);
  const activeBar = series.bars[activeIndex];

  function moveActive(direction: -1 | 1): void {
    const nextIndex = Math.min(series.bars.length - 1, Math.max(0, activeIndex + direction));
    setActiveSession(series.bars[nextIndex]?.session ?? null);
  }

  function resetRange(): void {
    chartHandleRef.current?.fitContent();
    setActiveSession(null);
  }

  function scrollTable(direction: -1 | 1): void {
    const container = tableScrollRef.current;
    if (!container) return;
    container.scrollLeft += direction * Math.max(240, Math.floor(container.clientWidth * 0.8));
  }

  return (
    <section className="price-chart" aria-label={`${series.symbol} 价格图表`}>
      <header className="price-chart__heading">
        <div>
          <p className="eyebrow">OHLC / {series.source.toUpperCase()} · {series.feed.toUpperCase()}</p>
          <h2>{series.symbol} 日线</h2>
          <p className="price-chart__summary">
            共 {series.bars.length} 个交易日；图形为辅助视图，下方表格提供同口径精确值。
          </p>
        </div>
        <div className="price-chart__controls" aria-label="K 线键盘控制">
          <button
            className="button button--quiet"
            type="button"
            aria-label="上一根 K 线"
            aria-describedby={activeDetailId}
            onClick={() => moveActive(-1)}
            disabled={activeIndex <= 0}
          >
            <ChevronLeft aria-hidden="true" size={17} />
            上一根
          </button>
          <button
            className="button button--quiet"
            type="button"
            aria-label="下一根 K 线"
            aria-describedby={activeDetailId}
            onClick={() => moveActive(1)}
            disabled={activeIndex >= series.bars.length - 1}
          >
            下一根
            <ChevronRight aria-hidden="true" size={17} />
          </button>
          <button className="button button--quiet" type="button" aria-label="重置可见范围" aria-describedby={activeDetailId} onClick={resetRange}>
            <RotateCcw aria-hidden="true" size={17} />
            重置范围
          </button>
        </div>
      </header>

      {activeBar ? (
        <p id={activeDetailId} className="price-chart__active numeric-data" aria-label="当前 K 线详情">
          {activeBar.session} · 开 {activeBar.open} · 高 {activeBar.high} · 低 {activeBar.low} · 收 {activeBar.close} · 量 {activeBar.volume}
        </p>
      ) : null}

      <div className="price-chart__canvas" ref={containerRef} aria-hidden="true" />
      <p className="price-chart__attribution">
        图形由 <a href="https://www.tradingview.com/lightweight-charts/" rel="noreferrer">TradingView Lightweight Charts™</a> 提供。
      </p>

      <div ref={tableScrollRef} className="table-scroll" role="region" aria-label={`${series.symbol} OHLCV 表格，可横向滚动`}>
        <div className="table-scroll__controls" aria-label="OHLCV 表格横向滚动控制">
          <button className="button button--quiet" type="button" onClick={() => scrollTable(-1)}>向左浏览表格</button>
          <button className="button button--quiet" type="button" onClick={() => scrollTable(1)}>向右浏览表格</button>
        </div>
        <table className="market-table numeric-data" aria-label={`${series.symbol} OHLCV 数据`}>
          <caption>{series.symbol} 与图形完全一致的 OHLCV 数据</caption>
          <thead>
            <tr>
              <th scope="col">交易日</th>
              <th scope="col">开盘</th>
              <th scope="col">最高</th>
              <th scope="col">最低</th>
              <th scope="col">收盘</th>
              <th scope="col">成交量</th>
            </tr>
          </thead>
          <tbody>
            {series.bars.map((bar) => (
              <tr key={bar.session} data-active={bar.session === activeBar?.session ? "true" : "false"}>
                <th scope="row">{bar.session}</th>
                <td>{bar.open}</td>
                <td>{bar.high}</td>
                <td>{bar.low}</td>
                <td>{bar.close}</td>
                <td>{bar.volume}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function token(styles: CSSStyleDeclaration, name: string): string {
  const value = styles.getPropertyValue(name).trim();
  if (!value) throw new Error(`Missing chart design token: ${name}`);
  return value;
}
