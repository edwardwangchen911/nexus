"use client";

import { formatDisplayDecimal } from "../lib/display-decimal";
import { Activity, ChevronLeft, ChevronRight, Clock3, TriangleAlert } from "lucide-react";
import { useRef } from "react";

import type { components } from "../lib/generated/schema";
import {
  marketDeliveryModeLabel,
  marketSourceLabel,
} from "../lib/market-evidence-labels";
import { MarketQualityReasons } from "./market-quality-reasons";

export type IntradayMonitorData = components["schemas"]["IntradayMonitorResponse"];

type IntradayMonitorProps = {
  symbol: string;
  monitor: IntradayMonitorData;
};

const MINIMUM_CHART_SAMPLES = 4;

export function IntradayMonitor({ symbol, monitor }: IntradayMonitorProps): React.ReactElement {
  const tableScrollRef = useRef<HTMLDivElement>(null);
  const chartPoints = buildChartPoints(monitor.bars);
  const hasChart = chartPoints.length >= MINIMUM_CHART_SAMPLES;

  function scrollTable(direction: -1 | 1): void {
    const container = tableScrollRef.current;
    if (!container) return;
    container.scrollLeft += direction * Math.max(240, Math.floor(container.clientWidth * 0.8));
  }

  return (
    <section
      className="intraday-monitor"
      aria-label={`${symbol} 盘中证据`}
      data-status={monitor.status}
    >
      <header className="intraday-monitor__heading">
        <div>
          <p className="eyebrow">盘中监控 · 1 分钟证据</p>
          <h2>{symbol} 盘中监控</h2>
          <p>仅展示来源明确的完整分钟事实和确定性指标，不生成买卖信号。</p>
        </div>
        <span className="state-chip" data-state={statusState(monitor.status)}>
          {statusLabel(monitor.status)}
        </span>
      </header>

      <div className="intraday-monitor__trace" aria-label="盘中来源与时效">
        <Activity aria-hidden="true" size={18} />
        <strong>{marketSourceLabel(monitor.source, monitor.feed)}</strong>
        <span>{intradayDeliveryModeLabel(monitor)}</span>
        <span className="numeric-data">分钟截止 {formatUtc(monitor.bar_as_of)}</span>
        {usesGatewayReceiptTime(monitor) ? (
          <span className="numeric-data">网关接收时间 {formatUtc(gatewayReceiptTime(monitor))}</span>
        ) : null}
        <span className="numeric-data">分钟年龄 {formatAge(monitor.bar_age_seconds)}</span>
        <span className="numeric-data">报价年龄 {formatAge(monitor.quote_age_seconds)}</span>
      </div>

      <dl className="intraday-monitor__metrics">
        <Metric term="5 分钟变化" value={formatPercent(monitor.return_5m, true)} />
        <Metric term="30 分钟变化" value={formatPercent(monitor.return_30m, true)} />
        <Metric term="30 分钟振幅" value={formatPercent(monitor.range_30m, false)} />
        <Metric term="买卖价差" value={formatSpread(monitor)} />
      </dl>

      {monitor.quality_reasons.length > 0 ? (
        <div className="intraday-monitor__reasons" aria-label={`${symbol} 盘中质量原因`}>
          <TriangleAlert aria-hidden="true" size={18} />
          <MarketQualityReasons reasons={monitor.quality_reasons} />
        </div>
      ) : null}

      {hasChart ? (
        <figure className="intraday-monitor__figure">
          <svg
            className="intraday-monitor__chart"
            data-testid="intraday-chart"
            viewBox="0 0 1000 240"
            preserveAspectRatio="none"
            aria-hidden="true"
            focusable="false"
          >
            <line x1="0" y1="40" x2="1000" y2="40" />
            <line x1="0" y1="120" x2="1000" y2="120" />
            <line x1="0" y1="200" x2="1000" y2="200" />
            <polyline points={chartPoints.join(" ")} />
          </svg>
          <figcaption>
            {monitor.bars.length} 个已返回样本；折线仅作趋势辅助，下方表格最多显示 3 位小数。
          </figcaption>
        </figure>
      ) : (
        <p className="intraday-monitor__empty">
          <Clock3 aria-hidden="true" size={18} />
          证据不足，暂不绘制趋势图。
        </p>
      )}

      <div
        ref={tableScrollRef}
        className="table-scroll intraday-monitor__table-scroll"
        role="region"
        aria-label={`${symbol} 1 分钟行情表格，可横向滚动`}
      >
        <div className="table-scroll__controls" aria-label="1 分钟行情表格横向滚动控制">
          <button className="button button--quiet" type="button" onClick={() => scrollTable(-1)}>
            <ChevronLeft aria-hidden="true" size={17} />
            向左浏览 {symbol} 盘中表格
          </button>
          <button className="button button--quiet" type="button" onClick={() => scrollTable(1)}>
            向右浏览 {symbol} 盘中表格
            <ChevronRight aria-hidden="true" size={17} />
          </button>
        </div>
        <table className="market-table numeric-data" aria-label={`${symbol} 1 分钟行情数据`}>
          <caption>{symbol} 已通过来源策略的完整 1 分钟 OHLCV 事实</caption>
          <thead>
            <tr>
              <th scope="col">观测时间</th>
              <th scope="col">开盘</th>
              <th scope="col">最高</th>
              <th scope="col">最低</th>
              <th scope="col">收盘</th>
              <th scope="col">成交量</th>
              <th scope="col">可用时间</th>
            </tr>
          </thead>
          <tbody>
            {monitor.bars.map((bar) => (
              <tr key={`${bar.observed_at}:${bar.available_at}`}>
                <th scope="row">{formatUtc(bar.observed_at)}</th>
                <td>{formatDisplayDecimal(bar.open)}</td>
                <td>{formatDisplayDecimal(bar.high)}</td>
                <td>{formatDisplayDecimal(bar.low)}</td>
                <td>{formatDisplayDecimal(bar.close)}</td>
                <td>{formatDisplayDecimal(bar.volume)}</td>
                <td>{formatUtc(bar.available_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Metric({ term, value }: { term: string; value: string }): React.ReactElement {
  return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>;
}

function intradayDeliveryModeLabel(monitor: IntradayMonitorData): string {
  const { source, feed, delivery_mode: deliveryMode } = monitor;
  if (source === "alpaca" && feed === "iex" && deliveryMode === "real_time") {
    return "实时 IEX（仅 IEX）";
  }
  if (source === "alpaca" && feed === "sip" && deliveryMode === "real_time") {
    return "实时 SIP（全市场）";
  }
  if (source === "alpaca" && feed === "delayed_sip") return "延迟 SIP";
  return marketDeliveryModeLabel(deliveryMode);
}

function statusLabel(status: IntradayMonitorData["status"]): string {
  const labels: Record<IntradayMonitorData["status"], string> = {
    ready: "已就绪",
    partial: "证据不完整",
    stale: "证据已过期",
    outside_session: "非交易时段",
    unavailable: "当前不可用",
  };
  return labels[status];
}

function statusState(status: IntradayMonitorData["status"]): "ready" | "partial" | "stale" {
  if (status === "ready" || status === "outside_session") return "ready";
  return status === "stale" ? "stale" : "partial";
}

function formatPercent(value: string | null, signed: boolean): string {
  if (value === null) return "证据不足";
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "证据不足";
  const percent = parsed * 100;
  const prefix = signed && percent > 0 ? "+" : "";
  return `${prefix}${percent.toFixed(2)}%`;
}

function formatBps(value: string | null): string {
  if (value === null) return "证据不足";
  const parsed = Number(value);
  return Number.isFinite(parsed) ? `${parsed.toFixed(2)} bps` : "证据不足";
}

function formatSpread(monitor: IntradayMonitorData): string {
  if (monitor.bid === null || monitor.ask === null) return "盘口未提供";
  return formatBps(monitor.spread_bps);
}

function usesGatewayReceiptTime(monitor: IntradayMonitorData): boolean {
  return monitor.source === "fosun"
    && monitor.quality_reasons.includes("FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE");
}

function gatewayReceiptTime(monitor: IntradayMonitorData): string | null {
  return monitor.trade_as_of ?? monitor.quote_as_of ?? monitor.bar_as_of;
}

function formatAge(value: number | null | undefined): string {
  return value === null || value === undefined ? "未知" : `${formatDisplayDecimal(value)} 秒`;
}

function formatUtc(value: string | null): string {
  if (!value) return "无";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return "无效时间";
  return parsed.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC");
}

function buildChartPoints(bars: IntradayMonitorData["bars"]): string[] {
  if (bars.length < MINIMUM_CHART_SAMPLES) return [];
  const closes = bars.map((bar) => Number(bar.close));
  if (closes.some((close) => !Number.isFinite(close))) return [];
  const minimum = Math.min(...closes);
  const maximum = Math.max(...closes);
  const range = maximum - minimum;
  return closes.map((close, index) => {
    const x = bars.length === 1 ? 500 : index * (1000 / (bars.length - 1));
    const y = range === 0 ? 120 : 220 - ((close - minimum) / range) * 200;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  });
}
