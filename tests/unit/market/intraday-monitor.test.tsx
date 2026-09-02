import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  IntradayMonitor,
  type IntradayMonitorData,
} from "../../../app/components/intraday-monitor";

const monitor: IntradayMonitorData = {
  instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781",
  status: "ready",
  source: "alpaca",
  feed: "iex",
  delivery_mode: "real_time",
  bar_as_of: "2026-08-21T20:00:00Z",
  quote_as_of: "2026-08-21T20:00:01Z",
  trade_as_of: "2026-08-21T20:00:02Z",
  bar_age_seconds: 15,
  quote_age_seconds: 14,
  last: "227.160000000000",
  bid: "227.110000000000",
  ask: "227.210000000000",
  return_5m: "0.010000000000",
  return_30m: "-0.025000000000",
  range_30m: "0.031250000000",
  spread_bps: "4.402300000000",
  sample_count: 4,
  quality_reasons: [],
  bars: [
    ["2026-08-21T19:57:00Z", "224.000000000000"],
    ["2026-08-21T19:58:00Z", "225.500000000000"],
    ["2026-08-21T19:59:00Z", "226.800000000000"],
    ["2026-08-21T20:00:00Z", "227.160000000000"],
  ].map(([observedAt, close], index) => ({
    observed_at: observedAt,
    available_at: observedAt,
    open: close,
    high: `${225 + index}.500000000000`,
    low: `${223 + index}.500000000000`,
    close,
    volume: `${1_000_000 + index}.000000000000`,
    quality_flags: [],
  })),
};

describe("IntradayMonitor", () => {
  it("renders exact source authority, deterministic metrics and a semantic minute table", () => {
    render(<IntradayMonitor symbol="AAPL" monitor={monitor} />);

    const region = screen.getByRole("region", { name: "AAPL 盘中证据" });
    expect(region).toHaveTextContent("盘中监控 · 1 分钟证据");
    expect(region).toHaveTextContent("实时 IEX（仅 IEX）");
    expect(region).toHaveTextContent("已就绪");
    expect(region).toHaveTextContent("+1%");
    expect(region).toHaveTextContent("-2.5%");
    expect(region).toHaveTextContent("3.125%");
    expect(region).toHaveTextContent("4.402 基点");
    expect(screen.getByRole("table", { name: "AAPL 1 分钟行情数据" })).toHaveTextContent(
      "227.16",
    );
    expect(screen.getByRole("button", { name: "向左浏览 AAPL 盘中表格" })).toBeVisible();
    expect(screen.getByRole("button", { name: "向右浏览 AAPL 盘中表格" })).toBeVisible();
    expect(screen.getByTestId("intraday-chart")).toHaveAttribute("aria-hidden", "true");
  });

  it("keeps explicit quality reasons and refuses to draw a chart with insufficient evidence", () => {
    render(<IntradayMonitor
      symbol="AAPL"
      monitor={{
        ...monitor,
        status: "unavailable",
        source: null,
        feed: null,
        delivery_mode: null,
        bars: monitor.bars.slice(0, 2),
        sample_count: 2,
        return_5m: null,
        return_30m: null,
        range_30m: null,
        spread_bps: null,
        quality_reasons: ["INSUFFICIENT_CONTIGUOUS_BARS"],
      }}
    />);

    expect(screen.getByText("证据不足，暂不绘制趋势图。")).toBeVisible();
    expect(screen.getAllByText("证据不足").length).toBeGreaterThan(0);
    expect(screen.getByText("实时性不可用")).toBeVisible();
    expect(screen.getByText("INSUFFICIENT_CONTIGUOUS_BARS")).toBeVisible();
    expect(screen.queryByTestId("intraday-chart")).not.toBeInTheDocument();
  });

  it("labels delayed outside-session evidence without presenting a recommendation", () => {
    render(<IntradayMonitor
      symbol="AAPL"
      monitor={{
        ...monitor,
        status: "outside_session",
        feed: "delayed_sip",
        delivery_mode: "delayed",
      }}
    />);

    const region = screen.getByRole("region", { name: "AAPL 盘中证据" });
    expect(region).toHaveTextContent("延迟 SIP");
    expect(region).toHaveTextContent("非交易时段");
    expect(region).not.toHaveTextContent(/交易建议|买入信号|卖出信号|加仓|减仓/);
  });

  it("labels Fosun receipt-time evidence and never invents a bid/ask spread", () => {
    render(<IntradayMonitor
      symbol="AAPL"
      monitor={{
        ...monitor,
        status: "partial",
        source: "fosun",
        feed: "l1",
        delivery_mode: "unknown",
        bid: null,
        ask: null,
        spread_bps: "4.402300000000",
        trade_as_of: "2026-08-21T20:00:03Z",
        quality_reasons: [
          "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
          "INTRADAY_BID_ASK_UNAVAILABLE",
          "INTRADAY_DELIVERY_UNVERIFIED",
        ],
      }}
    />);

    const region = screen.getByRole("region", { name: "AAPL 盘中证据" });
    expect(region).toHaveTextContent("复星 L1（只读）");
    expect(region).toHaveTextContent("实时性未获证明");
    expect(region).toHaveTextContent("盘口未提供");
    expect(region).not.toHaveTextContent("4.402 基点");
    expect(region).toHaveTextContent("分钟截止 2026-08-22 04:00:00 北京时间");
    expect(region).toHaveTextContent("网关接收时间 2026-08-22 04:00:03 北京时间");
    expect(region).toHaveTextContent("上游报价未提供成交时间");
    expect(region).toHaveTextContent("FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE");
  });
});
