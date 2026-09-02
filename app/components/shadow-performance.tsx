"use client";

import { formatDisplayTime, formatPercent } from "../lib/display-format";
import { statusLabel, reasonText } from "../lib/display-labels";
import { marketDeliveryModeLabel } from "../lib/market-evidence-labels";

import { formatDisplayDecimal } from "../lib/display-decimal";
import { ChevronLeft, ChevronRight, ShieldCheck } from "lucide-react";
import { useEffect, useId, useRef, useState } from "react";

import type { components } from "../lib/generated/schema";

type Shadow = components["schemas"]["ShadowGroupResponse"];
type Portfolio = Shadow["portfolios"][number];
type Variant = Portfolio["variant"];
type Metric = components["schemas"]["ShadowMetricValueResponse"];

const variants: Variant[] = ["hold_current", "minimum_change", "strategy_target"];
const variantLabels: Record<Variant, string> = {
  hold_current: "保持现状",
  minimum_change: "最小调整",
  strategy_target: "策略目标",
};
const lineLabels: Record<Variant, string> = {
  hold_current: "实线",
  minimum_change: "虚线",
  strategy_target: "点线",
};
const statusLabels: Record<Shadow["proposal_status"], string> = {
  draft: "草稿",
  compared: "已比较",
  shadowing: "观察中",
  validated: "已验证",
  rejected: "已拒绝",
  expired: "已过期",
};

export function ShadowPerformance({ shadow }: { shadow: Shadow }): React.ReactElement {
  const chartRef = useRef<HTMLDivElement>(null);
  const detailId = useId();
  const sessions = shadow.portfolios[0].nav.map((point) => point.session);
  const selectionScope = `${shadow.shadow_group_id}:${shadow.coverage.completed}`;
  const [selection, setSelection] = useState({
    scope: selectionScope,
    index: Math.max(0, shadow.coverage.completed - 1),
  });
  const activeIndex = selection.scope === selectionScope
    ? selection.index
    : Math.max(0, shadow.coverage.completed - 1);
  const boundedIndex = Math.min(Math.max(activeIndex, 0), Math.max(0, sessions.length - 1));

  useEffect(() => {
    const container = chartRef.current;
    if (!container || typeof ResizeObserver === "undefined" || sessions.length === 0) return;
    let disposed = false;
    let cleanup = () => undefined;
    void import("lightweight-charts").then(({ ColorType, LineSeries, LineStyle, createChart }) => {
      if (disposed) return;
      const styles = getComputedStyle(document.documentElement);
      const chart = createChart(container, {
        autoSize: true,
        height: 320,
        layout: {
          attributionLogo: false,
          background: { type: ColorType.Solid, color: token(styles, "--component-chart-background") },
          textColor: token(styles, "--component-chart-text"),
        },
        grid: {
          vertLines: { color: token(styles, "--component-chart-grid") },
          horzLines: { color: token(styles, "--component-chart-grid") },
        },
        rightPriceScale: { borderColor: token(styles, "--component-chart-border") },
        timeScale: { borderColor: token(styles, "--component-chart-border") },
      });
      const lineStyles = [LineStyle.Solid, LineStyle.Dashed, LineStyle.Dotted];
      const colors = [
        token(styles, "--component-shadow-hold"),
        token(styles, "--component-shadow-minimum"),
        token(styles, "--component-shadow-strategy"),
      ];
      shadow.portfolios.forEach((portfolio, index) => {
        const series = chart.addSeries(LineSeries, {
          color: colors[index],
          lineStyle: lineStyles[index],
          lineWidth: 2,
          lastValueVisible: false,
          priceLineVisible: false,
        });
        series.setData(portfolio.nav.map((point) => ({ time: point.session, value: Number(point.nav) })));
        if (index === 0) {
          series.createPriceLine({
            price: 100,
            color: token(styles, "--component-shadow-anchor"),
            lineStyle: LineStyle.Dashed,
            lineWidth: 1,
            axisLabelVisible: true,
            title: "NAV 100",
          });
        }
      });
      chart.timeScale().fitContent();
      const observer = new ResizeObserver(() => chart.timeScale().fitContent());
      observer.observe(container);
      cleanup = () => {
        observer.disconnect();
        chart.remove();
      };
    });
    return () => {
      disposed = true;
      cleanup();
    };
  }, [shadow.portfolios, sessions.length]);

  function moveTo(index: number): void {
    setSelection({
      scope: selectionScope,
      index: Math.min(Math.max(index, 0), Math.max(0, sessions.length - 1)),
    });
  }

  function handleKey(event: React.KeyboardEvent<HTMLDivElement>): void {
    if (event.key === "Home") moveTo(0);
    else if (event.key === "End") moveTo(sessions.length - 1);
    else if (event.key === "ArrowLeft") moveTo(boundedIndex - 1);
    else if (event.key === "ArrowRight") moveTo(boundedIndex + 1);
    else return;
    event.preventDefault();
  }

  const latestCheckpoint = shadow.checkpoints.at(-1);
  const activeSession = sessions[boundedIndex];
  const activePoints = shadow.portfolios.map((portfolio) => portfolio.nav[boundedIndex]);
  const outcomes = shadow.portfolios.flatMap((portfolio) =>
    portfolio.initial_outcomes.map((outcome) => ({ portfolio, outcome })));

  return (
    <section className="shadow-performance" aria-labelledby="shadow-performance-title">
      <header className="shadow-performance__heading">
        <div>
          <p className="eyebrow">前向证据 · 63 个交易日</p>
          <h2 id="shadow-performance-title">前向影子绩效</h2>
          <p>三方案从同一冻结起点前向观察；所有数值来自后端封存事实，浏览器不重算绩效。</p>
        </div>
        <span className="state-chip" data-state={shadow.sealed ? "stale" : "ready"}>
          {shadow.sealed ? `最终状态：${statusLabels[shadow.proposal_status]}` : statusLabels[shadow.proposal_status]}
        </span>
      </header>

      <div className="shadow-performance__safety" role="note">
        <ShieldCheck aria-hidden="true" size={20} />
        <p><strong>纯观察边界：</strong>不会创建订单，也不需要交易审批；本区没有验证、晋级或自进化控制。</p>
      </div>

      <dl className="shadow-performance__summary">
        <div><dt>开始交易日</dt><dd className="numeric-data">{shadow.start_session}</dd></div>
        <div><dt>最新交易日</dt><dd className="numeric-data">{shadow.freshness.latest_session ?? "尚无"}</dd></div>
        <div><dt>完成进度</dt><dd className="numeric-data">{shadow.coverage.completed} / {shadow.coverage.required}</dd></div>
        <div><dt>当前检查点</dt><dd className="numeric-data">{latestCheckpoint ? `${latestCheckpoint.ordinal} 日` : "尚无"}</dd></div>
        <div><dt>数据状态</dt><dd>{freshnessLabel(shadow.freshness.status)}</dd></div>
        <div><dt>可用时间</dt><dd className="numeric-data">{formatDisplayTime(shadow.freshness.available_at)}</dd></div>
      </dl>

      <div className="shadow-performance__chart-shell">
        <div className="shadow-performance__legend-row">
          <ul className="shadow-performance__legend" aria-label="三方案线型图例">
            {variants.map((variant) => (
              <li key={variant}>
                <span className={`shadow-line-key shadow-line-key--${lineClass(variant)}`} aria-hidden="true" />
                {variantLabels[variant]} · {lineLabels[variant]}
              </li>
            ))}
          </ul>
          <span className="shadow-performance__anchor"><span aria-hidden="true" />净值基准 100</span>
        </div>
        <div ref={chartRef} className="shadow-performance__canvas" aria-hidden="true" />
        <ul className="shadow-performance__end-labels" aria-label="曲线终点直接标注">
          {shadow.portfolios.map((portfolio) => (
            <li key={portfolio.variant} data-variant={portfolio.variant}>
              {variantLabels[portfolio.variant]} <span className="numeric-data">{portfolio.nav.length ? formatDisplayDecimal(portfolio.nav.at(-1)!.nav) : "尚无"}</span>
            </li>
          ))}
        </ul>
        <p className="shadow-performance__attribution">
          图形由 <a href="https://www.tradingview.com/lightweight-charts/" rel="noreferrer">TradingView Lightweight Charts™</a> 提供。
        </p>
      </div>

      {/* This focusable group is the keyboard equivalent of selecting a chart session. */}
      {/* eslint-disable jsx-a11y/no-noninteractive-element-interactions, jsx-a11y/no-noninteractive-tabindex */}
      <div
        className="shadow-performance__navigator"
        role="group"
        aria-label="选择影子交易日"
        aria-describedby={detailId}
        tabIndex={0}
        onKeyDown={handleKey}
      >
        <button className="button button--quiet" type="button" onClick={() => moveTo(boundedIndex - 1)} disabled={boundedIndex <= 0}>
          <ChevronLeft aria-hidden="true" size={17} />上一交易日
        </button>
        <p id={detailId} role="status" aria-label="当前影子交易日详情" aria-live="polite" className="numeric-data">
          {activeSession ?? "尚无交易日"}
          {activeSession ? activePoints.map((point, index) => ` · ${variantLabels[variants[index]]} ${point ? formatDisplayDecimal(point.nav) : "未知"}`) : ""}
        </p>
        <button className="button button--quiet" type="button" onClick={() => moveTo(boundedIndex + 1)} disabled={boundedIndex >= sessions.length - 1}>
          下一交易日<ChevronRight aria-hidden="true" size={17} />
        </button>
      </div>
      {/* eslint-enable jsx-a11y/no-noninteractive-element-interactions, jsx-a11y/no-noninteractive-tabindex */}

      <ScrollableTable label="三方案逐交易日净值 表格，可横向滚动">
        <table className="shadow-nav-table numeric-data" aria-label="三方案逐交易日净值">
          <caption>图形对应的逐交易日 NAV，最多显示 3 位小数；底层数值保持原精度。</caption>
          <thead><tr><th scope="col">交易日</th>{variants.map((variant) => <th scope="col" key={variant}>{variantLabels[variant]}</th>)}</tr></thead>
          <tbody>
            {sessions.map((session, index) => (
              <tr key={session} data-active={index === boundedIndex ? "true" : "false"}>
                <th scope="row">{session}</th>
                {shadow.portfolios.map((portfolio) => <td key={portfolio.variant}>{formatDisplayDecimal(portfolio.nav[index].nav)}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </ScrollableTable>

      <section className="shadow-performance__section" aria-labelledby="shadow-checkpoints-title">
        <header><div><p className="eyebrow">检查点 · 5 / 21 / 63</p><h3 id="shadow-checkpoints-title">检查点绩效</h3></div><p>未知值保留原因，不替换为零。</p></header>
        <ScrollableTable label="影子检查点绩效表格，可横向滚动">
          <table className="shadow-metrics-table numeric-data">
            <caption>每个检查点的后端封存绩效</caption>
            <thead><tr><th scope="col">检查点</th><th scope="col">方案</th><th scope="col">净累计收益</th><th scope="col">相对保持差异</th><th scope="col">最大回撤</th><th scope="col">回撤改善</th><th scope="col">年化波动</th><th scope="col">波动改善</th><th scope="col">累计换手</th><th scope="col">费用拖累 <span>净值指数点</span></th><th scope="col">最差交易日</th></tr></thead>
            <tbody>
              {shadow.checkpoints.flatMap((checkpoint) => checkpoint.metrics.map((item) => (
                <tr key={`${checkpoint.ordinal}:${item.variant}`}>
                  <th scope="row">{checkpoint.ordinal} 日 · {checkpoint.status === "complete" ? "完整" : "不完整"}</th>
                  <td>{variantLabels[item.variant]}</td>
                  <MetricCell metric={item.metrics.net_cumulative_return} />
                  <MetricCell metric={item.metrics.return_delta_vs_hold} difference />
                  <MetricCell metric={item.metrics.max_drawdown} />
                  <MetricCell metric={item.metrics.drawdown_improvement} difference />
                  <MetricCell metric={item.metrics.annualized_volatility} />
                  <MetricCell metric={item.metrics.volatility_improvement} difference />
                  <MetricCell metric={item.metrics.cumulative_turnover} />
                  <MetricCell metric={item.metrics.fee_drag_index_points} indexPoints />
                  <td>{item.metrics.worst_sessions.length === 0 ? `未知 · ${reasonText(checkpoint.reason ?? item.metrics.net_cumulative_return.reason)}` : item.metrics.worst_sessions.map((worst) => `${worst.session} ${formatPercent(worst.return_value)}`).join("；")}</td>
                </tr>
              )))}
            </tbody>
          </table>
        </ScrollableTable>
      </section>

      <section className="shadow-performance__evidence-grid" aria-label="覆盖率与数据来源">
        <div>
          <h3>覆盖与刷新</h3>
          <dl>
            <div><dt>已完成 / 应到</dt><dd className="numeric-data">{shadow.coverage.completed} / {shadow.coverage.due}</dd></div>
            <div><dt>下一交易日</dt><dd className="numeric-data">{shadow.coverage.next_expected_session ?? "已完成"}</dd></div>
            <div><dt>缺失交易日</dt><dd className="numeric-data">{shadow.coverage.missing_sessions.join("、") || "无"}</dd></div>
            <div><dt>最后尝试</dt><dd className="numeric-data">{formatDisplayTime(shadow.freshness.last_attempt_at)}</dd></div>
            <div><dt>失败原因</dt><dd>{shadow.last_failed_attempt ? reasonText(shadow.last_failed_attempt.reason) : "无"}</dd></div>
          </dl>
        </div>
        <div>
          <h3>来源与交付</h3>
          <ul>
            {shadow.sources.map((source) => (
              <li key={`${source.source}:${source.kind}:${source.delivery_mode}`}>
                <strong>{source.source} · {source.kind === "price_bars" ? "历史行情" : source.kind === "corporate_actions" ? "公司行动" : source.kind} · {marketDeliveryModeLabel(source.delivery_mode)}</strong>
                <span className="numeric-data">{source.latest_session} · {formatDisplayTime(source.available_at)}</span>
              </li>
            ))}
          </ul>
        </div>
      </section>

      <section className="shadow-performance__section" aria-labelledby="shadow-outcomes-title">
        <header><div><p className="eyebrow">首交易日 · 模拟结果</p><h3 id="shadow-outcomes-title">首交易日模拟结果</h3></div><p>这些是模拟成交事实，不是订单或交易回执。</p></header>
        {outcomes.length === 0 ? <p className="inline-notice">首交易日没有模拟调仓结果。</p> : (
          <ScrollableTable label="首交易日模拟结果表格，可横向滚动">
            <table className="shadow-outcomes-table numeric-data">
              <caption>三方案首交易日的确定性模拟结果</caption>
              <thead><tr><th scope="col">方案</th><th scope="col">标的</th><th scope="col">方向</th><th scope="col">状态</th><th scope="col">原因</th><th scope="col">数量</th><th scope="col">价格</th><th scope="col">名义金额</th><th scope="col">成本</th></tr></thead>
              <tbody>{outcomes.map(({ portfolio, outcome }) => <tr key={`${portfolio.variant}:${outcome.instrument_id}`}><th scope="row">{variantLabels[portfolio.variant]}</th><td>{outcome.instrument_id}</td><td>{statusLabel(outcome.side)}</td><td>{statusLabel(outcome.status)}</td><td>{reasonText(outcome.reason)}</td><td>{outcome.fill ? formatDisplayDecimal(outcome.fill.quantity) : "—"}</td><td>{outcome.fill ? formatDisplayDecimal(outcome.fill.price) : "—"}</td><td>{outcome.fill ? formatDisplayDecimal(outcome.fill.notional) : "—"}</td><td>{outcome.fill ? formatDisplayDecimal(outcome.fill.cost) : "—"}</td></tr>)}</tbody>
            </table>
          </ScrollableTable>
        )}
      </section>
    </section>
  );
}

function ScrollableTable({ label, children }: { label: string; children: React.ReactNode }): React.ReactElement {
  // The bounded horizontal scroll region must be keyboard-focusable on narrow screens.
  // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
  return <div className="table-scroll" role="region" aria-label={label} tabIndex={0}>{children}</div>;
}

function MetricCell({ metric, difference = false, indexPoints = false }: { metric: Metric; difference?: boolean; indexPoints?: boolean }): React.ReactElement {
  return <td>{metric.status === "known" ? (indexPoints ? formatDisplayDecimal(metric.value) : formatPercent(metric.value, difference)) : `未知 · ${reasonText(metric.reason)}`}</td>;
}

function freshnessLabel(value: Shadow["freshness"]["status"]): string {
  return value === "current" ? "当前" : value === "stale" ? "已滞后" : "尚无到期交易日";
}

function lineClass(variant: Variant): "solid" | "dashed" | "dotted" {
  return variant === "hold_current" ? "solid" : variant === "minimum_change" ? "dashed" : "dotted";
}

function token(styles: CSSStyleDeclaration, name: string): string {
  const value = styles.getPropertyValue(name).trim();
  if (!value) throw new Error(`Missing Shadow chart token: ${name}`);
  return value;
}
