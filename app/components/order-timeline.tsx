"use client";

import { Ban, Clock3, ReceiptText } from "lucide-react";
import { useId } from "react";

import { formatDisplayDecimal } from "../lib/display-decimal";
import type { components } from "../lib/generated/schema";

export type PaperOrder = components["schemas"]["PaperOrderResponse"];
type OrderStatus = PaperOrder["status"];

const statusLabels: Record<OrderStatus, string> = {
  proposed: "已提议",
  risk_rejected: "风控拒绝",
  risk_approved: "风控通过",
  awaiting_approval: "等待批准",
  submitted: "已提交模拟盘",
  partially_filled: "部分成交",
  filled: "已成交",
  cancelled: "已取消",
  rejected: "已拒绝",
  reconciling: "对账中",
};

export function OrderTimeline({ order, allowCancel = true, onCancel }: { order: PaperOrder; allowCancel?: boolean; onCancel: (order: PaperOrder, trigger?: HTMLButtonElement) => void }): React.ReactElement {
  const titleId = useId();
  const symbol = order.symbol ?? "未知标的";
  return (
    <article className="paper-order" data-status={order.status} aria-labelledby={titleId}>
      <header className="paper-order__heading">
        <div>
          <p className="eyebrow">订单 · 仅追加事实</p>
          <h3 id={titleId}>{symbol} · {order.side === "buy" ? "买入" : "卖出"}</h3>
          <p className="paper-order__identity numeric-data">内部订单 {order.order_id}</p>
        </div>
        <div className="paper-order__actions">
          <span className="state-chip" data-state={order.status}>{statusLabels[order.status]}</span>
          {order.cancellable && allowCancel ? (
            <button className="button button--danger" type="button" onClick={(event) => onCancel(order, event.currentTarget)}>
              <Ban aria-hidden="true" size={17} />取消 {symbol} 模拟订单
            </button>
          ) : null}
        </div>
      </header>

      <dl className="paper-order__facts">
        <Fact term="数量" value={formatPaperDecimal(order.quantity)} />
        <Fact term="已成交" value={formatPaperDecimal(order.filled_quantity)} />
        <Fact term="剩余" value={formatPaperDecimal(order.remaining_quantity)} />
        <Fact term="最坏价格" value={`USD ${formatPaperDecimal(order.worst_price, 2)}`} />
        <Fact term="事件版本" value={String(order.current_event_seq)} />
        <Fact term="提交时间" value={formatPaperTime(order.submitted_at)} />
      </dl>

      <details className="paper-order__ledger" open>
        <summary><Clock3 aria-hidden="true" size={17} />事件与成交凭条</summary>
        <ol className="paper-timeline" aria-label={`${symbol} 订单事件时间线`}>
          {order.events.map((event) => (
            <li key={event.event_seq} data-event-seq={event.event_seq}>
              <span className="paper-timeline__seq numeric-data">E{String(event.event_seq).padStart(2, "0")}</span>
              <div>
                <strong>{event.from_status ?? "初始"} → {event.to_status}</strong>
                <p>{event.actor_kind} · {event.reason_code}</p>
              </div>
              <time className="numeric-data" dateTime={event.occurred_at}>{formatPaperTime(event.occurred_at)}</time>
            </li>
          ))}
        </ol>

        <div className="paper-order__fills">
          <h4><ReceiptText aria-hidden="true" size={17} />模拟成交事实</h4>
          {order.fills.length === 0 ? <p className="paper-order__empty">尚无模拟成交；不以零值替代。</p> : (
            <ScrollableTable label={`${symbol} 模拟成交表，可横向滚动`}>
              <table className="paper-fill-table numeric-data" aria-label={`${symbol} 模拟成交事实`}>
                <caption>价格、滑点与数据权威均来自后端确定性执行模型。</caption>
                <thead><tr><th scope="col">序号</th><th scope="col">数量</th><th scope="col">价格</th><th scope="col">名义金额</th><th scope="col">费用</th><th scope="col">滑点</th><th scope="col">来源 / feed</th><th scope="col">交付</th><th scope="col">来源时点</th></tr></thead>
                <tbody>{order.fills.map((fill) => (
                  <tr key={fill.fill_id}>
                    <th scope="row">F{String(fill.fill_seq).padStart(2, "0")}</th>
                    <td>{formatPaperDecimal(fill.quantity)}</td>
                    <td>USD {formatPaperDecimal(fill.price, 2)}</td>
                    <td>USD {formatPaperDecimal(fill.notional, 2)}</td>
                    <td>USD {formatPaperDecimal(fill.fee, 2)}</td>
                    <td>{formatDisplayDecimal(fill.slippage_bps)} bps · USD {formatPaperDecimal(fill.slippage, 2)}</td>
                    <td>{fill.source} / {fill.feed}</td>
                    <td>{fill.delivery_mode}</td>
                    <td>{formatPaperTime(fill.source_as_of)}</td>
                  </tr>
                ))}</tbody>
              </table>
            </ScrollableTable>
          )}
        </div>
      </details>
    </article>
  );
}

function Fact({ term, value }: { term: string; value: string }): React.ReactElement {
  return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>;
}

function ScrollableTable({ label, children }: { label: string; children: React.ReactNode }): React.ReactElement {
  // Bounded horizontal data regions must remain keyboard-scrollable on narrow screens.
  // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
  return <div className="table-scroll" role="region" aria-label={label} tabIndex={0}>{children}</div>;
}

export function formatPaperDecimal(value: string, fixedDigits?: number): string {
  return formatDisplayDecimal(value, { group: true, fixedDigits });
}

export function formatPaperTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? "无效时间" : date.toISOString().replace("T", " ").replace(/(?:\.\d+)?Z$/, " UTC");
}
