import { statusLabel } from "../../lib/display-labels";
import type { components } from "../../lib/generated/schema";
import { formatPaperDecimal } from "../order-timeline";

type BrokerIntent = components["schemas"]["BrokerIntentResponse"];

export function OrderBatch({ intent }: { intent: BrokerIntent }): React.ReactElement {
  return (
    <section className="broker-section" aria-labelledby="broker-orders-title">
      <header className="broker-section__heading">
        <div><p className="eyebrow">服务端推导订单</p><h2 id="broker-orders-title">订单预览</h2></div>
        <p>{intent.orders.length} 笔·不可在浏览器中改写</p>
      </header>
      {/* Bounded horizontal data regions remain keyboard-scrollable at 200% zoom. */}
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex */}
      <div className="table-scroll" role="region" aria-label="服务端订单批次，可横向滚动" tabIndex={0}>
        <table className="broker-order-table numeric-data" aria-label="服务端订单批次">
          <caption>仅展示被确定性风控接受的服务端事实。</caption>
          <thead><tr><th scope="col">方向</th><th scope="col">标的识别</th><th scope="col">数量</th><th scope="col">限价</th><th scope="col">类型</th><th scope="col">时段</th><th scope="col">状态</th></tr></thead>
          <tbody>{intent.orders.map((order) => (
            <tr key={order.order_id}>
              <td>{order.side === "buy" ? "买入" : "卖出"}</td>
              <th scope="row">{shortId(order.instrument_id)}</th>
              <td>{formatPaperDecimal(order.quantity)}</td>
              <td>USD {formatPaperDecimal(order.limit_price, 2)}</td>
              <td>{statusLabel(order.order_type)} · {statusLabel(order.time_in_force)}</td>
              <td>{statusLabel(order.session)}</td>
              <td><span className="state-chip" data-state={order.status}>{statusLabel(order.status)}</span></td>
            </tr>
          ))}</tbody>
        </table>
      </div>
      <p className="broker-section__guard">派发、改单、撤单控件未启用；当前批准也不会创建派发任务。</p>
    </section>
  );
}

function shortId(value: string): string {
  return `${value.slice(0, 8)}…${value.slice(-4)}`;
}
