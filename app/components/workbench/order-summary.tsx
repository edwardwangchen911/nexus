import { ArrowRight, ClipboardList } from "lucide-react";

import { formatDisplayDecimal } from "../../lib/display-decimal";
import { executionPresentation } from "../../lib/product-status";
import type { components } from "../../lib/generated/schema";

type Cycle = components["schemas"]["AgentPaperCycleResponse"];
type Order = components["schemas"]["PaperOrderResponse"];

const sideLabels: Record<Order["side"], string> = { buy: "买入", sell: "卖出" };

export function OrderSummary({ orders, latestCycle }: { orders: Order[]; latestCycle: Cycle | null }): React.ReactElement {
  const emptyState = latestCycle ? executionPresentation(latestCycle, []) : null;
  return (
    <section className="workbench-panel workbench-orders" aria-label="活动订单">
      <header className="workbench-panel__heading">
        <div><p className="eyebrow">ACTIVE ORDERS</p><h2>活动订单</h2></div>
        <a href="/paper">查看全部订单<ArrowRight aria-hidden="true" size={15} /></a>
      </header>
      {orders.length === 0 ? <div className="workbench-orders__empty">
        <ClipboardList aria-hidden="true" size={22} />
        <div><strong>{emptyState?.label ?? "暂无活动订单"}</strong><p>{emptyState?.detail ?? "尚无可跟踪的内部模拟订单。"}</p></div>
      </div> : <ul>
        {orders.map((order) => <li key={order.order_id}>
          <span className="workbench-order__symbol numeric-data">{order.symbol}</span>
          <span>{sideLabels[order.side]}</span>
          <span className="numeric-data">{formatDisplayDecimal(order.filled_quantity)} / {formatDisplayDecimal(order.quantity)}</span>
          <span className="workbench-order__state">{order.status}</span>
        </li>)}
      </ul>}
      {latestCycle ? <a className="workbench-cycle-link" href={`/decisions/${latestCycle.cycle_id}`}>最近周期 {latestCycle.cycle_id.slice(0, 8)}<ArrowRight aria-hidden="true" size={15} /></a> : null}
    </section>
  );
}
