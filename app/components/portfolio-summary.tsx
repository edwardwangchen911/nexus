import { CircleDollarSign, LockKeyhole } from "lucide-react";
import type { PortfolioItem, PortfolioSnapshot } from "../(protected)/portfolio/page";

/* eslint-disable jsx-a11y/no-noninteractive-tabindex -- Overflow data tables need keyboard focus for horizontal scrolling. */

type PortfolioSummaryProps = {
  portfolio: PortfolioItem;
  snapshot: PortfolioSnapshot;
  currency: string;
};

export function PortfolioSummary({ portfolio, snapshot, currency }: PortfolioSummaryProps): React.ReactElement {
  const cash = snapshot.cash.find((bucket) => bucket.currency === currency) ?? null;
  const positions = snapshot.positions.filter((position) => position.currency === currency);
  const orders = snapshot.open_orders.filter((order) => order.currency === currency);
  const containsHongKongHolding = positions.some((position) => position.symbol === "00656.HK");

  return (
    <section className="portfolio-summary" aria-label={`${portfolio.name} ${currency} 组合摘要`}>
      <header className="portfolio-summary__heading">
        <div>
          <p className="eyebrow">ORIGINAL CURRENCY</p>
          <h2>{currency}</h2>
        </div>
        <span className="mode-badge"><LockKeyhole aria-hidden="true" size={15} />只读</span>
      </header>

      <dl className="portfolio-summary__cash" aria-label={`${currency} 现金事实`}>
        <Fact term="账面现金" value={cash ? `${currency} ${formatDecimal(cash.cash_balance)}` : "无数据"} />
        <Fact term="可用现金" value={cash ? `${currency} ${formatDecimal(cash.available_cash)}` : "无数据"} />
        <Fact term="数据更新时间" value={formatTime(cash?.provider_as_of ?? snapshot.as_of)} />
      </dl>

      <div className="portfolio-summary__section">
        <h3>持仓</h3>
        {positions.length === 0 ? <p className="portfolio-summary__empty">该原币种没有持仓事实。</p> : (
          <div className="table-scroll" tabIndex={0} role="region" aria-label={`${currency} 持仓表，可横向滚动`}>
            <table className="portfolio-table">
              <caption>{portfolio.name} · {currency} · 原币种</caption>
              <thead><tr><th>标的</th><th>总量</th><th>可用</th><th>冻结</th><th>平均成本</th><th>摊薄成本</th><th>现价</th><th>权限</th></tr></thead>
              <tbody>{positions.map((position) => (
                <tr key={position.instrument_id}>
                  <th scope="row"><span className="portfolio-symbol">{position.symbol ?? "未知标的"}</span><small>{position.exchange}</small></th>
                  <td>{formatDecimal(position.quantity)}</td>
                  <td>{formatDecimal(position.available_quantity)}</td>
                  <td>{formatDecimal(position.frozen_quantity)}</td>
                  <td>{formatNullableDecimal(position.avg_cost, currency)}</td>
                  <td>{formatNullableDecimal(position.diluted_cost, currency)}</td>
                  <td>{formatNullableDecimal(position.current_price, currency)}</td>
                  <td>{position.read_only ? "只读" : "未知"}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
      </div>

      <div className="portfolio-summary__section">
        <h3>未结订单快照</h3>
        {orders.length === 0 ? <p className="portfolio-summary__empty">该原币种没有未结订单事实。</p> : (
          <div className="table-scroll" tabIndex={0} role="region" aria-label={`${currency} 未结订单表，可横向滚动`}>
            <table className="portfolio-table">
              <caption>仅展示脱敏内部快照 ID</caption>
              <thead><tr><th>标的</th><th>方向</th><th>数量</th><th>已成交</th><th>价格</th><th>状态</th><th>权限</th></tr></thead>
              <tbody>{orders.map((order) => (
                <tr key={order.broker_order_snapshot_id}>
                  <th scope="row">{order.symbol ?? "未知标的"}</th>
                  <td>{order.side}</td><td>{formatDecimal(order.quantity)}</td>
                  <td>{formatDecimal(order.filled_quantity)}</td>
                  <td>{formatNullableDecimal(order.price, currency)}</td>
                  <td>{order.status}</td><td>{order.read_only ? "只读" : "未知"}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
      </div>

      {containsHongKongHolding ? (
        <aside className="portfolio-evidence-note">
          <CircleDollarSign aria-hidden="true" size={18} />
          <p><strong>00656.HK 仅做持仓与市场风险展示。</strong>香港交易所财报证据尚未准入，不生成基本面买卖结论。</p>
        </aside>
      ) : null}
    </section>
  );
}

function Fact({ term, value }: { term: string; value: string }): React.ReactElement {
  return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>;
}

function formatNullableDecimal(value: string | null, currency: string): string {
  return value === null ? "无" : `${currency} ${formatDecimal(value)}`;
}

function formatDecimal(value: string): string {
  const match = /^(-?)(\d+)(\.\d+)?$/.exec(value);
  if (!match) return "无效数值";
  return `${match[1]}${match[2].replace(/\B(?=(\d{3})+(?!\d))/g, ",")}${match[3] ?? ""}`;
}

function formatTime(value: string | null): string {
  if (!value) return "无";
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? "无效时间" : `${date.toISOString().replace("T", " ").replace(".000Z", " UTC")}`;
}
