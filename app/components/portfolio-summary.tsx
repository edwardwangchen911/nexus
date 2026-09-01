import { CircleDollarSign, LockKeyhole } from "lucide-react";
import type { PortfolioItem, PortfolioSnapshot } from "../(protected)/portfolio/page";

/* eslint-disable jsx-a11y/no-noninteractive-tabindex -- Overflow data tables need keyboard focus for horizontal scrolling. */

type PortfolioSummaryProps = {
  portfolio: PortfolioItem;
  snapshot: PortfolioSnapshot;
  currency: string;
};

type Position = PortfolioSnapshot["positions"][number];
type PositionValuation = { position: Position; value: bigint | null };
type CurrencyValuation = {
  cashValue: bigint | null;
  positionValues: PositionValuation[];
  holdingsValue: bigint | null;
  totalValue: bigint | null;
  missingFacts: string[];
  asOf: string | null;
  asOfKind: "Provider 截止时间" | "快照截止时间";
};

const DECIMAL_PLACES = 12;
const DECIMAL_FACTOR = BigInt(10) ** BigInt(DECIMAL_PLACES);
const BIGINT_ZERO = BigInt(0);
const BIGINT_TWO = BigInt(2);
const BIGINT_HUNDRED = BigInt(100);
const PERCENT_HUNDREDTHS_FACTOR = BigInt(10_000);

export function PortfolioSummary({ portfolio, snapshot, currency }: PortfolioSummaryProps): React.ReactElement {
  const cash = snapshot.cash.find((bucket) => bucket.currency === currency) ?? null;
  const positions = snapshot.positions
    .filter((position) => position.currency === currency)
    .sort((left, right) => (left.symbol ?? left.instrument_id).localeCompare(right.symbol ?? right.instrument_id));
  const orders = snapshot.open_orders.filter((order) => order.currency === currency);
  const containsHongKongHolding = positions.some((position) => position.symbol === "00656.HK");
  const valuation = currencyValuation(snapshot, cash, positions);

  return (
    <section className="portfolio-summary" aria-label={`${portfolio.name} ${currency} 组合摘要`}>
      <header className="portfolio-summary__heading">
        <div>
          <p className="eyebrow">原币种账户</p>
          <h2>{currency}</h2>
        </div>
        <span className="mode-badge"><LockKeyhole aria-hidden="true" size={15} />只读</span>
      </header>

      <CurrencyValuationSummary
        currency={currency}
        provider={portfolio.provider}
        snapshot={snapshot}
        valuation={valuation}
      />

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
              <thead><tr><th>标的</th><th>总量</th><th>可用</th><th>冻结</th><th>平均成本</th><th>摊薄成本</th><th>现价</th><th>持仓市值</th><th>权限</th></tr></thead>
              <tbody>{valuation.positionValues.map(({ position, value }) => (
                <tr key={position.instrument_id}>
                  <th scope="row">
                    {isPersistedUsMarketHolding(position) ? (
                      <a
                        className="portfolio-market-link"
                        href={`/market?selected=${encodeURIComponent(position.instrument_id)}`}
                        aria-label={`查看 ${position.symbol} 行情和 K 线`}
                      >
                        <span className="portfolio-symbol">{position.symbol}</span>
                      </a>
                    ) : <span className="portfolio-symbol">{position.symbol ?? "未知标的"}</span>}
                    <small>{position.exchange}</small>
                  </th>
                  <td>{formatDecimal(position.quantity)}</td>
                  <td>{formatDecimal(position.available_quantity)}</td>
                  <td>{formatDecimal(position.frozen_quantity)}</td>
                  <td>{formatNullableDecimal(position.avg_cost, currency)}</td>
                  <td>{formatNullableDecimal(position.diluted_cost, currency)}</td>
                  <td>{formatNullableDecimal(position.current_price, currency)}</td>
                  <td>{value === null ? "无法计算" : `${currency} ${formatMoney(value)}`}</td>
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

export function PortfolioValuationSummary({
  provider,
  snapshot,
  currency,
}: {
  provider: string;
  snapshot: PortfolioSnapshot;
  currency: string;
}): React.ReactElement {
  const cash = snapshot.cash.find((bucket) => bucket.currency === currency) ?? null;
  const positions = snapshot.positions
    .filter((position) => position.currency === currency)
    .sort((left, right) => (left.symbol ?? left.instrument_id).localeCompare(right.symbol ?? right.instrument_id));
  return <CurrencyValuationSummary
    currency={currency}
    provider={provider}
    snapshot={snapshot}
    valuation={currencyValuation(snapshot, cash, positions)}
  />;
}

function CurrencyValuationSummary({
  currency,
  provider,
  snapshot,
  valuation,
}: {
  currency: string;
  provider: string;
  snapshot: PortfolioSnapshot;
  valuation: CurrencyValuation;
}): React.ReactElement {
  const complete = valuation.totalValue !== null
    && valuation.cashValue !== null
    && valuation.holdingsValue !== null;
  return (
    <section className="portfolio-valuation" aria-label={`${currency} 原币种资产估值`}>
      <header className="portfolio-valuation__heading">
        <div><p className="eyebrow">原币种估值</p><h3>资产概览</h3></div>
        <span className="quality-badge" data-quality={snapshot.status}>{snapshotStatusLabel(snapshot.status)} <code>{snapshot.status}</code></span>
      </header>
      <dl className="portfolio-valuation__facts">
        <Fact term="总资产" value={complete ? `${currency} ${formatMoney(valuation.totalValue!)}` : "无法计算"} />
        <Fact term="持仓市值" value={complete ? `${currency} ${formatMoney(valuation.holdingsValue!)}` : "无法计算"} />
        <Fact term="现金" value={complete ? `${currency} ${formatMoney(valuation.cashValue!)}` : "无法计算"} />
        <Fact term="数据来源" value={provider === "fosun" ? "复星（只读）" : provider} />
        <Fact term="质量状态" value={`${snapshotStatusLabel(snapshot.status)} · ${snapshot.status}`} />
        <Fact term={valuation.asOfKind} value={formatTime(valuation.asOf)} />
      </dl>
      {complete && valuation.totalValue! > BIGINT_ZERO ? (
        <div className="portfolio-allocation">
          <h4>配置结构</h4>
          <ul>
            <AllocationLine label="现金" value={valuation.cashValue!} total={valuation.totalValue!} currency={currency} />
            {valuation.positionValues.map(({ position, value }) => value === null ? null : (
              <AllocationLine
                key={position.instrument_id}
                label={position.symbol ?? "未知标的"}
                value={value}
                total={valuation.totalValue!}
                currency={currency}
              />
            ))}
          </ul>
        </div>
      ) : complete ? (
        <div className="portfolio-allocation">
          <h4>配置结构</h4>
          <p className="portfolio-summary__empty">配置结构无法计算：原币种总资产为零。</p>
        </div>
      ) : (
        <p className="inline-notice inline-notice--error">
          总资产无法计算。{valuation.missingFacts.join("；")}。上一份冻结事实保持可见，不进行部分汇总或 FX 折算。
        </p>
      )}
      <p className="portfolio-valuation__note">仅按复星冻结快照中的原币种账面现金、持仓数量与现价计算；不进行跨币种折算，也不构成投资建议。</p>
    </section>
  );
}

function AllocationLine({ label, value, total, currency }: { label: string; value: bigint; total: bigint; currency: string }): React.ReactElement {
  return (
    <li>
      <span>{label}</span>
      <span className="numeric-data">{currency} {formatMoney(value)}</span>
      <strong className="numeric-data">{label} {formatPercent(value, total)}</strong>
    </li>
  );
}

function currencyValuation(
  snapshot: PortfolioSnapshot,
  cash: PortfolioSnapshot["cash"][number] | null,
  positions: Position[],
): CurrencyValuation {
  const cashValue = cash ? parseFixedDecimal(cash.cash_balance) : null;
  const missingFacts: string[] = [];
  if (cashValue === null) missingFacts.push("缺失现金事实");
  const positionValues = positions.map((position): PositionValuation => {
    const quantity = parseFixedDecimal(position.quantity);
    const price = position.current_price === null ? null : parseFixedDecimal(position.current_price);
    if (quantity === null) missingFacts.push(`数量无效：${position.symbol ?? "未知标的"}`);
    if (price === null) missingFacts.push(`缺失现价：${position.symbol ?? "未知标的"}`);
    return { position, value: quantity === null || price === null ? null : multiplyFixed(quantity, price) };
  });
  const complete = cashValue !== null && positionValues.every((item) => item.value !== null);
  const holdingsValue = complete
    ? positionValues.reduce((total, item) => total + item.value!, BIGINT_ZERO)
    : null;
  const totalValue = complete && holdingsValue !== null ? cashValue + holdingsValue : null;
  const providerTimes = [cash?.provider_as_of, ...positions.map((position) => position.provider_as_of)];
  const completeProviderTimes = providerTimes.length > 0 && providerTimes.every((value) => value !== null);
  const asOf = completeProviderTimes
    ? providerTimes.filter((value): value is string => value !== null).sort()[0]
    : snapshot.as_of;
  return {
    cashValue,
    positionValues,
    holdingsValue,
    totalValue,
    missingFacts,
    asOf,
    asOfKind: completeProviderTimes ? "Provider 截止时间" : "快照截止时间",
  };
}

function isPersistedUsMarketHolding(position: Position): position is Position & { symbol: string } {
  return position.exchange === "XNYS"
    && position.symbol !== null
    && /^[A-Z][A-Z0-9.-]{0,14}$/.test(position.symbol);
}

function parseFixedDecimal(value: string): bigint | null {
  const match = /^(-?)(\d+)(?:\.(\d{1,12}))?$/.exec(value);
  if (!match) return null;
  const fraction = (match[3] ?? "").padEnd(DECIMAL_PLACES, "0");
  const magnitude = BigInt(match[2]) * DECIMAL_FACTOR + BigInt(fraction || "0");
  return match[1] === "-" ? -magnitude : magnitude;
}

function multiplyFixed(left: bigint, right: bigint): bigint {
  return roundDivide(left * right, DECIMAL_FACTOR);
}

function formatMoney(value: bigint): string {
  const cents = roundDivide(value * BIGINT_HUNDRED, DECIMAL_FACTOR);
  const negative = cents < BIGINT_ZERO;
  const magnitude = negative ? -cents : cents;
  const whole = (magnitude / BIGINT_HUNDRED).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return `${negative ? "-" : ""}${whole}.${(magnitude % BIGINT_HUNDRED).toString().padStart(2, "0")}`;
}

function formatPercent(value: bigint, total: bigint): string {
  if (total <= BIGINT_ZERO) return "无法计算";
  const hundredths = roundDivide(value * PERCENT_HUNDREDTHS_FACTOR, total);
  const negative = hundredths < BIGINT_ZERO;
  const magnitude = negative ? -hundredths : hundredths;
  return `${negative ? "-" : ""}${magnitude / BIGINT_HUNDRED}.${(magnitude % BIGINT_HUNDRED).toString().padStart(2, "0")}%`;
}

function roundDivide(value: bigint, divisor: bigint): bigint {
  const negative = value < BIGINT_ZERO;
  const magnitude = negative ? -value : value;
  const rounded = (magnitude + divisor / BIGINT_TWO) / divisor;
  return negative ? -rounded : rounded;
}

function Fact({ term, value }: { term: string; value: string }): React.ReactElement {
  return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>;
}

function snapshotStatusLabel(status: PortfolioSnapshot["status"]): string {
  return status === "READY" ? "已就绪" : status === "STALE" ? "已过期" : "需要对账";
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
