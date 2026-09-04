import { statusLabel } from "../lib/display-labels";
import { formatDisplayTime, snapshotTimeLabel } from "../lib/display-format";
import { formatDisplayDecimal } from "../lib/display-decimal";
import { CircleDollarSign, LockKeyhole } from "lucide-react";
import type { PortfolioItem, PortfolioSnapshot } from "../(protected)/portfolio/page";

/* eslint-disable jsx-a11y/no-noninteractive-tabindex -- Overflow data tables need keyboard focus for horizontal scrolling. */

type PortfolioSummaryProps = {
  portfolio: PortfolioItem;
  snapshot: PortfolioSnapshot;
  currency: string;
  showValuation?: boolean;
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
  asOfKind: string;
};

const DECIMAL_PLACES = 12;
const DECIMAL_FACTOR = BigInt(10) ** BigInt(DECIMAL_PLACES);
const BIGINT_ZERO = BigInt(0);
const BIGINT_TWO = BigInt(2);
const BIGINT_HUNDRED = BigInt(100);
const PERCENT_HUNDREDTHS_FACTOR = BigInt(10_000);

export function PortfolioSummary({ portfolio, snapshot, currency, showValuation = true }: PortfolioSummaryProps): React.ReactElement {
  const cash = snapshot.cash.find((bucket) => bucket.currency === currency) ?? null;
  const positions = snapshot.positions
    .filter((position) => position.currency === currency)
    .sort((left, right) => (left.symbol ?? left.instrument_id).localeCompare(right.symbol ?? right.instrument_id));
  const orders = snapshot.open_orders.filter((order) => order.currency === currency);
  const containsHongKongHolding = positions.some((position) => position.symbol === "00656.HK");
  const valuation = currencyValuation(snapshot, cash, positions);
  const cashTime = cash ? cash.provider_as_of ?? snapshot.as_of : null;
  const cashTimeLabel = cash?.provider_as_of ? "数据源更新时间"
    : snapshot.time_basis === "gateway_observed" ? "本机读取时间"
      : snapshot.time_basis === "provider_event" ? "快照截止时间" : "快照时间（含义未确认）";
  const researchPositions = positions.filter((position) => position.symbol !== "00656.HK");
  const completeResearch = researchPositions.filter((position) => position.research?.status === "completed" && position.research.quality_status === "complete").length;
  const partialResearch = researchPositions.filter((position) => position.research?.status === "completed" && position.research.quality_status === "partial").length;

  if (!showValuation && positions.length === 0 && orders.length === 0 && cash) {
    return <section className="portfolio-summary portfolio-summary--cash-only" aria-label={`${portfolio.name} ${currency} 组合摘要`}>
      <header><h2>{currency} 现金</h2><span className="portfolio-summary__empty">无持仓 · 无未结订单 · 只读</span></header>
      <dl className="portfolio-summary__cash">
        <Fact term="账面现金" value={`${currency} ${formatDecimal(cash.cash_balance)}`} />
        <Fact term="可用现金" value={`${currency} ${formatDecimal(cash.available_cash)}`} />
        <Fact term={cashTimeLabel} value={formatTime(cashTime)} />
      </dl>
    </section>;
  }

  return (
    <section className="portfolio-summary" aria-label={`${portfolio.name} ${currency} 组合摘要`}>
      <header className="portfolio-summary__heading">
        <div>
          <p className="eyebrow">原币种账户</p>
          <h2>{currency}</h2>
        </div>
        <span className="mode-badge"><LockKeyhole aria-hidden="true" size={15} />只读</span>
      </header>

      {showValuation ? <CurrencyValuationSummary
        currency={currency}
        provider={portfolio.provider}
        snapshot={snapshot}
        valuation={valuation}
      /> : null}

      <details className="portfolio-disclosure">
      <summary>现金明细与更新时间</summary>
      <dl className="portfolio-summary__cash" aria-label={`${currency} 现金事实`}>
        <Fact term="账面现金" value={cash ? `${currency} ${formatDecimal(cash.cash_balance)}` : "无数据"} />
        <Fact term="可用现金" value={cash ? `${currency} ${formatDecimal(cash.available_cash)}` : "无数据"} />
        <Fact term={cashTimeLabel} value={formatTime(cashTime)} />
      </dl>
      </details>

      <div className="portfolio-summary__section">
        <h3>持仓</h3>
        {researchPositions.some((position) => position.research) ? <p className="portfolio-evidence-note">
          AI 研究输出已生成 {completeResearch + partialResearch} / {researchPositions.length} 个可研究持仓；其中证据完整 {completeResearch}，证据不完整 {partialResearch}。
          研究按各自冻结时点展示，不等于当前组合诊断通过；交易保持锁定。打开报告不会调用模型。
          状态读取：{formatTime(positions.find((position) => position.research)?.research?.read_at ?? null)}。
        </p> : null}
        {positions.length === 0 ? <p className="portfolio-summary__empty">该原币种没有持仓事实。</p> : (
          <div className="table-scroll" tabIndex={0} role="region" aria-label={`${currency} 持仓表，可横向滚动`}>
            <table className="portfolio-table portfolio-table--holdings">
              <caption>{portfolio.name} · {currency} · 占比以同币种现金与证券总市值为分母；数值最多显示 3 位小数。</caption>
              <thead><tr><th>标的</th><th>持仓市值</th><th>资产占比（含现金）</th><th>现价</th><th>总量</th><th>AI 研究</th><th>账户明细</th></tr></thead>
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
                    {parseFixedDecimal(position.frozen_quantity) !== BIGINT_ZERO ? <small className="portfolio-frozen">冻结 {formatDecimal(position.frozen_quantity)}</small> : null}
                  </th>
                  <td>{value === null ? "无法计算" : `${currency} ${formatMoney(value)}`}</td>
                  <td>{value !== null && valuation.totalValue !== null ? formatPercent(value, valuation.totalValue) : "无法计算"}</td>
                  <td>{formatNullableDecimal(position.current_price, currency)}</td>
                  <td>{formatDecimal(position.quantity)}</td>
                  <td><PositionResearch position={position} /></td>
                  <td><details className="portfolio-disclosure portfolio-position-details">
                    <summary>成本与数量 · {position.read_only ? "只读" : "权限未知"}</summary>
                    <dl>
                      <Fact term="平均成本" value={formatNullableDecimal(position.avg_cost, currency)} />
                      <Fact term="摊薄成本" value={formatNullableDecimal(position.diluted_cost, currency)} />
                      <Fact term="可用" value={formatDecimal(position.available_quantity)} />
                      <Fact term="冻结" value={formatDecimal(position.frozen_quantity)} />
                    </dl>
                  </details></td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
      </div>

      <div className="portfolio-summary__section" data-empty={orders.length === 0}>
        <h3>未结订单快照</h3>
        {orders.length === 0 ? <p className="portfolio-summary__empty">该原币种没有未结订单事实。</p> : (
          <div className="table-scroll" tabIndex={0} role="region" aria-label={`${currency} 未结订单表，可横向滚动`}>
            <table className="portfolio-table">
              <caption>仅展示脱敏内部快照 ID</caption>
              <thead><tr><th>标的</th><th>方向</th><th>数量</th><th>已成交</th><th>价格</th><th>状态</th><th>权限</th></tr></thead>
              <tbody>{orders.map((order) => (
                <tr key={order.broker_order_snapshot_id}>
                  <th scope="row">{order.symbol ?? "未知标的"}</th>
                  <td>{statusLabel(order.side)}</td><td>{formatDecimal(order.quantity)}</td>
                  <td>{formatDecimal(order.filled_quantity)}</td>
                  <td>{formatNullableDecimal(order.price, currency)}</td>
                  <td>{statusLabel(order.status)}</td><td>{order.read_only ? "只读" : "未知"}</td>
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

function PositionResearch({ position }: { position: Position }): React.ReactElement {
  const research = position.research;
  if (position.symbol === "00656.HK") return <span>只读展示，不生成基本面判断</span>;
  if (!research) return <span>研究状态未读取</span>;
  const labels = {
    not_started: research.data_snapshot_id ? "研究资料已冻结 · 待研究" : "研究证据待补齐",
    prepared: "研究已登记 · 待执行", running: "研究执行中", validated: "研究核验中",
    audited: "证据审计中", synthesized: "研究汇总中", completed: research.quality_status === "complete"
      ? "研究输出已生成 · 证据完整" : research.quality_status === "partial" ? "研究输出已生成 · 证据不完整" : "结果待核验",
    failed: "研究失败 · 保持观望", abstained: "证据不足 · 未形成研究",
  } as const;
  const href = research.run_id
    ? `/research/${encodeURIComponent(position.instrument_id)}?run_id=${encodeURIComponent(research.run_id)}`
    : research.data_snapshot_id && research.decision_time
    ? `/research/${encodeURIComponent(position.instrument_id)}?as_of=${encodeURIComponent(research.decision_time)}` : null;
  const updatedInputsHref = research.newer_inputs_available
    ? `/research/${encodeURIComponent(position.instrument_id)}?as_of=${encodeURIComponent(research.read_at)}` : null;
  const linkLabel = research.run_id ? research.status === "completed" ? "研究报告" : "研究状态" : "研究资料";
  return <div className="portfolio-position-research">
    <span>{labels[research.status]}</span>
    {research.newer_inputs_available ? <small>已有更新的冻结资料，当前报告未使用</small> : null}
    {href ? <a className="portfolio-market-link" href={href} aria-label={`查看 ${position.symbol ?? "该持仓"} ${linkLabel}`}>
      查看{linkLabel}
    </a> : <small>尚无可引用的研究快照</small>}
    {updatedInputsHref ? <a className="portfolio-market-link" href={updatedInputsHref} aria-label={`查看 ${position.symbol ?? "该持仓"} 更新资料`}>查看更新资料</a> : null}
    {research.run_id ? <small>研究截止 {formatTime(research.decision_time ?? null)}</small> : null}
    {research.summary || research.uncertainties.length ? <details className="portfolio-disclosure">
      <summary>摘要与证据缺口</summary>
      {research.summary ? <p>{research.summary}</p> : null}
      {research.uncertainties.map((item, index) => <p key={index}>{item === "Price authority is unavailable." ? "缺少合格价格证据，不能生成仓位动作。" : item}</p>)}
    </details> : null}
  </div>;
}

export function PortfolioValuationSummary({
  provider,
  snapshot,
  currency,
  compact = false,
}: {
  provider: string;
  snapshot: PortfolioSnapshot;
  currency: string;
  compact?: boolean;
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
    compact={compact}
  />;
}

function CurrencyValuationSummary({
  currency,
  provider,
  snapshot,
  valuation,
  compact = false,
}: {
  currency: string;
  provider: string;
  snapshot: PortfolioSnapshot;
  valuation: CurrencyValuation;
  compact?: boolean;
}): React.ReactElement {
  const complete = valuation.totalValue !== null
    && valuation.cashValue !== null
    && valuation.holdingsValue !== null;
  return (
    <section className="portfolio-valuation" data-compact={compact} aria-label={`${currency} 原币种资产估值`}>
      <header className="portfolio-valuation__heading">
        <div><p className="eyebrow">原币种估值 · 只读</p><h3>{compact ? currency : "资产概览"}</h3></div>
        <span className="quality-badge" data-quality={snapshot.status}>{snapshotStatusLabel(snapshot.status)}</span>
      </header>
      <dl className="portfolio-valuation__facts">
        <Fact term="总资产" value={complete ? `${currency} ${formatMoney(valuation.totalValue!)}` : "无法计算"} />
        <Fact term="持仓市值" value={complete ? `${currency} ${formatMoney(valuation.holdingsValue!)}` : "无法计算"} />
        <Fact term="现金" value={complete ? `${currency} ${formatMoney(valuation.cashValue!)}` : "无法计算"} />
        {!compact ? <><Fact term="数据来源" value={provider === "fosun" ? "复星（只读）" : provider} />
        <Fact term="质量状态" value={snapshotStatusLabel(snapshot.status)} />
        <Fact term={valuation.asOfKind} value={formatTime(valuation.asOf)} /></> : null}
      </dl>
      <details className="portfolio-disclosure portfolio-valuation__detail" open={compact ? undefined : true}>
      <summary>配置、来源与估值口径</summary>
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
      ) : null}
      {compact ? <>
        <dl className="portfolio-valuation__facts">
          <Fact term="数据来源" value={provider === "fosun" ? "复星（只读）" : provider} />
          <Fact term={valuation.asOfKind} value={formatTime(valuation.asOf)} />
        </dl>
        <p className="portfolio-valuation__note">冻结事实 · 原币种独立估值，不进行跨币种折算，也不构成投资建议。</p>
      </> : <p className="portfolio-valuation__note">仅按复星冻结快照中的原币种账面现金、持仓数量与现价计算；不进行跨币种折算，也不构成投资建议。</p>}
      </details>
      {!complete ? <p className="inline-notice inline-notice--error">总资产无法计算。{valuation.missingFacts.join("；")}。上一份冻结事实保持可见，不进行部分汇总或 FX 折算。</p> : null}
    </section>
  );
}

function AllocationLine({ label, value, total, currency }: { label: string; value: bigint; total: bigint; currency: string }): React.ReactElement {
  return (
    <li>
      <span>{label}</span>
      <span className="numeric-data">{currency} {formatMoney(value)}</span>
      <strong className="numeric-data">{formatPercent(value, total)}</strong>
      {value >= BIGINT_ZERO && value <= total && total > BIGINT_ZERO ? <span className="portfolio-allocation__meter" aria-hidden="true"><span style={{ inlineSize: formatPercent(value, total, true) }} /></span> : null}
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
    asOfKind: completeProviderTimes ? "数据源截止时间" : snapshot.time_basis === "gateway_observed" ? "快照读取时间" : snapshotTimeLabel(snapshot.time_basis),
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
  const magnitude = value < BIGINT_ZERO ? -value : value;
  const decimal = `${value < BIGINT_ZERO ? "-" : ""}${magnitude / DECIMAL_FACTOR}.${(magnitude % DECIMAL_FACTOR).toString().padStart(DECIMAL_PLACES, "0")}`;
  return formatDisplayDecimal(decimal, { group: true, fixedDigits: 2 });
}

function formatPercent(value: bigint, total: bigint, forChart = false): string {
  if (total <= BIGINT_ZERO) return "无法计算";
  const hundredths = roundDivide(value * PERCENT_HUNDREDTHS_FACTOR, total);
  if (!forChart && hundredths === BIGINT_ZERO && value !== BIGINT_ZERO) {
    return value < BIGINT_ZERO ? "(-0.01, 0)%" : "<0.01%";
  }
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
  return value === null ? "未知" : `${currency} ${formatDecimal(value)}`;
}

function formatDecimal(value: string): string {
  return formatDisplayDecimal(value, { group: true });
}

function formatTime(value: string | null | undefined): string { return formatDisplayTime(value); }
