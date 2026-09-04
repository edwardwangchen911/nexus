import { formatDisplayDecimal } from "../lib/display-decimal";
import { formatDisplayTime, formatPercent } from "../lib/display-format";
import type { components } from "../lib/generated/schema";

type Trend = components["schemas"]["ResearchInputPriceTrend"];

export function ResearchPriceTrend({ trend }: { trend: Trend }): React.ReactElement {
  const { features } = trend;
  const price = (value: string | null) => value === null ? "资料不足" : `${formatDisplayDecimal(value)} USD`;
  const missingFeatures = Object.values(features).some((value) => value === null);
  return <section className="decision-card__section" aria-label="拆股调整价格趋势">
    <h3>拆股调整价格趋势</h3>
    <p>Alpaca SIP 合并行情 · 拆股调整日线 · 本次实收的历史价格资料，不是实时行情，也不证明历史当时已知这些价格；拆股调整不是总收益。</p>
    <p>{trend.missing_sessions_total > 0 ? "查询窗口存在缺日。" : "查询窗口内未发现缺失交易日，不代表窗口之外覆盖完整。"}{missingFeatures ? "部分指标资料不足，缺失不补零。" : "以下指标仅为价格观察，不代表研究结论通过。"}指标仅使用最后一个连续已覆盖交易日片段，不跨缺日串接收益。</p>
    <dl className="decision-card__authority">
      <div><dt>末观察日拆股调整收盘价</dt><dd className="numeric-data">{price(features.close)}</dd></div>
      <div><dt>20 日均线</dt><dd className="numeric-data">{price(features.sma_20)}</dd></div>
      <div><dt>200 日均线</dt><dd className="numeric-data">{price(features.sma_200)}</dd></div>
      <div><dt>RSI14（0–100）</dt><dd className="numeric-data">{features.rsi_14 === null ? "资料不足" : formatDisplayDecimal(features.rsi_14)}</dd></div>
      <div><dt>20 日样本年化价格波动率</dt><dd className="numeric-data">{features.volatility_20 === null ? "资料不足" : formatPercent(features.volatility_20)}</dd></div>
      <div><dt>查询交易日范围</dt><dd>{trend.query_start} 至 {trend.query_end}</dd></div>
      <div><dt>实际观察日期范围</dt><dd>{trend.first_observed_on === null || trend.last_observed_on === null ? "无观察" : `${trend.first_observed_on} 至 ${trend.last_observed_on}`}</dd></div>
      <div><dt>观察／预期交易日</dt><dd>{trend.observed_sessions} / {trend.expected_sessions}</dd></div>
      <div><dt>末段连续覆盖</dt><dd>{trend.contiguous_tail_sessions} 个交易日</dd></div>
      <div><dt>本机收到时间</dt><dd>{formatDisplayTime(trend.received_at)}</dd></div>
      <div><dt>证据可用时间</dt><dd>{formatDisplayTime(trend.available_at)}</dd></div>
    </dl>
    <p>均线分别需要 20／200 个连续收盘，RSI14 需要 15 个，价格波动需要 21 个。年化波动按 20 日简单价格收益率的总体标准差乘 √252，不是期间涨跌幅或总收益波动。</p>
    <p>最多显示 3 位小数，极小非零值显示范围；原始精度不变。资料不授予历史回测、估值或交易价格资格，不形成买卖判断，执行保持锁定。</p>
    <details className="portfolio-disclosure">
      <summary>趋势证据、身份与缺口</summary>
      <dl className="decision-card__versions">
        <div><dt>身份映射日期</dt><dd>{trend.identity_asof}</dd></div>
        <div><dt>缺失交易日总数</dt><dd>{trend.missing_sessions_total}</dd></div>
        <div><dt>缺失交易日</dt><dd>{trend.missing_sessions.length ? trend.missing_sessions.join("、") : "无"}{trend.missing_sessions_truncated ? "（仅展示前 20 个）" : ""}</dd></div>
        <div><dt>证据标识</dt><dd>{trend.evidence_ref_id}</dd></div>
        <div><dt>本地内容版本</dt><dd>{trend.revision}</dd></div>
        <div><dt>内容 SHA-256</dt><dd>{trend.content_hash}</dd></div>
      </dl>
      <p>身份映射日期不是行情知识截止。收件与可用时间不回填历史，版本是本地内容版本，不是来源发布序号。</p>
    </details>
  </section>;
}
