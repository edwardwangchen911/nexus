import { formatPercent } from "../lib/display-format";
import { statusLabel, reasonText } from "../lib/display-labels";
import { formatDisplayTime } from "../lib/display-format";
import { formatDisplayDecimal } from "../lib/display-decimal";
import type { components } from "../lib/generated/schema";

type Inputs = components["schemas"]["ResearchInputsSummary"];

const factLabels: Record<string, string> = {
  revenue: "收入", gross_profit: "毛利润", operating_income: "营业利润", net_income: "净利润",
  operating_cash_flow: "经营现金流", capital_expenditure: "资本支出", total_assets: "总资产",
  total_liabilities: "总负债", stockholders_equity: "股东权益", diluted_shares: "稀释股数", diluted_eps: "稀释每股收益",
};

export function ResearchInputs({ inputs, asOf }: { inputs: Inputs; asOf: string }): React.ReactElement {
  const distributions = inputs.issuer_distributions ?? [];
  const hasIssuer = distributions.length > 0;
  const hasSec = inputs.filings.length > 0;
  const documentLabel = hasIssuer ? (hasSec ? "已冻结研究资料" : "已冻结发行商资料") : inputs.filings.some((filing) => filing.form === "497K") ? "已冻结基金资料" : inputs.filings.some((filing) => filing.form === "6-K") ? "已冻结披露" : "已冻结财报";
  return <article className="decision-card" aria-labelledby="research-inputs-title">
    <header className="decision-card__heading">
      <div><p className="eyebrow">{hasIssuer ? (hasSec ? "SEC / 发行商" : "发行商") : "SEC"} · 本地只读证据</p><h2 id="research-inputs-title">{inputs.display_symbol ?? "标的"} · {documentLabel}</h2></div>
      <span className="mode-badge">资料核验 · 非 AI 结论</span>
    </header>
    <dl className="decision-card__authority">
      <div><dt>冻结截止</dt><dd>{formatUtc(asOf)}</dd></div>
      {hasSec || !hasIssuer ? <>
        <div><dt>SEC 资料</dt><dd>{inputs.filings.length} 份</dd></div>
        <div><dt>财务事实</dt><dd>{inputs.total_facts} 条</dd></div>
      </> : null}
      {hasIssuer ? <div><dt>分红观察</dt><dd>{distributions.length} 条</dd></div> : null}
      <div><dt>使用边界</dt><dd>未形成买卖判断，不触发交易</dd></div>
    </dl>
    {inputs.truncated ? <p role="note">当前仅展示部分冻结{hasIssuer ? " SEC " : ""}资料，不代表完整覆盖。</p> : null}
    {inputs.portfolio_context ? <section className="decision-card__section">
      {inputs.portfolio_context.research_focus ? <section aria-label="本次组合研究问题">
        <h3>本次组合研究问题</h3>
        <p>{inputs.portfolio_context.research_focus.question}</p>
        <p>依据同一资产快照的集中度诊断。证券占比 {formatPercent(inputs.portfolio_context.research_focus.observed_weight)}，预警阈值 {formatPercent(inputs.portfolio_context.research_focus.reference_limit)}；均为同币种证券内比例，不含现金。</p>
        <p>预警触发研究，不代表已决定减仓。政策版本：{inputs.portfolio_context.research_focus.policy_version}。</p>
      </section> : null}
      <h3>此持仓的冻结组合背景</h3>
      <p>复星已入库快照，不是实时仓位；仅提供研究背景，执行保持锁定。</p>
      <dl className="decision-card__authority">
        <div><dt>资产快照时间</dt><dd>{formatUtc(inputs.portfolio_context.snapshot_as_of)}</dd></div>
        <div><dt>资产占比（含现金）</dt><dd>{inputs.portfolio_context.position_weight === null ? "估值资料不完整" : `${formatPercent(inputs.portfolio_context.position_weight)}（${inputs.portfolio_context.currency} 资产内）`}</dd></div>
        <div><dt>同币种现金占比</dt><dd>{inputs.portfolio_context.cash_weight === null ? "估值资料不完整" : formatPercent(inputs.portfolio_context.cash_weight)}</dd></div>
        <div><dt>该快照对账</dt><dd>{inputs.portfolio_context.snapshot_decision_ready ? "已通过；不代表研究证据齐备" : "未通过；不可作为正式决策依据"}</dd></div>
        <div><dt>确定性诊断</dt><dd>{inputs.portfolio_context.optimization_need ? statusLabel(inputs.portfolio_context.optimization_need) : "尚无正式诊断"}</dd></div>
      </dl>
      {inputs.portfolio_context.diagnostics.filter((item) => item.unknown_reason).map((item) => <p key={item.code}>{reasonText(item.unknown_reason)}</p>)}
      {inputs.portfolio_context.diagnostics_truncated ? <p>诊断项仅展示前 100 项。</p> : null}
      {inputs.portfolio_context.proposal_comparison ? <p>
        此次研究已包含同一基线的方案比较，冻结于 {formatUtc(inputs.portfolio_context.proposal_comparison.frozen_at)}。
        <a href={`/optimize/${inputs.portfolio_context.proposal_comparison.proposal_group_id}`}>查看方案详情</a>
      </p> : null}
      <details className="portfolio-disclosure"><summary>组合背景证据</summary>
        <dl className="decision-card__versions"><div><dt>快照标识</dt><dd>{inputs.portfolio_context.portfolio_snapshot_id}</dd></div><div><dt>内容 SHA-256</dt><dd>{inputs.portfolio_context.content_hash}</dd></div></dl>
      </details>
    </section> : null}
    {distributions.map((distribution) => <section className="decision-card__section" aria-label="发行商现金分红资料" key={distribution.evidence_ref_id}>
      <h3>发行商现金分红资料</h3>
      <p>SSGA 官方资料 · 仅展示该冻结集合内最新一条分红观察，不代表完整分红历史。</p>
      <dl className="decision-card__authority">
        <div><dt>除息日</dt><dd>{distribution.ex_date}</dd></div>
        <div><dt>登记日</dt><dd>{distribution.record_date}</dd></div>
        <div><dt>发行商支付日</dt><dd>{distribution.payable_date}</dd></div>
        <div><dt>现金分红</dt><dd>{formatDistributionAmount(distribution.dividend_per_share, distribution.currency)}</dd></div>
        <div><dt>短期资本利得</dt><dd>{formatDistributionAmount(distribution.short_term_capital_gain, distribution.currency)}</dd></div>
        <div><dt>长期资本利得</dt><dd>{formatDistributionAmount(distribution.long_term_capital_gain, distribution.currency)}</dd></div>
        <div><dt>本机收到时间</dt><dd>{formatUtc(distribution.received_at)}</dd></div>
        <div><dt>证据可用时间</dt><dd>{formatUtc(distribution.available_at)}</dd></div>
      </dl>
      <p>金额为每份基金份额，最多显示 3 位小数；舍入为零的非零金额显示范围，精确数值请查看原始来源。不推算年化收益、总回报或费用，不代表账户实际到账。</p>
      <p>收到／可用时间不是历史公告时间。本条仅供当前研究观察，不可用于历史回测；不代表研究已完成，也不授予执行权限。</p>
      <details className="portfolio-disclosure"><summary>分红证据与来源</summary>
        <dl className="decision-card__versions">
          <div><dt>证据标识</dt><dd>{distribution.evidence_ref_id}</dd></div>
          <div><dt>版本</dt><dd>{distribution.revision}</dd></div>
          <div><dt>内容 SHA-256</dt><dd>{distribution.content_hash}</dd></div>
          <div><dt>现金分红（展示值）</dt><dd>{formatDistributionAmount(distribution.dividend_per_share, distribution.currency)}</dd></div>
          <div><dt>短期资本利得（展示值）</dt><dd>{formatDistributionAmount(distribution.short_term_capital_gain, distribution.currency)}</dd></div>
          <div><dt>长期资本利得（展示值）</dt><dd>{formatDistributionAmount(distribution.long_term_capital_gain, distribution.currency)}</dd></div>
        </dl>
        <a href={distribution.source_url} target="_blank" rel="noreferrer">查看 SSGA 官方原件</a>
      </details>
    </section>)}
    {inputs.filings.map((filing) => <section className="decision-card__section" key={filing.evidence_ref_id}>
      <h3>SEC {filing.form}</h3>
      {filing.form === "497K" ? <p>ETF 摘要招募书：用于研究基金目标、费用与风险，不是公司财报，也不是基金最新持仓。</p> : null}
      <dl className="decision-card__authority">
        <div><dt>申报编号</dt><dd>{filing.accession_number}</dd></div>
        <div><dt>SEC 接受时间</dt><dd>{formatUtc(filing.accepted_at)}</dd></div>
        <div><dt>本机入库时间</dt><dd>{formatUtc(filing.received_at)}</dd></div>
      </dl>
      <details className="portfolio-disclosure"><summary>查看证据版本与哈希</summary>
        <dl className="decision-card__versions"><div><dt>版本</dt><dd>{filing.revision}</dd></div><div><dt>内容 SHA-256</dt><dd>{filing.content_hash}</dd></div></dl>
      </details>
      {filing.disclosure_excerpt ? <>
        <p>文本披露，不是结构化财务指标；不代表完整覆盖，不据此推导财务数值。</p>
        <details className="portfolio-disclosure"><summary>查看冻结披露正文{filing.disclosure_truncated ? "（节选）" : ""}</summary>
          <p style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>{filing.disclosure_excerpt}</p>
          {filing.disclosure_truncated ? <p>正文超出展示上限，完整原文请查看 SEC 来源。</p> : null}
          {filing.source_url ? <a href={filing.source_url} target="_blank" rel="noreferrer">查看 SEC 原文</a> : null}
        </details>
      </> : ["6-K", "497K"].includes(filing.form) ? <p>披露正文暂不可用，不作为已读取的文本证据。</p> : null}
      {inputs.facts.some((fact) => fact.evidence_ref_id === filing.evidence_ref_id) ? <>
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- The overflow table needs keyboard focus for horizontal scrolling. */}
      <div className="table-scroll" role="region" aria-label={`${filing.form} 财务事实，可横向滚动`} tabIndex={0}>
        <table className="market-table numeric-data">
          <caption>财务数值（最多 3 位小数）· 保留原单位与期间，未年化、未合并不同期间</caption>
          <thead><tr><th scope="col">指标</th><th scope="col">数值</th><th scope="col">单位</th><th scope="col">统计期间 / 时点</th><th scope="col">原始标签</th></tr></thead>
          <tbody>{inputs.facts.filter((fact) => fact.evidence_ref_id === filing.evidence_ref_id).map((fact) => <tr key={fact.id}>
            <th scope="row">{factLabels[fact.fact] ?? fact.fact}</th>
            <td>{formatDisplayDecimal(fact.value)}</td><td>{fact.unit}</td>
            <td>{fact.period_kind === "duration" ? `${fact.period_start} 至 ${fact.period_end}` : `${fact.period_end}（时点）`}</td>
            <td>{fact.raw_tag}</td>
          </tr>)}</tbody>
        </table>
      </div>
      </> : <p>尚无结构化财务指标。</p>}
    </section>)}
  </article>;
}

function formatUtc(value: string | null | undefined): string { return formatDisplayTime(value); }

function formatDistributionAmount(value: string | null, currency: string): string {
  if (value === null) return "未知";
  return `${currency} ${formatDisplayDecimal(value)} / 每份`;
}
