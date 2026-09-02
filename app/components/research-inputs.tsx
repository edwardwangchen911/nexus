import type { components } from "../lib/generated/schema";

type Inputs = components["schemas"]["ResearchInputsSummary"];

const factLabels: Record<string, string> = {
  revenue: "收入", gross_profit: "毛利润", operating_income: "营业利润", net_income: "净利润",
  operating_cash_flow: "经营现金流", capital_expenditure: "资本支出", total_assets: "总资产",
  total_liabilities: "总负债", stockholders_equity: "股东权益", diluted_shares: "稀释股数", diluted_eps: "稀释每股收益",
};

export function ResearchInputs({ inputs, asOf }: { inputs: Inputs; asOf: string }): React.ReactElement {
  return <article className="decision-card" aria-labelledby="research-inputs-title">
    <header className="decision-card__heading">
      <div><p className="eyebrow">SEC · 本地只读证据</p><h2 id="research-inputs-title">{inputs.display_symbol ?? "标的"} · 已冻结财报</h2></div>
      <span className="mode-badge">资料核验 · 非 AI 结论</span>
    </header>
    <dl className="decision-card__authority">
      <div><dt>冻结截止</dt><dd>{formatUtc(asOf)}</dd></div>
      <div><dt>财报</dt><dd>{inputs.filings.length} 份</dd></div>
      <div><dt>财务事实</dt><dd>{inputs.total_facts} 条</dd></div>
      <div><dt>使用边界</dt><dd>未形成买卖判断，不触发交易</dd></div>
    </dl>
    {inputs.truncated ? <p role="note">当前仅展示部分冻结资料，不代表完整覆盖。</p> : null}
    {inputs.filings.map((filing) => <section className="decision-card__section" key={filing.evidence_ref_id}>
      <h3>SEC {filing.form}</h3>
      <dl className="decision-card__authority">
        <div><dt>申报编号</dt><dd>{filing.accession_number}</dd></div>
        <div><dt>SEC 接受时间</dt><dd>{formatUtc(filing.accepted_at)}</dd></div>
        <div><dt>本机入库时间</dt><dd>{formatUtc(filing.received_at)}</dd></div>
      </dl>
      <details className="portfolio-disclosure"><summary>查看证据版本与哈希</summary>
        <dl className="decision-card__versions"><div><dt>版本</dt><dd>{filing.revision}</dd></div><div><dt>内容 SHA-256</dt><dd>{filing.content_hash}</dd></div></dl>
      </details>
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- The overflow table needs keyboard focus for horizontal scrolling. */}
      <div className="table-scroll" role="region" aria-label={`${filing.form} 财务事实，可横向滚动`} tabIndex={0}>
        <table className="market-table numeric-data">
          <caption>财务原始值 · 保留原单位与期间，未年化、未合并不同期间</caption>
          <thead><tr><th scope="col">指标</th><th scope="col">数值</th><th scope="col">单位</th><th scope="col">统计期间 / 时点</th><th scope="col">原始标签</th></tr></thead>
          <tbody>{inputs.facts.filter((fact) => fact.evidence_ref_id === filing.evidence_ref_id).map((fact) => <tr key={fact.id}>
            <th scope="row">{factLabels[fact.fact] ?? fact.fact}</th>
            <td>{fact.value.replace(/(\.\d*?[1-9])0+$|\.0+$/, "$1")}</td><td>{fact.unit}</td>
            <td>{fact.period_kind === "duration" ? `${fact.period_start} 至 ${fact.period_end}` : `${fact.period_end}（时点）`}</td>
            <td>{fact.raw_tag}</td>
          </tr>)}</tbody>
        </table>
      </div>
    </section>)}
  </article>;
}

function formatUtc(value: string): string {
  return value.replace("T", " ").replace(/(?:Z|\+00:00)$/, " UTC");
}
