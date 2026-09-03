import { formatDisplayDecimal } from "../lib/display-decimal";
import { formatDisplayTime } from "../lib/display-format";
import type { components } from "../lib/generated/schema";

type Observation = components["schemas"]["ResearchInputFundObservation"];

export function ResearchFundObservation({ observation }: { observation: Observation }): React.ReactElement {
  return <section className="decision-card__section" aria-label="发行商分类与持仓资料">
    <h3>发行商分类与持仓资料</h3>
    <p>SSGA 官方资料 · 本冻结集合内的一次观察，不代表实时持仓。保留发行商原始细分行业，不映射为统一行业。</p>
    <dl className="decision-card__authority">
      <div><dt>资料观察日期</dt><dd>{observation.as_of}（日期，不是精确时刻）</dd></div>
      <div><dt>证据可用时间</dt><dd>{formatDisplayTime(observation.available_at)}</dd></div>
      <div><dt>来源发布时刻</dt><dd>未知</dd></div>
      <div><dt>资料有效期</dt><dd>未知</dd></div>
    </dl>
    <p>权重分母未知；原权重单位为百分比，不重新归一化，不把未列出部分视为现金。零值是已知零，负值按原件保留，不推断杠杆或空头属性。</p>
    <p>本机收到时间不是历史公告时间。不用于历史回测或组合行业风险计算，也不授予交易权限。</p>
    {observation.truncated ? <p role="note">当前显示 {observation.industries.length} / {observation.total_industries} 项分类，显示 {observation.holdings.length} / {observation.total_holdings} 项持仓；完整内容见官方原件。</p> : null}
    <details className="portfolio-disclosure">
      <summary>查看发行商行业分类</summary>
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard access to an overflow evidence table. */}
      <div className="table-scroll" role="region" aria-label="发行商分类表，可横向滚动" tabIndex={0}>
        <table className="market-table numeric-data">
          <caption>发行商行业分类原始权重</caption>
          <thead><tr><th scope="col">原始分类</th><th scope="col">原始权重（%）</th></tr></thead>
          <tbody>{observation.industries.map((row) => <tr key={row.label}>
            <th className="research-fund-label" scope="row">{row.label}</th><td>{formatDisplayDecimal(row.weight_percent)}%</td>
          </tr>)}</tbody>
        </table>
      </div>
    </details>
    <details className="portfolio-disclosure">
      <summary>查看发行商持仓</summary>
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard access to an overflow evidence table. */}
      <div className="table-scroll" role="region" aria-label="发行商持仓表，可横向滚动" tabIndex={0}>
        <table className="market-table numeric-data">
          <caption>发行商持仓原始权重</caption>
          <thead><tr><th scope="col">名称</th><th scope="col">原始代码</th><th scope="col">原始分类</th><th scope="col">原始权重（%）</th><th scope="col">原币种</th></tr></thead>
          <tbody>{observation.holdings.map((row, index) => <tr key={index}>
            <th className="research-fund-label" scope="row">{row.name}</th><td className="research-fund-label">{row.ticker ?? "未知"}</td><td className="research-fund-label">{row.classification ?? "未知"}</td>
            <td>{formatDisplayDecimal(row.weight_percent)}%</td><td>{row.local_currency ?? "未知"}</td>
          </tr>)}</tbody>
        </table>
      </div>
    </details>
    <details className="portfolio-disclosure">
      <summary>分类与持仓证据来源</summary>
      <dl className="decision-card__versions">
        <div><dt>证据标识</dt><dd>{observation.evidence_ref_id}</dd></div>
        <div><dt>版本 / 内容 SHA-256</dt><dd>{observation.content_hash}</dd></div>
        <div><dt>产品页收到时间</dt><dd>{formatDisplayTime(observation.html_received_at)}</dd></div>
        <div><dt>持仓原件收到时间</dt><dd>{formatDisplayTime(observation.workbook_received_at)}</dd></div>
        <div><dt>产品页 SHA-256</dt><dd>{observation.html_content_hash}</dd></div>
        <div><dt>持仓原件 SHA-256</dt><dd>{observation.workbook_content_hash}</dd></div>
      </dl>
      <p><a href={observation.html_url} target="_blank" rel="noreferrer">查看发行商产品页</a> · <a href={observation.workbook_url} target="_blank" rel="noreferrer">查看发行商持仓原件</a></p>
    </details>
  </section>;
}
