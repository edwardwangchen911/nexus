import { formatDisplayDecimal } from "../lib/display-decimal";
import { formatDisplayTime } from "../lib/display-format";
import type { components } from "../lib/generated/schema";

type History = components["schemas"]["ResearchInputNavHistory"];

export function ResearchNavHistory({ history }: { history: History }): React.ReactElement {
  return <section className="decision-card__section" aria-label="发行商净值历史资料">
    <h3>发行商净值历史资料</h3>
    <p>SSGA 官方资料 · 每份基金净值，不是市场成交价格或总收益序列。基金净资产不是个人账户资产。</p>
    <dl className="decision-card__authority">
      <div><dt>原件观察日期范围</dt><dd>{history.coverage_start} 至 {history.coverage_end}</dd></div>
      <div><dt>原件记录数</dt><dd>{history.total_observations} 条</dd></div>
      <div><dt>证据可用时间</dt><dd>{formatDisplayTime(history.available_at)}</dd></div>
      <div><dt>来源发布时刻 / 有效期</dt><dd>未知</dd></div>
    </dl>
    <p>日期为发行商观察日期，可能包含非交易日，不代表精确市场时刻。币种来自产品页基础币种，净值原件未提供独立币种列。</p>
    <p>拆分调整、分红再投资与修订方法未知。本机收到时间不是历史公告时间，不用于历史回测、总收益计算或成交价格判断，不授予执行权限。</p>
    {history.truncated ? <p role="note">显示最近 {history.observations.length} / {history.total_observations} 条，按观察日期排列；完整记录见官方原件。</p> : null}
    <details className="portfolio-disclosure">
      <summary>查看净值观察记录</summary>
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard access to the bounded overflow evidence table. */}
      <div className="table-scroll" role="region" aria-label="发行商净值表，可横向滚动" tabIndex={0}>
        <table className="market-table numeric-data">
          <caption>发行商净值原始观察</caption>
          <thead><tr><th scope="col">观察日期</th><th scope="col">每份净值（USD / 份）</th><th scope="col">流通份额（份）</th><th scope="col">基金净资产（USD）</th></tr></thead>
          <tbody>{history.observations.map((row) => <tr key={row.date}>
            <th scope="row">{row.date}</th><td>{formatDisplayDecimal(row.nav)}</td>
            <td>{row.shares_outstanding === null ? "未知" : formatDisplayDecimal(row.shares_outstanding)}</td>
            <td>{row.total_net_assets === null ? "未知" : formatDisplayDecimal(row.total_net_assets)}</td>
          </tr>)}</tbody>
        </table>
      </div>
      <p>最多显示 3 位小数；极小非零数显示范围，零与未知分别保留。精确数值以原件为准。</p>
    </details>
    <details className="portfolio-disclosure">
      <summary>净值证据与来源</summary>
      <dl className="decision-card__versions">
        <div><dt>证据标识</dt><dd>{history.evidence_ref_id}</dd></div>
        <div><dt>版本 / 内容 SHA-256</dt><dd>{history.content_hash}</dd></div>
        <div><dt>产品页收到时间</dt><dd>{formatDisplayTime(history.html_received_at)}</dd></div>
        <div><dt>净值原件收到时间</dt><dd>{formatDisplayTime(history.workbook_received_at)}</dd></div>
        <div><dt>产品页 SHA-256</dt><dd>{history.html_content_hash}</dd></div>
        <div><dt>净值原件 SHA-256</dt><dd>{history.workbook_content_hash}</dd></div>
      </dl>
      <p><a href={history.html_url} target="_blank" rel="noreferrer">查看发行商产品页</a> · <a href={history.workbook_url} target="_blank" rel="noreferrer">查看发行商净值原件</a></p>
    </details>
  </section>;
}
