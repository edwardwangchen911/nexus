import { formatDisplayTime } from "../lib/display-format";
import type { components } from "../lib/generated/schema";

type Profile = components["schemas"]["ResearchInputCompanyProfile"];

export function ResearchCompanyProfile({ profile }: { profile: Profile }): React.ReactElement {
  return <section className="decision-card__section" aria-label="公司背景资料">
    <h3>公司背景资料</h3>
    <p>FMP · 冻结的公司背景，不是 SEC 财务事实。行业与板块保留来源标签，不是已验证的组合风控分类。</p>
    <dl className="decision-card__authority">
      <div><dt>公司名称</dt><dd>{profile.company_name ?? "未知"}</dd></div>
      <div><dt>来源代码</dt><dd>{profile.symbol}</dd></div>
      <div><dt>来源行业标签</dt><dd>{profile.industry ?? "未知"}</dd></div>
      <div><dt>来源板块标签</dt><dd>{profile.sector ?? "未知"}</dd></div>
      <div><dt>行业分类体系</dt><dd>未知</dd></div>
      <div><dt>来源发布时刻</dt><dd>未知</dd></div>
      <div><dt>资料有效期</dt><dd>未知</dd></div>
      <div><dt>证据可用时间</dt><dd>{formatDisplayTime(profile.available_at)}</dd></div>
    </dl>
    <p>本条不用于组合行业风险计算、财务指标核验、价格认定或历史回测；不代表研究完成，执行保持锁定。</p>
    <details className="portfolio-disclosure">
      <summary>查看公司描述与标识</summary>
      <p style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere", maxInlineSize: "75ch" }}>{profile.description ?? "公司描述：未知"}</p>
      <dl className="decision-card__versions">
        <div><dt>CIK</dt><dd>{profile.cik ?? "未知"}</dd></div>
        <div><dt>ISIN</dt><dd>{profile.isin ?? "未知"}</dd></div>
        <div><dt>CUSIP</dt><dd>{profile.cusip ?? "未知"}</dd></div>
      </dl>
    </details>
    <details className="portfolio-disclosure">
      <summary>公司证据与来源</summary>
      <p>版本为本地内容哈希，不是来源修订编号；本机收到时间不是历史发布时间。</p>
      <dl className="decision-card__versions">
        <div><dt>证据标识</dt><dd>{profile.evidence_ref_id}</dd></div>
        <div><dt>本地版本</dt><dd>{profile.revision}</dd></div>
        <div><dt>内容 SHA-256</dt><dd>{profile.content_hash}</dd></div>
        <div><dt>原件 SHA-256</dt><dd>{profile.raw_content_hash}</dd></div>
        <div><dt>收件凭证 SHA-256</dt><dd>{profile.receipt_content_hash}</dd></div>
        <div><dt>本机收到时间</dt><dd>{formatDisplayTime(profile.received_at)}</dd></div>
      </dl>
      <a href={profile.source_url} target="_blank" rel="noreferrer">查看 FMP 来源地址</a>
    </details>
  </section>;
}
