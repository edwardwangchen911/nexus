import { formatDisplayDecimal } from "../lib/display-decimal";
import { formatDisplayTime } from "../lib/display-format";
import type { components } from "../lib/generated/schema";

type Observation = components["schemas"]["ResearchInputMacroObservation"];

export function ResearchMacroObservations({ observations }: { observations: Observation[] }): React.ReactElement {
  return <section className="decision-card__section" aria-label="宏观观察资料">
    <h3>宏观观察资料</h3>
    <p>FRED · 本冻结集合中的观察。经济日期与历史版本日期不是精确发布时刻，也不表示本机当日已经取得资料。</p>
    <p>按来源元数据保留单位；Percent 是百分比原值，不乘 100。最多显示 3 位小数，极小非零值保留范围，零不等于未知。</p>
    <p>收到时间不回填历史可用性；本条不可用于历史回测，不代表完整宏观覆盖或研究完成，执行保持锁定。</p>
    {observations.map((observation) => <section key={observation.evidence_ref_id} className="decision-card__section" aria-label={`${observation.series} 宏观观察`}>
      <h4 style={{ overflowWrap: "anywhere" }}>{observation.series}</h4>
      <dl className="decision-card__authority">
        <div><dt>观察值</dt><dd className="numeric-data">{formatDisplayDecimal(observation.value)}{observation.unit === "Percent" ? "%" : ""}</dd></div>
        <div><dt>来源单位</dt><dd>{observation.unit}</dd></div>
        <div><dt>经济观察日期</dt><dd>{observation.observed_on}</dd></div>
        <div><dt>历史版本日期（vintage）</dt><dd>{observation.vintage}</dd></div>
        <div><dt>本机收到时间</dt><dd>{formatDisplayTime(observation.received_at)}</dd></div>
        <div><dt>证据可用时间</dt><dd>{formatDisplayTime(observation.available_at)}</dd></div>
      </dl>
      <details className="portfolio-disclosure">
        <summary>{observation.series} 证据与日历绑定</summary>
        <p>版本是本地内容版本，不是来源修订序号；日历引用用于保守可用时点核验，不是宏观发布日期证明。</p>
        <dl className="decision-card__versions">
          <div><dt>观察标识</dt><dd>{observation.observation_id}</dd></div>
          <div><dt>证据标识</dt><dd>{observation.evidence_ref_id}</dd></div>
          <div><dt>日历证据标识</dt><dd>{observation.calendar_evidence_ref_id}</dd></div>
          <div><dt>本地版本</dt><dd>{observation.revision}</dd></div>
          <div><dt>内容 SHA-256</dt><dd>{observation.content_hash}</dd></div>
          <div><dt>原件 SHA-256</dt><dd>{observation.raw_content_hash}</dd></div>
        </dl>
      </details>
    </section>)}
  </section>;
}
