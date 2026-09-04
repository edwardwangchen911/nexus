import { formatDisplayDecimal } from "../lib/display-decimal";
import { formatDisplayTime } from "../lib/display-format";
import type { components } from "../lib/generated/schema";

type Observation = components["schemas"]["ResearchInputActionObservation"];
type Event = components["schemas"]["ResearchInputActionEvent"];
type NumericField = "rate" | "old_rate" | "new_rate";

const dates = [
  ["process_date", "来源处理日期"], ["ex_date", "除息日期"],
  ["record_date", "登记日期"], ["payable_date", "支付日期"],
  ["declaration_date", "声明日期"],
] as const;
const numbers = [["rate", "来源数值"], ["old_rate", "原数值"], ["new_rate", "新数值"]] as const;

function numericValue(event: Event, field: NumericField): string {
  if (event.unavailable_numeric_fields.includes(field)) return "原件数值超出展示范围";
  return formatDisplayDecimal(event[field]);
}

export function ResearchActionObservation({ observation }: { observation: Observation }): React.ReactElement {
  return <section className="decision-card__section" aria-label="公司行动观测资料">
    <h3>公司行动观测资料</h3>
    <p>Alpaca · 本地冻结原件的当前研究观察。分页已读取完毕，来源完整性未知。</p>
    <dl className="decision-card__authority">
      <div><dt>来源查询窗口</dt><dd>{observation.query_start} 至 {observation.query_end}</dd></div>
      <div><dt>已保存分页</dt><dd>{observation.page_count} 页</dd></div>
      <div><dt>原件事件总量</dt><dd>{observation.total_event_count} 条</dd></div>
      <div><dt>证券身份绑定</dt><dd>已绑定 {observation.bound_event_count} 条 / 未绑定 {observation.unbound_event_count} 条</dd></div>
      <div><dt>各页最晚收到时间</dt><dd>{formatDisplayTime(observation.available_at)}</dd></div>
      <div><dt>使用范围</dt><dd>仅供当前研究观察 · 执行锁定</dd></div>
    </dl>
    <p>查询窗口不是除息覆盖范围。收到时间取各页实际接收时间的最大值，不是历史公告时间；未绑定事件不归属于本标的，也不能当作无事件证明。</p>
    <p>不用于历史回测或账本入账，不推断来源数值的单位、拆股比率或实际到账，不授予执行权限。</p>
    {observation.truncated ? <p role="note">显示 {observation.events.length} / {observation.bound_event_count} 条已绑定观测；其余保留在冻结原件中，未改变证据全文。</p> : null}
    {observation.events.length === 0 ? <p>没有可展示的已绑定观测，不代表该证券没有公司行动；来源完整性仍未知。</p> : <details className="portfolio-disclosure">
      <summary>查看公司行动观察记录</summary>
      <p>日期与类别保留来源报告值，不据此推导行动的经济含义。数值最多显示 3 位小数；极小非零数显示范围，零与未知分别保留。超出展示合同的数值仍保存在原件中。</p>
      {observation.events.map((event, ordinal) => <details className="portfolio-disclosure" key={`${event.page_number}:${event.category}:${event.index}`}>
        <summary>观察 {ordinal + 1} · 第 {event.page_number} 页</summary>
        <dl className="decision-card__versions">
          <div><dt>来源类别</dt><dd>{event.category}</dd></div>
          <div><dt>原事件标识</dt><dd>{event.provider_event_id}</dd></div>
          <div><dt>原件定位</dt><dd>第 {event.page_number} 页 · 类别内索引 {event.index}（从 0 起）</dd></div>
          <div><dt>来源证券代码</dt><dd>{event.symbol || "未知"}</dd></div>
          <div><dt>来源旧代码</dt><dd>{event.old_symbol || "未知"}</dd></div>
          <div><dt>来源新代码</dt><dd>{event.new_symbol || "未知"}</dd></div>
        </dl>
        {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard access to the existing bounded overflow table. */}
        <div className="table-scroll" role="region" aria-label={`观察 ${ordinal + 1} 的来源字段表，可横向滚动`} tabIndex={0}>
          <table className="market-table numeric-data" style={{ minInlineSize: "100%" }}>
            <caption>观察 {ordinal + 1} 的来源字段</caption>
            <thead><tr><th scope="col">来源字段</th><th scope="col">原件观察值</th></tr></thead>
            <tbody>
              {dates.map(([field, label]) => <tr key={field}><th className="research-fund-label" scope="row">{label}（{field}）</th><td>{event[field] ?? "未知"}</td></tr>)}
              {numbers.map(([field, label]) => <tr key={field}><th className="research-fund-label" scope="row">{label}（{field}）</th><td style={{ whiteSpace: event[field] === null ? "normal" : undefined }}>{numericValue(event, field)}</td></tr>)}
              <tr><th className="research-fund-label" scope="row">来源币种（currency）</th><td style={{ whiteSpace: "normal" }}>{event.currency === "" ? "未知（原件为空）" : event.currency ?? "未知"}</td></tr>
            </tbody>
          </table>
        </div>
        <p>较长数值可横向滚动查看；聚焦表格后也可使用方向键。</p>
      </details>)}
    </details>}
    <details className="portfolio-disclosure">
      <summary>公司行动证据与版本</summary>
      <dl className="decision-card__versions">
        <div><dt>证据标识</dt><dd>{observation.evidence_ref_id}</dd></div>
        <div><dt>版本</dt><dd>{observation.revision}</dd></div>
        <div><dt>内容 SHA-256</dt><dd>{observation.content_hash}</dd></div>
        <div><dt>采集清单 SHA-256</dt><dd>{observation.manifest_content_hash}</dd></div>
      </dl>
      <p>版本为本地内容哈希，不是来源修订编号。完整原件与逐页接收凭证保存在本地归档；重新读取页面不会重新采集。</p>
    </details>
  </section>;
}
