import type { components } from "../lib/generated/schema";
import { formatDisplayTime } from "../lib/display-format";
import { Badge } from "./ui/badge";

type View = components["schemas"]["DurableSecView"];
const states = { active: "规则已启用", paused: "规则已暂停", revoked: "规则已撤销", unknown: "规则状态待核验" } as const;
const coverage = { complete: "完整覆盖", partial: "部分覆盖", unknown: "覆盖范围未知" } as const;
const results = { true: "发现规则生效后发布的新公告", false: "本次完整覆盖区间未发现新公告", unknown: "本次检查无法确认是否有新公告" } as const;
const reasons = {
  evidence_unverified: "检查依据尚未核验完整。",
  coverage_incomplete: "覆盖依据尚不完整；独立可证的新增仍会单独核验，其余保持未知。",
  coverage_conflict: "覆盖记录或公告元数据存在矛盾，受影响部分保持未知。",
  source_stale: "本次原件已超过新鲜度要求。",
  source_invalid: "原件或发行人身份未通过核验。",
  time_invalid: "原件接收时间或核对区间未通过核验。",
  history_invalid: "既有公告身份记录未通过核验。",
  rule_inactive: "本次检查时规则未生效。",
} as const;

export function DurableSecDiscovery({ discovery }: { discovery: View }): React.ReactElement {
  const check = discovery.latest_check;
  return <section className="decision-card" aria-label="长期 SEC 公告发现">
    <header className="decision-card__heading"><div><p className="eyebrow">独立于研究有效期 · 只读记录</p><h2>长期 SEC 公告发现</h2></div><Badge>{states[discovery.state]}</Badge></header>
    <p>公告发现规则不随旧研究计划到期；规则已启用不代表宿主当前在线。</p>
    <dl className="decision-card__versions">
      <div><dt>规则生效时间</dt><dd>{formatDisplayTime(discovery.effective_at)}</dd></div>
      <div><dt>读取时点</dt><dd>{formatDisplayTime(discovery.observed_at)}</dd></div>
      <div><dt>公告类型</dt><dd>{discovery.forms.length ? discovery.forms.join(" · ") : "待核验"}</dd></div>
    </dl>
    {check ? <section className="decision-card__section" aria-label="最近公告发现检查">
      <h3>最近已保存检查</h3><p>{results[check.result]} · {coverage[check.coverage]}</p>
      {check.interval_start && check.interval_end ? <p>本次核对区间：{formatDisplayTime(check.interval_start)} 至 {formatDisplayTime(check.interval_end)}。</p> : null}
      <p>检查时间：{formatDisplayTime(check.checked_at)}。这是当时的保存结果，不代表当前资料仍然新鲜。</p>
      {check.result === "true" && check.coverage !== "complete" ? <p>已独立确认的新增公告仍成立；未覆盖部分保持未知。</p> : null}
      {check.result === "unknown" ? <p>{reasons[check.reason ?? "evidence_unverified"]}不按“没有公告”处理。</p> : null}
      {check.coverage_details ? <div>
        {check.coverage_details.unread_history_files ? <p>另有 {check.coverage_details.unread_history_files} 份历史分片未纳入本次核验，不能据此给出完整范围的无新增结论。</p> : null}
        <p>{check.coverage_details.invalid_rows ? `${check.coverage_details.invalid_rows} 行原件存在问题，受影响记录保持未知。` : "已核验原件没有发现坏行。"}</p>
        {!check.coverage_details.coverage_start_known ? <p>本次覆盖起点尚未核验。</p> : null}
      </div> : null}
    </section> : <p>尚无可核验的已保存检查；不表示没有公告。</p>}
    <p>共保存 {discovery.event_count} 项新增公告{discovery.event_count > 5 ? "，仅显示最近 5 项" : ""}。首次发现时间不等于公告发布时间；生效前的旧公告不作为新增。</p>
    {discovery.events.map((event) => <article className="decision-card__section" key={event.event_key}>
      <h3>{event.form} · {event.event_key.split(":").at(-1)}</h3>
      <dl className="decision-card__versions"><div><dt>SEC 接受时间</dt><dd>{formatDisplayTime(event.accepted_at)}</dd></div><div><dt>首次发现时间</dt><dd>{formatDisplayTime(event.first_seen_at)}</dd></div></dl>
      {event.metadata_status === "conflict" ? <p>公告元数据出现冲突，后续处理状态待核验。</p> : null}
    </article>)}
    <p role="note">公告发现不代表通知已送达或研究已完成。研究有效期、旧事件一次限制、每日增量名额与未知费用仍独立核验；本区不表示可以发起研究或交易，交易保持锁定。</p>
  </section>;
}
