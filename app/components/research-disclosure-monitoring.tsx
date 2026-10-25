import type { components } from "../lib/generated/schema";
import { formatDisplayTime } from "../lib/display-format";
import { Badge } from "./ui/badge";
import { blockedReasons, coverageReasons, notificationStatuses, researchHref, researchStatuses, stateLabels, usageStatuses } from "./research-monitoring";

export type DisclosureMonitoringView = components["schemas"]["ResearchDisclosureMonitoringView"];

const eventReviews = {
  addressed: "事件已复核处理", unresolved: "事件仍未解决",
  unverified: "事件复核尚未获确认", not_performed: "尚未进行事件复核",
} as const;

function accession(eventKey: string): string | null {
  return /^sec:\d{10}:(\d{10}-\d{2}-\d{6})$/.exec(eventKey)?.[1] ?? null;
}

export function ResearchDisclosureMonitoring({ monitoring, instrumentId }: { monitoring: DisclosureMonitoringView; instrumentId: string }): React.ReactElement {
  const recent = [...monitoring.triggers].sort((a, b) => Date.parse(b.checked_at) - Date.parse(a.checked_at)).slice(0, 5);
  const batches = new Map<string, typeof recent>();
  for (const trigger of recent) {
    const key = trigger.batch_id ?? `unbatched:${trigger.trigger_id}`;
    batches.set(key, [...(batches.get(key) ?? []), trigger]);
  }
  const check = monitoring.latest_check;
  return (
    <section className="decision-card" aria-label="SEC 披露监控记录">
      <header className="decision-card__heading">
        <div><p className="eyebrow">研究之后 · 只读监控记录</p><h2>SEC 披露监控</h2></div>
        <Badge>{stateLabels[monitoring.state]}</Badge>
      </header>
      <p>读取时点：<time className="numeric-data" dateTime={monitoring.observed_at}>{formatDisplayTime(monitoring.observed_at)}</time>。冻结研究内容保持原样。</p>
      <p role="note">计划状态不代表监控进程当前在线。官方 SEC 披露触发仅表示收录变化，不是经营影响结论；交易保持锁定。</p>
      {monitoring.coverage_reason ? <p role="note">{monitoring.coverage_reason === "no_condition" ? "研究未提供可安装的披露复核条件。" : coverageReasons[monitoring.coverage_reason]}</p> : null}
      {monitoring.condition ? <section className="decision-card__section" aria-label="披露复核条件">
        <h3>披露复核条件</h3><p>SEC · {monitoring.condition.forms.join(" · ")}</p>
        <p>对比冻结索引与本地实际收录，检查是否新增指定类型披露。检查依据须在 {monitoring.condition.max_age_seconds} 秒以内。</p>
      </section> : <p>当前未读到已安装的披露复核条件。</p>}
      <dl className="decision-card__versions">
        <div><dt>冻结索引截止时间</dt><dd>{formatDisplayTime(monitoring.baseline_as_of)}</dd></div>
        <div><dt>计划有效期至</dt><dd>{formatDisplayTime(monitoring.valid_until)}</dd></div>
        <div><dt>研究复核到期时间</dt><dd>{formatDisplayTime(monitoring.review_due_at)}</dd></div>
      </dl>
      {monitoring.state === "superseded" ? (monitoring.replacement_run_id
        ? <a href={researchHref(instrumentId, monitoring.replacement_run_id)}>查看替代披露研究</a>
        : <p>替代研究的精确记录暂不可用。</p>) : null}
      <section className="decision-card__section" aria-label="最近一次披露检查">
        <h3>最近一次披露检查</h3>
        {check ? <>
          <p>{({ true: "该次检查收录了指定类型的新披露", false: "该次检查未收录指定类型的新披露", unknown: "该次披露检查结果未知" } as const)[check.result]}</p>
          <dl className="decision-card__versions">
            <div><dt>计划检查时间</dt><dd>{formatDisplayTime(check.scheduled_at)}</dd></div>
            <div><dt>实际检查时间</dt><dd>{formatDisplayTime(check.checked_at)}</dd></div>
          </dl>
          {check.result === "unknown" ? <p>披露原件或时间依据尚未核验，无法确认该次收录变化。</p> : null}
        </> : <p>尚无已保存的披露检查；不能据此认定没有新增披露。</p>}
      </section>
      <section className="decision-card__section" aria-label="披露事件与后续处理">
        <h3>披露事件与后续处理</h3>
        <p>共记录 {monitoring.trigger_count} 项披露事件。研究完成不等于每项事件已经解决。</p>
        {recent.length ? <details>
          <summary>查看最近 {recent.length} 项披露事件</summary>
          <p>仅显示最近 {recent.length} 项；按原封存批次分组，不表示批次已拆分或重新派发。本区只列披露成员；同批价格触发见价格监控。</p>
          {Array.from(batches, ([key, triggers]) => <section key={key} aria-label={triggers[0].batch_id ? `封存批次 ${triggers[0].batch_id}` : "未归入封存批次的事件"}>
            <h4>{triggers[0].batch_id ? "同一封存批次" : "未归入封存批次"}</h4>
            {triggers[0].batch_id ? <p>批次编号：<span className="numeric-data">{triggers[0].batch_id}</span></p> : null}
            <ol>{triggers.map((trigger) => <li key={trigger.trigger_id}>
              <p>公开申报号：<span className="numeric-data">{accession(trigger.event_key) ?? "无法核验申报号"}</span></p>
              <p>实际检查时间：<time className="numeric-data" dateTime={trigger.checked_at}>{formatDisplayTime(trigger.checked_at)}</time></p>
              <dl className="decision-card__versions">
                <div><dt>事件复核</dt><dd>{eventReviews[trigger.event_review]}</dd></div>
                <div><dt>研究处理</dt><dd>{researchStatuses[trigger.status]}</dd></div>
                <div><dt>研究用量</dt><dd>{usageStatuses[trigger.usage_status]}</dd></div>
                <div><dt>通知处理</dt><dd>{notificationStatuses[trigger.notification_status]}</dd></div>
              </dl>
              {trigger.blocked_reason ? <p>{blockedReasons[trigger.blocked_reason]}</p> : null}
              <p>设备送达状态未知；通知处理状态不能证明设备已收到。</p>
              {trigger.run_id ? <a href={researchHref(instrumentId, trigger.run_id)}>查看本次事件的研究</a> : null}
            </li>)}</ol>
          </section>)}
        </details> : <p>本次读回未列出披露事件明细。</p>}
      </section>
    </section>
  );
}
