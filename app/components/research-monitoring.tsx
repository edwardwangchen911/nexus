import type { components } from "../lib/generated/schema";
import { formatDisplayDecimal } from "../lib/display-decimal";
import { formatDisplayTime } from "../lib/display-format";
import { Badge } from "./ui/badge";

export type PriceMonitoringView = components["schemas"]["ResearchMonitoringView"];

const stateLabels = {
  not_installed: "尚未安装监控计划", valid: "计划有效期内", expired: "计划已过期",
  superseded: "已由新研究替代", withdrawn: "计划已撤回", unknown: "计划状态未知",
} as const;

const checkReasons = {
  observation_missing: "缺少已保存的观测，无法判断本次条件。",
  authority_mismatch: "报价来源或标的不匹配，无法作为本次检查依据。",
  time_unproven: "报价时间无法核验，无法判断本次条件。",
  price_missing: "缺少可用价格，无法判断本次条件。",
} as const;

const coverageReasons = {
  audit_rejected: "研究审查未通过，监控覆盖尚未建立。",
  source_invalid: "监控所需来源未通过核验，覆盖暂不可用。",
  no_condition: "研究未提供可安装的价格复核条件。",
  run_failed: "研究运行失败，未建立新的监控覆盖。",
  usage_unknown: "研究用量未知，预算继续保留，监控覆盖尚待核对。",
  awaiting_reconciliation: "研究结果仍待核对，监控覆盖尚未确认。",
} as const;

const researchStatuses = {
  pending: "等待处理", blocked: "处理受阻", admitted: "研究已登记",
  completed: "研究已完成", failed: "研究失败", unknown: "研究结果未知", covered: "已由其他研究覆盖",
} as const;

const blockedReasons = {
  budget: "预算不足或预算仍被占用。", account_occupied: "账户已有研究任务占用。",
  inputs: "研究输入尚不满足要求。", overflow: "待处理任务达到容量上限。", unknown: "受阻原因尚未确认。",
} as const;

const usageStatuses = {
  not_started: "研究尚未开始", in_progress: "用量待结算", known: "用量已确认",
  unknown: "用量未知，预算继续保留；不会据此重放研究。",
} as const;

const notificationStatuses = {
  queued: "通知等待处理", delivering: "通知正在处理", published: "通知发布处理已完成",
  dead_letter: "通知处理失败", unknown: "通知处理状态未知",
} as const;

function researchHref(instrumentId: string, runId: string): string {
  return `/research/${encodeURIComponent(instrumentId)}?run_id=${encodeURIComponent(runId)}`;
}

export function ResearchMonitoring({ monitoring, instrumentId }: { monitoring: PriceMonitoringView; instrumentId: string }): React.ReactElement {
  const check = monitoring.latest_check;
  const condition = monitoring.condition;
  const recent = [...monitoring.triggers].sort((a, b) => Date.parse(b.checked_at) - Date.parse(a.checked_at)).slice(0, 5);
  return (
    <section className="decision-card" aria-label="价格监控记录">
      <header className="decision-card__heading">
        <div><p className="eyebrow">研究之后 · 只读监控记录</p><h2>价格监控</h2></div>
        <Badge>{stateLabels[monitoring.state]}</Badge>
      </header>
      <p>读取时点：<time className="numeric-data" dateTime={monitoring.observed_at}>{formatDisplayTime(monitoring.observed_at)}</time>。冻结研究内容保持原样。</p>
      <p role="note">计划状态不代表监控进程当前在线。价格条件用于触发研究复核，不是交易指令；交易保持锁定。</p>
      {monitoring.coverage_reason ? <p role="note">{coverageReasons[monitoring.coverage_reason]}</p> : null}
      {condition ? (
        <section className="decision-card__section" aria-label="价格复核条件">
          <h3>价格复核条件</h3>
          <p>Schwab L1 · 未复权买卖报价中间价</p>
          <p>{condition.operator === "gte" ? "不低于" : "不高于"} <span className="numeric-data" title={`精确阈值：${condition.threshold} ${condition.currency}`}>{formatDisplayDecimal(condition.threshold)} {condition.currency}</span></p>
          <p>检查采用的报价须在 {condition.max_age_seconds} 秒以内；达到阈值不等于原研究论点已经失效。</p>
        </section>
      ) : <p>当前未读到已安装的价格复核条件。</p>}
      <dl className="decision-card__versions">
        <div><dt>计划有效期至</dt><dd>{formatDisplayTime(monitoring.valid_until)}</dd></div>
        <div><dt>研究复核到期时间</dt><dd>{formatDisplayTime(monitoring.review_due_at)}</dd></div>
      </dl>
      {monitoring.state === "superseded" ? (monitoring.replacement_run_id
        ? <a href={researchHref(instrumentId, monitoring.replacement_run_id)}>查看替代研究</a>
        : <p>替代研究的精确记录暂不可用。</p>) : null}
      <section className="decision-card__section" aria-label="最近一次检查">
        <h3>最近一次检查</h3>
        {check ? <>
          <p>{({ true: "该次检查条件已满足", false: "该次检查条件未满足", unknown: "该次检查结果未知" } as const)[check.result]}</p>
          <dl className="decision-card__versions">
            <div><dt>计划检查时间</dt><dd>{formatDisplayTime(check.scheduled_at)}</dd></div>
            <div><dt>实际检查时间</dt><dd>{formatDisplayTime(check.checked_at)}</dd></div>
          </dl>
          {check.result === "unknown" ? <p>{check.reason ? checkReasons[check.reason] : "观测来源或时间依据尚不完整，无法判断本次条件。"}</p> : null}
        </> : <p>尚无已保存的检查；不能据此认定条件未满足或没有触发。</p>}
      </section>
      <section className="decision-card__section" aria-label="触发与后续处理">
        <h3>触发与后续处理</h3>
        <p>已记录 {monitoring.trigger_count} 次触发。触发记录与研究完成、通知处理分别判断。</p>
        {recent.length ? <details>
          <summary>查看最近 {recent.length} 次触发</summary>
          <ol>{recent.map((trigger) => <li key={trigger.trigger_id}>
            <p><time className="numeric-data" dateTime={trigger.checked_at}>{formatDisplayTime(trigger.checked_at)}</time> · {trigger.cause === "initial_match" ? "首次检查即满足条件" : "价格越过复核阈值"}</p>
            <dl className="decision-card__versions">
              <div><dt>研究处理</dt><dd>{researchStatuses[trigger.status]}</dd></div>
              <div><dt>研究用量</dt><dd>{usageStatuses[trigger.usage_status]}</dd></div>
              <div><dt>通知处理</dt><dd>{notificationStatuses[trigger.notification_status]}</dd></div>
            </dl>
            {trigger.blocked_reason ? <p>{blockedReasons[trigger.blocked_reason]}</p> : null}
            <p>设备送达状态未知；通知处理状态不能证明设备已收到。</p>
            {trigger.run_id ? <a href={researchHref(instrumentId, trigger.run_id)}>查看本次触发的研究</a> : null}
          </li>)}</ol>
        </details> : <p>本次读回未列出触发明细。</p>}
      </section>
    </section>
  );
}
