import type { components } from "../lib/generated/schema";
import { formatDisplayTime } from "../lib/display-format";
import { Badge } from "./ui/badge";

type View = components["schemas"]["DurableSecView"];
type DailyProposal = components["schemas"]["DurableSecPendingProposal"];
type Notice = components["schemas"]["DurableSecNoticeView"];
type ResearchStatus = NonNullable<components["schemas"]["DurableSecEventView"]["research_status"]>;
const states = { active: "规则已启用", paused: "规则已暂停", revoked: "规则已撤销", unknown: "规则状态待核验" } as const;
const coverage = { complete: "完整覆盖", partial: "部分覆盖", unknown: "覆盖范围未知" } as const;
const results = { true: "发现规则生效后发布的新公告", false: "本次完整覆盖区间未发现新公告", unknown: "本次检查无法确认是否有新公告" } as const;
const reasons = {
  evidence_unverified: "检查依据尚未核验完整。",
  coverage_incomplete: "覆盖依据尚不完整；独立可证的新增仍会单独核验，其余保持未知。",
  accepted_cutoff_unproven: "SEC 原件已核验，尚无完整受理截止证明。",
  coverage_conflict: "覆盖记录或公告元数据存在矛盾，受影响部分保持未知。",
  source_stale: "本次原件已超过新鲜度要求。",
  source_invalid: "原件或发行人身份未通过核验。",
  time_invalid: "原件接收时间或核对区间未通过核验。",
  history_invalid: "既有公告身份记录未通过核验。",
  rule_inactive: "本次检查时规则未生效。",
} as const;
const noticeStatuses = {
  queued: "等待领取",
  delivering: "已领取，等待结果",
  published: "已记录响应",
  dead_letter: "发送尝试已结束",
} satisfies Record<Notice["status"], string>;
const researchStatuses = {
  no_receipt: "暂无逐事件处理回执。",
  bound: "已关联增量材料，暂无逐事件处理回执。",
  increment_failed: "原增量研究失败，暂无逐事件处理回执。",
  addressed_increment: "已保存增量研究的逐事件处理回执。",
  addressed_daily: "已保存日研的逐事件处理回执。",
  unverified: "逐事件处理证据待核验。",
} satisfies Record<ResearchStatus, string>;
const serverResults = {
  accepted: "Bark 已接受",
  http_error: "收到非成功 HTTP 响应",
  unconfirmed: "待核验",
} satisfies Record<Notice["server_result"], string>;
const failureReasons = {
  UNKNOWN: "原始失败原因待核验",
  PAYLOAD_INVALID: "通知内容校验未通过",
  EVENT_UNSUPPORTED: "通知类型校验未通过",
  ENCRYPTION_INVALID: "通知加密配置校验未通过",
  ENCRYPTION_FAILED: "通知加密失败",
  CONNECT_TIMEOUT: "连接 Bark 超时",
  READ_TIMEOUT: "等待 Bark 响应超时",
  WRITE_TIMEOUT: "向 Bark 提交请求超时",
  POOL_TIMEOUT: "等待可用连接超时",
  TRANSPORT_ERROR: "Bark 连接发生错误",
  HTTP_REJECTED: "Bark 返回非成功响应",
  RESPONSE_DECODE: "无法核验 Bark 响应",
  PROVIDER_REJECTED: "Bark 的响应未能确认接受",
} satisfies Record<components["schemas"]["FailureReason"], string>;

function NoticeReceipt({ notice }: { notice: Notice }): React.ReactElement {
  return <>
    <dl className="decision-card__versions">
      <div><dt>通知记录</dt><dd>{noticeStatuses[notice.status]} · {notice.attempt_count} 次尝试</dd></div>
      <div><dt>通知服务结果</dt><dd>{serverResults[notice.server_result]}</dd></div>
      <div><dt>手机实收</dt><dd>待核验</dd></div>
    </dl>
    {notice.failure_reason ? <p>{failureReasons[notice.failure_reason]}</p> : null}
    <details>
      <summary>通知编号与时间</summary>
      <dl className="decision-card__versions">
        <div><dt>通知编号</dt><dd>{notice.outbox_id}</dd></div>
        {notice.attempted_at ? <div><dt>开始时间</dt><dd>{formatDisplayTime(notice.attempted_at)}</dd></div> : null}
        {notice.finished_at ? <div><dt>结束时间</dt><dd>{formatDisplayTime(notice.finished_at)}</dd></div> : null}
      </dl>
    </details>
  </>;
}

const collectionStatuses = {
  archived: "SEC 索引原件已归档",
  failed: "采集失败",
  unknown: "采集结果待核验",
} as const;

export function DurableSecDiscovery({ discovery, dailyProposal }: {
  discovery: View;
  dailyProposal?: DailyProposal | null;
}): React.ReactElement {
  const check = discovery.latest_check;
  const collection = discovery.latest_collection;
  const dailyReviews = new Map(dailyProposal?.items.map((item) => [item.event_id, item]));
  return <section className="decision-card" aria-label="长期 SEC 公告发现">
    <header className="decision-card__heading"><div><p className="eyebrow">独立于研究有效期 · 只读记录</p><h2>长期 SEC 公告发现</h2></div><Badge>{states[discovery.state]}</Badge></header>
    <p>公告发现规则不随旧研究计划到期；规则已启用不代表宿主当前在线。</p>
    <dl className="decision-card__versions">
      <div><dt>规则生效时间</dt><dd>{formatDisplayTime(discovery.effective_at)}</dd></div>
      <div><dt>公告和通知读取时点</dt><dd>{formatDisplayTime(discovery.observed_at)}</dd></div>
      <div><dt>公告类型</dt><dd>{discovery.forms.length ? discovery.forms.join(" · ") : "待核验"}</dd></div>
    </dl>
    {collection ? <section className="decision-card__section" aria-label="最近 SEC 采集回执">
      <h3>最近 SEC 采集回执</h3>
      <p>{collectionStatuses[collection.status]}</p>
      <dl className="decision-card__versions">
        <div><dt>计划采集时间</dt><dd>{formatDisplayTime(collection.scheduled_at)}</dd></div>
        <div><dt>领取时间</dt><dd>{formatDisplayTime(collection.claimed_at)}</dd></div>
        {collection.finished_at ? <div><dt>回执时间</dt><dd>{formatDisplayTime(collection.finished_at)}</dd></div> : null}
      </dl>
    </section> : null}
    {check ? <section className="decision-card__section" aria-label="最近公告发现检查">
      <h3>最近已保存检查</h3>
      {!check.evaluation_version && check.coverage === "complete" ? <>
        <p>{check.result === "false" ? "历史记录（按旧口径）· 当时保存为完整覆盖、未发现新公告" : "历史记录（按旧口径）· 当时保存为完整覆盖"}</p>
        <p>当前受理区间完整性未知，等待按新口径完成下一次常规检查。</p>
      </> : <p>{results[check.result]} · {coverage[check.coverage]}</p>}
      {check.source_received_at ? <p>SEC 原件收到时间：{formatDisplayTime(check.source_received_at)}。</p> : null}
      {!check.evaluation_version && check.interval_start && check.interval_end ? <p>历史记录区间：{formatDisplayTime(check.interval_start)} 至 {formatDisplayTime(check.interval_end)}。</p> : null}
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
    {discovery.events.map((event) => {
      const review = event.event_id ? dailyReviews.get(event.event_id) : undefined;
      return <article className="decision-card__section" key={event.event_key}>
        <h3>{event.form} · {event.event_key.split(":").at(-1)}</h3>
        <dl className="decision-card__versions"><div><dt>SEC 接受时间</dt><dd>{formatDisplayTime(event.accepted_at)}</dd></div><div><dt>首次发现时间</dt><dd>{formatDisplayTime(event.first_seen_at)}</dd></div></dl>
        {event.metadata_status === "conflict" ? <p>公告元数据出现冲突，后续处理状态待核验。</p> : null}
        {review && (review.status === "unresolved" || review.status === "addressed") ? <>
          <p>本次日研判断 · {review.status === "unresolved" ? "未决" : "已回答"}</p>
          <p>{review.rationale}</p>
        </> : null}
        {event.preparation_rejection ? <>
          <p>原增量在准备阶段被拒，未创建研究任务。</p>
          <p>{event.preparation_rejection.reason === "account_unavailable"
            ? "当时账户占用或结算状态未核验通过。"
            : "原回执未记录具体拦截条件。"}</p>
          <p>原拒绝时间：<time dateTime={event.preparation_rejection.rejected_at}>
            {formatDisplayTime(event.preparation_rejection.rejected_at)}
          </time></p>
        </> : event.research_status ? <p>{researchStatuses[event.research_status]}</p> : null}
        {event.notice ? <NoticeReceipt notice={event.notice} /> : <p>暂无可核验通知记录。</p>}
      </article>;
    })}
    <p role="note">公告发现不代表通知已送达或研究已完成。研究有效期、旧事件一次限制、每日增量名额与未知费用仍独立核验；本区不表示可以发起研究或交易，交易保持锁定。</p>
  </section>;
}
