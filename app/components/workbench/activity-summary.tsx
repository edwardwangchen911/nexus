import { formatDisplayTime } from "../../lib/display-format";

export type ActivityLimitation = {
  source: "system" | "model"; kind: string; text: string; truncated: boolean; impact: string | null;
};
export type ActivityCandidate = {
  instrument_id: string; symbol: string; run_id: string | null; snapshot_id: string | null;
  research_as_of: string | null; run_status: string | null; job_status: string | null; quality: string | null;
  read_status: "verified" | "unavailable"; reason_code: string | null; href: string | null;
  limitations: ActivityLimitation[]; has_more: boolean;
};
export type ActivityScope = {
  mode: "research_and_paper" | "research_only" | "research_and_monitor"; today_registered: number | null; today_started: number | null;
  registration_date: string; decision_session: string | null; selected_decision_session?: string | null;
  selection: "active" | "historical_unresolved" | "today" | "historical" | "none" | "ambiguous" | "unavailable";
  request_id: string | null; cycle_id: string | null; requested_at: string | null; status: string | null;
  reason_code: string | null; trigger: "manual" | "scheduled" | null; historical: boolean;
  occupancy: "unresolved" | "released" | "not_indicated" | "unverified";
  portfolio_status: string | null; portfolio_reason_code: string | null; href: string | null;
  candidates: ActivityCandidate[];
};
export type WorkbenchActivity = { read_at: string; read_status: "complete" | "partial"; scopes: ActivityScope[] };

const selections: Record<ActivityScope["selection"], string> = {
  active: "当前活动轮次", historical_unresolved: "历史未决", today: "今日最近登记", historical: "最近历史记录",
  none: "今日无可关注轮次", ambiguous: "存在多个待核对对象", unavailable: "当前状态待核对",
};
const occupancyCopy: Record<ActivityScope["occupancy"], string> = {
  unresolved: "仍有未解除占用", released: "历史占用已按原合同处置，仅解除历史调度占用；原结果、未知用量、费用和预算预留仍按原记录保留，后续仍须重新通过准入",
  not_indicated: "未指示历史占用；不代表下一轮准入已通过", unverified: "占用状态尚未核验",
};
const statuses: Record<string, string> = {
  queued: "等待执行", preparing: "正在准备", prepared: "准备已完成", running: "运行中", researching: "研究中",
  completed: "本轮运行已完成", failed: "运行失败", unknown: "结果未知", cancelled: "已取消", stopped: "已停止",
  active: "本轮处理中", unavailable: "本轮不可用", decision_recorded: "决策已保存", risk_evaluated: "风控已核验", awaiting_market: "等待执行窗口", partially_filled: "部分成交",
  requested: "已登记", pending: "等待执行", settling: "正在收尾", finalizing: "正在收尾",
};
const portfolioStatuses: Record<string, string> = {
  deferred: "组合判断暂缓", no_order: "本轮未生成订单", no_order_required: "本轮无需下单",
  maintained: "已形成维持目标结论", maintain: "已形成维持目标结论", active_cash: "已形成主动保留现金结论",
  awaiting_cycle: "尚未创建周期，组合判断尚未开始", recorded: "组合结果已保存", unavailable: "组合结论暂不可核验",
  completed: "组合结果已保存", unknown: "组合结果未知", pending: "组合判断尚未完成", not_applicable: "本模式不适用",
};
const reasonCopy: Record<string, string> = {
  RESEARCH_OUTPUT_NOT_ACTIONABLE: "研究未通过交易准入；现有原因不说明具体资料缺口",
  MANUAL_PREPARATION_RESULT_UNKNOWN: "原准备操作结果未知", AGENT_PAPER_LOOP_RESULT_UNKNOWN: "原周期结果未知",
  RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN: "原研究执行结果仍未知",
  manual_unused_research_cancelled: "未使用的研究已按原取消记录收尾",
  RESEARCH_OUTPUT_UNAVAILABLE: "原研究输出暂不可核验", NO_ORDER_REQUIRED: "按已保存决定，本轮无需下单",
};

export function ActivitySummary({ activity }: { activity: unknown }): React.ReactElement {
  const present = activity !== undefined && activity !== null;
  const valid = validActivity(activity);
  return <section className="workbench-panel workbench-activity" aria-label="本轮运行与研究限制">
    <header className="workbench-panel__heading"><div><p className="eyebrow">已保存的本轮事实</p><h2>本轮运行与研究限制</h2></div></header>
    {!valid ? <p>{present ? "本轮摘要格式不可核验；未展示未核验正文。" : "当前服务尚未提供本轮运行摘要。"}</p> : <>
      <p>读回时间：{formatDisplayTime(activity.read_at)}。服务在线不代表业务已启动。</p>
      {activity.read_status === "partial" && <p>本轮读回不完整；已核验部分仍按原时点展示。</p>}
      {activity.scopes.map((scope) => <ScopeSummary key={scope.mode} scope={scope} />)}
    </>}
  </section>;
}

function ScopeSummary({ scope }: { scope: ActivityScope }): React.ReactElement {
  const title = scope.mode === "research_only" ? "纯研究" : scope.mode === "research_and_monitor" ? "独立研究与持续监控" : "正式研究与内部模拟";
  const scopeHref = safeDetailHref(scope.href);
  const uncertain = ["ambiguous", "unavailable"].includes(scope.selection);
  return <section aria-label={title} className="workbench-activity__scope">
    <h3>{title}</h3>
    <p>手动登记统计日期（纽约）：{scope.registration_date} · {countCopy(scope.today_registered, "今日手动", "登记", "请求")} · {countCopy(scope.today_started, scope.mode !== "research_and_paper" ? "今日" : "本交易日", "启动", scope.mode !== "research_and_paper" ? "研究请求" : "周期")}</p>
    <p>启动计数交易日：{scope.mode !== "research_and_paper" ? "本模式不适用，按纽约登记日期统计" : scope.decision_session ?? "尚未确定"}</p>
    <p><strong>{selections[scope.selection]}</strong>{scope.historical && scope.selection !== "historical_unresolved" ? "（历史记录）" : ""}
      {scope.selection !== "none" && <> · {uncertain ? "当前状态待核对" : statuses[scope.status ?? ""] ?? "运行状态待核对"}
        {!uncertain && scope.status === "preparing" && scope.cycle_id === null ? "；尚未创建周期" : ""}</>}</p>
    {scope.selection !== "none" && <>
      <dl className="workbench-readiness-facts">
        <div><dt>触发来源</dt><dd>{scope.trigger === "manual" ? "手动登记" : scope.trigger === "scheduled" ? "定时调度" : "尚未核验"}</dd></div>
        <div><dt>请求登记时间</dt><dd>{formatDisplayTime(scope.requested_at)}</dd></div>
        <div><dt>选中周期交易日</dt><dd>{scope.mode !== "research_and_paper" ? "本模式不适用" : scope.selected_decision_session ?? "尚未提供"}</dd></div>
        <div><dt>历史占用</dt><dd>{occupancyCopy[scope.occupancy]}</dd></div>
      </dl>
      <p>{scope.mode !== "research_and_paper" ? "组合决策与执行：本模式不适用" : uncertain ? "组合结论尚未核验" : portfolioStatuses[scope.portfolio_status ?? ""] ?? "组合结论尚未核验"}</p>
      {scope.reason_code && <Reason code={scope.reason_code} />}
      {scope.portfolio_reason_code && scope.portfolio_reason_code !== scope.reason_code && <Reason code={scope.portfolio_reason_code} />}
      {scope.candidates.length > 0 ? <div className="workbench-activity__candidates">
        {scope.candidates.map((candidate) => <CandidateSummary key={candidate.instrument_id} candidate={candidate} />)}
      </div> : <p>尚无可核验的冻结候选；不使用当前股票池补填。</p>}
      {scopeHref && !uncertain ? <p><a href={scopeHref}>查看本轮原记录</a></p> : <p>详情暂无法定位；请核对下方已保存的对象信息。</p>}
      <details><summary>查看本轮对象信息</summary><dl>
        <dt>请求</dt><dd><code>{scope.request_id ?? "未关联"}</code></dd>
        <dt>周期</dt><dd><code>{scope.cycle_id ?? "未关联"}</code></dd>
      </dl></details>
    </>}
  </section>;
}

function CandidateSummary({ candidate }: { candidate: ActivityCandidate }): React.ReactElement {
  const valid = candidate.read_status === "verified";
  const href = valid ? safeDetailHref(candidate.href) : null;
  const limitations = valid ? candidate.limitations : [];
  const runStatus = candidate.job_status === "running" ? "研究中"
    : candidate.job_status === "queued" ? "等待执行"
      : !valid ? "研究状态尚未核验"
        : candidate.reason_code === "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN" ? "研究结果未知"
          : candidate.run_status === "abstained" && candidate.job_status === "skipped"
            && candidate.reason_code === "manual_unused_research_cancelled" ? "未执行（已核验取消）"
            : candidate.run_status === "completed" ? "研究已完成"
              : candidate.run_status === "failed" ? "研究失败"
                : candidate.run_status === "unknown" ? "研究结果未知" : "研究状态尚未核验";
  return <article aria-label={`${candidate.symbol} 本轮研究`} className="workbench-activity__candidate">
    <h4>{candidate.symbol}</h4>
    <p><strong>{runStatus}</strong> · {valid && candidate.quality === "partial" ? "结论受限" : valid && candidate.quality === "complete" ? "研究内容完整；采用资格仍须独立核验" : "研究质量尚未核验"}</p>
    <p>研究证据截止：{formatDisplayTime(candidate.research_as_of)}</p>
    <p>本摘要未核验阶段轨迹。</p>
    {!valid ? <p>摘要暂不可核验；未展示模型正文。</p> : <>
      {limitations.length ? <ul>{limitations.slice(0, 2).map((item, index) => <Limitation key={index} item={item} />)}</ul>
        : <p>{candidate.quality === "partial" ? "结论受限，细分原因未记录／未核验。" : "未提供具体研究限制；不代表已通过正式准入。"}</p>}
      {limitations.length > 2 && <details><summary>展开更多已保存限制</summary><ul>{limitations.slice(2).map((item, index) => <Limitation key={index} item={item} />)}</ul></details>}
      {candidate.has_more && <p>还有未在摘要中展开的内容，请查阅原研究。</p>}
      {candidate.reason_code && <Reason code={candidate.reason_code} />}
    </>}
    {href ? <p><a href={href}>查看 {candidate.symbol} 原研究</a></p> : <p>原研究详情暂无法定位。</p>}
    <details><summary>查看 {candidate.symbol} 冻结对象</summary><dl>
      <dt>研究 Run</dt><dd><code>{candidate.run_id ?? "未关联"}</code></dd>
      <dt>冻结快照</dt><dd><code>{candidate.snapshot_id ?? "未关联"}</code></dd>
    </dl></details>
  </article>;
}

function Limitation({ item }: { item: ActivityLimitation }): React.ReactElement {
  return <li><p><strong>{item.kind === "review_status" && item.source === "system" ? "已保存审查状态" : item.source === "system" ? "系统核验" : "模型陈述"}</strong>
    {item.kind === "uncertainties" ? " · 研究限制／未来不确定性" : ""}{item.truncated ? " · 截断摘录" : ""}</p>
    <p>{item.text}</p>{item.impact && <p>{item.source === "model" ? "模型指出的受限结论" : "已记录的阻断影响"}：{item.impact}</p>}
    {item.source === "model" && <p>引用与绑定已核验；模型所述限制不等于系统已独立证实。</p>}
  </li>;
}
function Reason({ code }: { code: string }): React.ReactElement {
  return <p>{reasonCopy[code] ?? "已保存固定原因；具体含义请核对原记录"} · <code>{Object.hasOwn(reasonCopy, code) || /^[A-Z][A-Z0-9_]{0,120}$/.test(code) ? code : "原因不可核验"}</code></p>;
}
function countCopy(count: number | null, period: string, verb: string, unit: string): string {
  return count === null ? `${period}${verb}情况待核对` : count === 0 ? `${period}未${verb}` : `${period}已${verb} ${count} ${unit}`;
}
function safeDetailHref(href: string | null): string | null {
  const uuid = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
  return href && new RegExp(`^/(?:decisions/${uuid}|research/${uuid}\\?run_id=${uuid})$`).test(href) ? href : null;
}
function plain(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function nullableString(value: unknown): boolean { return value === null || typeof value === "string"; }
function nullableCount(value: unknown): boolean { return value === null || (Number.isSafeInteger(value) && Number(value) >= 0); }
export function validActivity(value: unknown): value is WorkbenchActivity {
  return plain(value) && typeof value.read_at === "string" && ["complete", "partial"].includes(String(value.read_status))
    && Array.isArray(value.scopes) && value.scopes.length <= 3 && new Set(value.scopes.map((scope) => plain(scope) ? scope.mode : null)).size === value.scopes.length
    && value.scopes.every((scope) => plain(scope) && ["research_and_paper", "research_only", "research_and_monitor"].includes(String(scope.mode))
      && (scope.selected_decision_session === undefined || nullableString(scope.selected_decision_session))
      && nullableCount(scope.today_registered) && nullableCount(scope.today_started) && typeof scope.registration_date === "string"
      && ["active", "historical_unresolved", "today", "historical", "none", "ambiguous", "unavailable"].includes(String(scope.selection))
      && [null, "manual", "scheduled"].includes(scope.trigger as null | string) && typeof scope.historical === "boolean"
      && ["unresolved", "released", "not_indicated", "unverified"].includes(String(scope.occupancy))
      && ["decision_session", "request_id", "cycle_id", "requested_at", "status", "reason_code", "portfolio_status", "portfolio_reason_code", "href"].every((key) => nullableString(scope[key]))
      && Array.isArray(scope.candidates) && scope.candidates.length <= 5 && scope.candidates.every(validCandidate));
}
function validCandidate(value: unknown): boolean {
  return plain(value) && typeof value.instrument_id === "string" && typeof value.symbol === "string"
    && ["run_id", "snapshot_id", "research_as_of", "run_status", "job_status", "quality", "reason_code", "href"].every((key) => nullableString(value[key]))
    && ["verified", "unavailable"].includes(String(value.read_status)) && typeof value.has_more === "boolean"
    && Array.isArray(value.limitations) && value.limitations.length <= 20 && value.limitations.every((item) => plain(item)
      && ["system", "model"].includes(String(item.source)) && typeof item.kind === "string" && typeof item.text === "string"
      && typeof item.truncated === "boolean" && nullableString(item.impact));
}
