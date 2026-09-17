import { formatDisplayTime } from "../../lib/display-format";
import type { components } from "../../lib/generated/schema";

type Workbench = components["schemas"]["WorkbenchResponse"];
type Readiness = components["schemas"]["WorkbenchReadinessResponse"];

const stages: Record<Readiness["stage"], string> = {
  window: "等待有效运行窗口", data: "等待数据采集 / 核对", agent: "等待 Agent",
  risk: "等待确定性风控", execution: "等待模拟执行", recovery: "等待恢复 / 核对原结果",
  completed: "本周期已完成", unknown: "阶段未知",
};
const blockers: Record<Readiness["blocker"], string> = {
  none: "未记录阻断", data_missing: "所需数据缺失", stale: "证据已过期",
  unbound: "所需权威未绑定或不匹配", research_not_actionable: "研究未通过交易准入",
  risk_rejected: "确定性风控拒绝", result_unknown: "原操作结果未知",
  incompatible: "服务 / 版本不兼容", unknown: "原因尚不能确定",
};
const changes: Record<Readiness["changes"][number], string> = {
  stage: "周期阶段", blocker: "阻断分类", result: "结果代码", risk: "风控结论", orders: "订单数量",
};
const nextReasons: Record<NonNullable<Workbench["session"]["next_eligible_reason"]>, string> = {
  CURRENT_SLOT: "当前五分钟槽", NEXT_SLOT: "下一五分钟槽", SESSION_OPEN: "本交易日开始",
  NEXT_SESSION: "下一交易日开始", CURRENT_CYCLE_IN_PROGRESS: "当前周期结束后再判断",
  CALENDAR_UNAVAILABLE: "等待交易日历证据",
};

export function ReadinessSummary({ workbench }: { workbench: Workbench }): React.ReactElement {
  const { readiness, compatibility, session, loop } = workbench;
  const incompatible = compatibility?.status === "incompatible";
  const recovery = incompatible || (loop.status === "offline" && ["in_progress", "ready"].includes(session.status));
  const nextReason = session.next_eligible_reason ? nextReasons[session.next_eligible_reason] : "下一运行时间未知";
  return <section className="workbench-panel workbench-readiness" aria-label="运行就绪与阻断">
    <header className="workbench-panel__heading"><div><p className="eyebrow">只读运行事实</p><h2>运行就绪与阻断</h2></div></header>
    <dl className="workbench-readiness-facts">
      <div><dt>当前阶段</dt><dd>{recovery ? stages.recovery : readiness ? stages[readiness.stage] : "当前服务尚未提供运行解释"}</dd></div>
      <div><dt>本周期阻断</dt><dd>{readiness ? blockers[readiness.blocker] : "原因尚不能确定"}</dd></div>
      <div><dt>下一可运行时段</dt><dd>{session.next_eligible_at ? `${formatDisplayTime(session.next_eligible_at)} · ${nextReason}` : nextReason}</dd></div>
      <div><dt>服务兼容性</dt><dd>{incompatible ? "服务 / 版本不兼容" : compatibility?.status === "compatible" ? "代码、迁移与心跳报告匹配" : "兼容状态未知"}</dd></div>
    </dl>
    {readiness ? <p>已保存阶段：{stages[readiness.stage]} · <code>{readiness.reason_code}</code></p> : null}
    {readiness?.blocker === "research_not_actionable" ? <p>现有记录只证明研究未通过交易准入；不据此推断具体资料缺口，也不等同于已形成充分证据的不调整结论。</p> : null}
    {readiness?.reason_code === "OPEN_ORDER_EXISTS" ? <p>已有订单尚未完成，等待原订单的执行结果。</p> : null}
    {readiness?.blocker === "result_unknown" ? <p>先核对原周期和订单结果；此处不提供重放或恢复按钮。</p> : null}
    <p>{readiness?.previous_cycle_id
      ? <>与本交易日上一周期相比：{readiness.changes.length ? `变化项为${readiness.changes.map((key) => changes[key]).join("、")}` : "阶段、阻断、结果、风控和订单数量均无变化"}。<a href={`/decisions/${readiness.previous_cycle_id}`}>查看上一周期</a></>
      : readiness ? "尚无本交易日上一周期可供比较。" : "当前服务尚未提供周期比较。"}</p>
    {readiness?.previous_result_code ? <p>上一周期 <code>{readiness.previous_result_code}</code> → 本周期 <code>{workbench.latest_cycle?.result_code ?? "未知"}</code></p> : null}
    <details><summary>查看版本与迁移证据</summary>
      <dl className="workbench-readiness-facts">
        <div><dt>代码预期进程</dt><dd>{compatibility?.expected_process_version ?? "未知"}</dd></div>
        <div><dt>心跳报告进程</dt><dd>{compatibility?.reported_process_version ?? "未知"}</dd></div>
        <div><dt>代码预期迁移</dt><dd>{compatibility?.expected_migration_head ?? "未知"}</dd></div>
        <div><dt>数据库迁移</dt><dd>{compatibility?.database_migration_heads.join("、") || "未知"}</dd></div>
        <div><dt>心跳报告迁移</dt><dd>{compatibility?.reported_migration_head ?? "未知"}</dd></div>
      </dl>
    </details>
    <p>阶段来自持久化记录；心跳只证明最近存活，版本匹配不证明已加载当前 Git 提交。下一时段不是启动承诺。刷新不会触发行情、模型、订单或恢复。</p>
  </section>;
}
