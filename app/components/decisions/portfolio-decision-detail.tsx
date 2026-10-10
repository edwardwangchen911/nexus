import { AlertTriangle, Bot, Info } from "lucide-react";
import { formatDisplayTime } from "../../lib/display-format";
import { cycleDecisionPresentation } from "../../lib/product-status";
import type { components } from "../../lib/generated/schema";
import { instrumentLabel, TargetComparison } from "./target-comparison";
type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];
const actions = { include: "拟纳入", increase: "拟增加", reduce: "拟降低", exit: "拟退出", maintain: "维持目标", defer: "暂缓" };
export function PortfolioDecisionDetail({ detail, paperHref }: { detail: Detail; paperHref?: string }): React.ReactElement {
  const { active_target: active, proposal, synthesis, blocker, activation_result: activation } = detail;
  const plan = detail.blocking_plan ?? detail.plan_summary;
  const blocked = blocker?.category === "resource_occupied" || activation?.result_code === "TARGET_ACTIVATION_BLOCKED";
  const proposalState = blocked ? "未生效 · 等待重新校验" : activation?.target_version_id ? "已完成生效校验" : "未生效";
  const blockText = blocker ? ({ resource_occupied: "前序计划仍占用资源；须结清且无未知后重新校验。", policy_unconfirmed: "政策未确认；不生成默认配置。", proposal_rejected: "提案校验未通过；保留原目标。", provider_capacity: "研究输入容量受限；未形成完整提案。" })[blocker.category] : null;
  const terminal = detail.cycle.status === "unavailable" || detail.cycle.status === "failed"
    ? cycleDecisionPresentation(detail.cycle).summary : null;
  return <div className="portfolio-decision pm-stack">
    <div className="pm-conclusion" role="note" aria-label="目标生效结论"><AlertTriangle aria-hidden="true" size={30} /><div><h2>{proposal ? <>提案 <span className="pm-id" title={proposal.strategy_decision_id}>…{proposal.strategy_decision_id.slice(-4)}</span> {proposalState}；</> : blocker?.category === "policy_unconfirmed" ? "政策未确认；" : terminal ? `${terminal.label}；` : "尚无完整提案；"}{active ? `目标 V${active.version_seq} 继续生效` : "尚未建立目标"}</h2><p>{blockText ?? terminal?.detail ?? "目标生效、计划建立和模拟成交分别核对。"}</p></div>{plan ? <a className="button button--quiet" href={paperHref ?? `/paper?plan=${encodeURIComponent(plan.batch_id)}`}>查看调整计划</a> : null}</div>
    <dl className="pm-identity"><div><dt>当前生效目标</dt><dd>{active ? `V${active.version_seq} · 生效` : "尚未建立目标"}</dd></div><div><dt>本轮提案</dt><dd>{proposal ? proposalState : "未形成"}</dd></div><div><dt>判断时点</dt><dd className="numeric-data">{formatDisplayTime(detail.cycle.decision_time)}</dd></div></dl>
    <section className="pm-panel" aria-labelledby="portfolio-choice-title"><header className="pm-panel__heading"><h2 id="portfolio-choice-title">本轮组合取舍</h2><span>Agent 原始判断</span></header><div className="pm-agent"><Bot aria-hidden="true" size={28} /><div>{synthesis ? <><p>{synthesis.cash_rationale ?? "现金理由未提供"}</p>{synthesis.choices.map((choice) => <div key={choice.instrument_id}><p><strong title={choice.instrument_id}>{instrumentLabel(detail, choice.instrument_id)} · {actions[choice.action]}</strong>：{choice.thesis}</p>{choice.replaces_instrument_id ? <p>拟替换 {instrumentLabel(detail, choice.replaces_instrument_id)}</p> : null}{choice.depends_on_instrument_ids.length ? <p>依赖：{choice.depends_on_instrument_ids.map((id) => instrumentLabel(detail, id)).join("、")}</p> : null}</div>)}</> : <p>组合级 Agent 判断暂不可用；不从单股方向拼接。</p>}</div></div></section>
    <TargetComparison detail={detail} />
    {plan ? <section className="pm-panel pm-plan-link" aria-label="关联计划摘要"><h2>当前计划与风险归属</h2><p>调整计划 <span className="pm-id" title={plan.batch_id}>…{plan.batch_id.slice(-4)}</span> · 目标 <span className="pm-id" title={plan.target_version_id ?? undefined}>{plan.target_version_id ? `…${plan.target_version_id.slice(-4)}` : "历史记录/未关联目标"}</span></p><p>资源{plan.occupation_state === "occupied" ? "仍占用" : plan.occupation_state === "released" ? "已释放" : "尚未查明"}；风险、成交与未结覆盖在原计划中独立核对。</p></section> : null}
    <p className="pm-notice"><Info aria-hidden="true" size={18} />前序结清且占用释放后，提案仍须按最新状态与证据重新校验；不自动生效。</p>
    <details className="pm-panel pm-disclosure"><summary>查看政策版本与证据依据</summary><dl className="pm-evidence"><div><dt>提案完整标识</dt><dd>{proposal?.strategy_decision_id ?? "未提供"}</dd></div><div><dt>政策哈希</dt><dd>{proposal?.policy_hash ?? "未提供"}</dd></div><div><dt>提案有效期</dt><dd>{proposal ? formatDisplayTime(proposal.expires_at) : "未提供"}</dd></div><div><dt>目标生效时点</dt><dd>{active ? formatDisplayTime(active.as_of) : "尚未建立"}</dd></div><div><dt>目标完整标识</dt><dd>{active?.target_version_id ?? "尚未建立"}</dd></div><div><dt>记录代码</dt><dd>{blocker?.code ?? activation?.result_code ?? detail.cycle.result_code}</dd></div></dl>{synthesis?.choices.map((choice) => <div key={choice.instrument_id}><h3>{instrumentLabel(detail, choice.instrument_id)}</h3><p className="pm-id">完整标的标识：{choice.instrument_id}</p><p>反方证据：{choice.counter_thesis}</p><p>失效条件：{choice.invalidation}</p></div>)}</details>
  </div>;
}
