"use client";
import { AlertTriangle, CheckCircle2, Info } from "lucide-react";
import { useEffect, useState } from "react";
import type { components } from "../../lib/generated/schema";
import { statusLabel } from "../../lib/display-labels";
import { formatPercent } from "../../lib/display-format";
import { formatPaperDecimal, formatPaperTime } from "../order-timeline";
import { CancelResultCheck, type CancelAttempt } from "./cancel-result-check";
type Plan = components["schemas"]["PaperPlanResponse"];
type Summary = components["schemas"]["PaperManagementSummaryResponse"];
export const occupationLabel = (state: Plan["plan"]["occupation_state"]): string => ({ occupied: "资源仍占用", released: "资源已释放", unknown: "资源尚未查明" })[state];
const money = (value: string | null): string => value === null ? "暂不可用" : `USD ${formatPaperDecimal(value, 2)}`;

export function AdjustmentPlanLoader({ refreshVersion = 0, attempt = null }: { refreshVersion?: number; attempt?: CancelAttempt | null }): React.ReactElement {
  const [plan, setPlan] = useState<Plan | null>(null);
  const [currentTarget, setCurrentTarget] = useState<Summary["current_target"]>(null);
  const [message, setMessage] = useState("正在读取调整计划");
  useEffect(() => {
    const controller = new AbortController();
    async function read(): Promise<void> {
      try {
        const requestedId = new URLSearchParams(window.location.search).get("plan");
        let id = requestedId;
        if (!id) {
          const response = await fetch("/api/v1/paper/management/portfolio", { cache: "no-store", signal: controller.signal });
          if (response.status === 401 || response.status === 403) { if (!controller.signal.aborted) { setPlan(null); setCurrentTarget(null); setMessage("无权读取调整计划"); } return; }
          if (!response.ok) throw new Error("summary unavailable");
          const envelope = await response.json() as { data: Summary };
          id = envelope.data.occupying_plan?.batch_id ?? null;
          if (!controller.signal.aborted) setCurrentTarget(id ? null : envelope.data.current_target);
        }
        if (!id) { if (!controller.signal.aborted) { setPlan(null); setMessage("暂无资源占用中的调整计划；历史记录见下方。"); } return; }
        const response = await fetch(`/api/v1/paper/plans/${encodeURIComponent(id)}`, { cache: "no-store", signal: controller.signal });
        if (response.status === 401 || response.status === 403) { if (!controller.signal.aborted) { setPlan(null); setCurrentTarget(null); setMessage("无权读取调整计划"); } return; }
        if (!response.ok) throw new Error("plan unavailable");
        const envelope = await response.json() as { data?: Plan };
        if (!envelope.data || envelope.data.batch_id !== id || !Array.isArray(envelope.data.plan?.items)) throw new Error("plan mismatch");
        if (!controller.signal.aborted) { setPlan(envelope.data); setCurrentTarget(null); setMessage(""); }
      } catch { if (!controller.signal.aborted) setMessage("调整计划暂不可用；已有事实仅为最后已知记录。"); }
    }
    void read(); return () => controller.abort();
  }, [refreshVersion]);
  return <>{message ? <p className="pm-footnote">{message}</p> : null}{plan ? <AdjustmentPlan plan={plan} attempt={attempt} /> : currentTarget ? <CurrentTargetDetail target={currentTarget} /> : null}</>;
}

function CurrentTargetDetail({ target }: { target: NonNullable<Summary["current_target"]> }): React.ReactElement {
  return <section className="pm-panel" aria-label="当前目标详情">
    <header className="pm-panel__heading"><h2>当前目标 V{target.version_seq}</h2><p>当前无资源占用计划；不据此推断无需交易、计划已结清或目标已达成。</p></header>
    <table className="pm-table" aria-label="当前目标确定性配置"><thead><tr><th scope="col">标的标识</th><th scope="col">目标权重</th></tr></thead><tbody>{target.targets.map((row) => <tr key={row.instrument_id}><th scope="row" title={row.instrument_id}>标的 …{row.instrument_id.slice(-4)}</th><td data-label="目标权重">{formatPercent(row.target_weight)}</td></tr>)}<tr><th scope="row">现金</th><td data-label="目标现金权重">{formatPercent(target.cash_weight)}</td></tr></tbody></table>
    <div className="pm-panel__body"><p>目标生效时点 {formatPaperTime(target.as_of)}</p><p className="pm-id">目标完整标识：{target.target_version_id}</p><p>权重分母：本目标完整配置。实际账户是独立快照，不计算目标金额或实际权重，也不跨快照相减。</p><a className="button button--quiet" href="#paper-actual-facts">查看独立实际账户事实</a></div>
  </section>;
}

export function AdjustmentPlan({ plan, attempt = null }: { plan: Plan; attempt?: CancelAttempt | null }): React.ReactElement {
  const boundAttempt = attempt && plan.plan.items.some((item) => item.order_id === attempt.orderId) ? attempt : null;
  const fills = plan.plan.items.flatMap((item) => item.confirmed_fills.map((fill) => ({ fill, orderId: item.order_id }))).sort((a, b) => Date.parse(a.fill.as_of) - Date.parse(b.fill.as_of));
  return <section className="pm-stack pm-adjustment" aria-label="当前调整计划">
    <header className="pm-plan-title"><h2>调整计划 <span className="pm-id" title={plan.batch_id}>…{plan.batch_id.slice(-4)}</span></h2><p>{plan.target ? `归属目标 V${plan.target.version_seq}` : "历史记录/未关联目标"} · 内部模拟 · 实盘锁定</p></header>
    <div className="pm-conclusion" role="note" aria-label="计划与资源结论"><AlertTriangle size={28} aria-hidden="true" /><div><h3>{occupationLabel(plan.plan.occupation_state)}</h3><p>{plan.plan.items.map((item) => item.status ? statusLabel(item.status) : "订单结果未知").join(" · ") || "未提供订单事实"}{boundAttempt ? " · 原取消结果待核对" : ""}</p><p>计划、订单、成交与占用分别核对；不据此自动生效提案。</p></div></div>
    <div className="pm-plan-grid"><CancelResultCheck attempt={boundAttempt} planId={plan.batch_id} targetId={plan.target?.target_version_id} /><section className="pm-panel pm-timeline" aria-labelledby="plan-timeline-title"><header className="pm-panel__heading"><h2 id="plan-timeline-title">执行时间线</h2></header><ol>
      {plan.target ? <li><CheckCircle2 aria-hidden="true" /><div><time>{formatPaperTime(plan.target.as_of)}</time><h3>目标 V{plan.target.version_seq} 生效</h3><p className="pm-id">{plan.target.target_version_id}</p></div></li> : null}
      <li><CheckCircle2 aria-hidden="true" /><div><time>{formatPaperTime(plan.plan.plan_created_at)}</time><h3>调整计划建立</h3><p>计划建立不代表成交或资源释放。</p></div></li>
      {fills.map(({ fill, orderId }) => <li key={fill.fill_id}><CheckCircle2 aria-hidden="true" /><div><time dateTime={fill.as_of}>{formatPaperTime(fill.as_of)}</time><h3>已确认成交</h3><p title={orderId}>订单 …{orderId.slice(-4)}</p><p>{money(fill.notional_usd)} · {formatPaperDecimal(fill.quantity)} 股</p></div></li>)}
      <li><Info aria-hidden="true" /><div><time>{formatPaperTime(plan.plan.reconciliation_as_of)}</time><h3>最近核对快照</h3><p>{occupationLabel(plan.plan.occupation_state)}</p></div></li>
    </ol></section></div>
    <section className="pm-panel" aria-labelledby="plan-amount-title"><header className="pm-panel__heading"><h2 id="plan-amount-title">金额核对 · USD</h2><p>确定性风控结果；不改写目标，不自动补单</p></header>{plan.plan.risk.map((risk) => <div className="pm-amount-row" key={risk.order_id}><p className="pm-id">订单 {risk.order_id}</p><dl className="pm-amounts">{[["调整需求", risk.desired_amount_usd], ["风控获准", risk.approved_amount_usd], ["风控缩减", risk.reduced_amount_usd], ["风控拒绝", risk.rejected_amount_usd]].map(([label, value]) => <div key={label}><dt>{label}</dt><dd className="numeric-data">{money(value)}</dd></div>)}</dl></div>)}</section>
    {plan.plan.items.map((item) => <section className="pm-panel pm-panel__body" key={item.order_id} aria-label={`订单核对 ${item.order_id}`}><h3 className="pm-id">订单 {item.order_id}</h3><dl className="pm-amounts"><div><dt>已确认成交数量</dt><dd>{formatPaperDecimal(item.confirmed_fill_quantity)} 股</dd></div><div><dt>未结订单覆盖</dt><dd>{money(item.open_order_coverage_amount_usd)}</dd></div><div><dt>剩余目标差额</dt><dd>{money(item.remaining_target_gap_amount_usd)}</dd></div><div><dt>现金预留未结清</dt><dd>{money(item.cash_reservation_outstanding)}</dd></div><div><dt>持仓预留未结清</dt><dd>{item.position_reservation_outstanding === null ? "不适用或暂不可用" : `${formatPaperDecimal(item.position_reservation_outstanding)} 股`}</dd></div></dl><p className="pm-footnote">差额不等于新下单需求；未结订单已覆盖的部分不可重复创建。订单时点 {item.order_as_of ? formatPaperTime(item.order_as_of) : "未知"}；预留时点 {item.reservation_as_of ? formatPaperTime(item.reservation_as_of) : "未知"}。</p></section>)}
    <section className="pm-panel" aria-labelledby="plan-actual-title"><header className="pm-panel__heading"><h2 id="plan-actual-title">持仓与现金核对 · 最后确认事实</h2><p>{plan.actual.as_of ? formatPaperTime(plan.actual.as_of) : "快照时点未知"}</p></header><dl className="pm-amounts pm-panel__body"><div><dt>可用现金</dt><dd>{money(plan.actual.available_cash)}</dd></div><div><dt>预留现金</dt><dd>{money(plan.actual.reserved_cash)}</dd></div><div><dt>账户净资产 NAV</dt><dd>{money(plan.actual.nav)}</dd></div></dl><table className="pm-table" aria-label="计划实际持仓"><thead><tr><th scope="col">标的标识</th><th scope="col">已确认数量</th><th scope="col">预留数量</th></tr></thead><tbody>{plan.actual.positions?.map((position) => <tr key={position.instrument_id}><th scope="row" className="pm-id">{position.instrument_id}</th><td data-label="已确认数量">{formatPaperDecimal(position.quantity)} 股</td><td data-label="预留数量">{formatPaperDecimal(position.reserved_quantity)} 股</td></tr>)}</tbody></table>{plan.actual.positions === null ? <p className="pm-footnote">持仓事实暂不可用</p> : null}<p className="pm-footnote">目标金额、实际权重与现金总额未提供，暂不可用；不同快照不直接相减。</p></section>
  </section>;
}
