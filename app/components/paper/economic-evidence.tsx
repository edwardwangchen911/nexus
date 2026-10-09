"use client";

import { useEffect, useState } from "react";
import type { components } from "../../lib/generated/schema";
import { formatPaperTime } from "../order-timeline";

type Evidence = components["schemas"]["PortfolioEconomicEvidenceResponse"];

const blockerLabels = {
  MODEL_AND_CANDIDATE_AUTHORITIES_UNBOUND: "候选与模型权威尚未绑定",
  CALENDAR_AUTHORITY_UNBOUND: "交易日历权威尚未绑定",
  DUAL_LANE_ENTRYPOINT_UNIMPLEMENTED: "双侧评价运行入口尚未实现",
  ATOMIC_GLOBAL_BUDGET_RESERVATION_UNIMPLEMENTED: "全局双侧预算预留尚未实现",
  COMMON_EXECUTION_WINDOW_UNVERIFIED: "共同合法执行窗口尚未验证",
  FIXED_STRATEGY_BASELINE_UNIMPLEMENTED: "固定策略基线尚未实现",
  CLOSE_PRICE_AND_COST_AUTHORITIES_UNBOUND: "收盘价格与成本权威尚未绑定",
  REAL_EVALUATION_ACTIVATION_NOT_AUTHORIZED: "实际评价运行尚未授权",
  DAILY_LIMIT_BELOW_EVALUATION_CEILING: "现行日预算不足以覆盖完整双侧评价",
} as const;

function valid(value: unknown): value is Evidence {
  if (typeof value !== "object" || value === null) return false;
  const data = value as Evidence;
  const protocol = data.protocol ?? null;
  const registered = data.protocol_status === "registered_not_activated";
  const protocolValid = registered ? protocol !== null
    && /^[0-9a-f]{64}$/.test(protocol.protocol_hash)
    && Number.isFinite(Date.parse(protocol.registered_at))
    && protocol.daily_model_jobs === 6 && protocol.daily_sdk_calls === 14
    && protocol.daily_model_input_cap === 576000 && protocol.daily_wire_input_cap === 512000
    && protocol.daily_output_cap === 49152 && protocol.serial_invocation_ceiling_seconds === 570
    && Array.isArray(protocol.activation_blockers) && protocol.activation_blockers.length > 0
    && protocol.activation_blockers.every((code) => Object.hasOwn(blockerLabels, code))
    && Array.isArray(protocol.slots) && protocol.slots.length === 24
    && protocol.slots.every((slot, index) => slot.ordinal === index + 1
      && slot.phase === (index < 4 ? "training" : "oos") && Number.isFinite(Date.parse(slot.decision_at))
      && (index === 0 || Date.parse(slot.decision_at) > Date.parse(protocol.slots[index - 1].decision_at)))
    && data.planned_slot_count === 24 : protocol === null && data.planned_slot_count === null;
  return protocolValid && data.execution_locked === true
    && ["not_implemented", "registered_not_activated", "unavailable"].includes(data.protocol_status)
    && data.economic_status === "not_evaluated"
    && data.economic_pair_count === null && typeof data.as_of === "string"
    && Number.isFinite(Date.parse(data.as_of))
    && ["ready", "insufficient", "unavailable"].includes(data.source_status)
    && Array.isArray(data.ordinary_samples) && data.ordinary_samples.length <= 20
    && (data.source_status === "ready" ? data.ordinary_samples.length > 0 : data.ordinary_samples.length === 0)
    && data.ordinary_samples.every((sample) => typeof sample === "object" && sample !== null
      && typeof sample.cycle_id === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(sample.cycle_id)
      && typeof sample.decision_slot_at === "string" && Number.isFinite(Date.parse(sample.decision_slot_at))
      && ["no_order", "orders_terminal"].includes(sample.execution_state)
      && ["missing", "account_observed"].includes(sample.performance_state));
}

export function EconomicEvidenceLoader(): React.ReactElement {
  const [data, setData] = useState<Evidence | null>(null);
  const [state, setState] = useState<"loading" | "ready" | "unknown">("loading");
  useEffect(() => {
    const controller = new AbortController();
    void (async () => {
      try {
        const response = await fetch("/api/v1/paper/economic-evidence", { cache: "no-store", signal: controller.signal });
        if (!response.ok) throw new Error("unavailable");
        const envelope = await response.json() as { data?: unknown };
        if (!valid(envelope.data)) throw new Error("invalid");
        if (!controller.signal.aborted) { setData(envelope.data); setState("ready"); }
      } catch {
        if (!controller.signal.aborted) setState("unknown");
      }
    })();
    return () => controller.abort();
  }, []);
  return <section className="paper-table-section" aria-labelledby="portfolio-economic-evidence-title">
    <header><h2 id="portfolio-economic-evidence-title">组合经济对照 · 证据与缺口</h2></header>
    {state === "loading" ? <p role="status">正在读取组合复盘证据。</p> : null}
    {state === "unknown" ? <p role="status">组合经济证据暂不可读；样本数量与评价状态未知。</p> : null}
    {data ? <>
      {data.protocol_status === "not_implemented" ? <>
        <p>多股票经济评价协议与双账户执行入口尚未实现。</p>
        <p>经济配对与计划槽位数量：尚未具备权威，无法统计</p>
      </> : null}
      {data.protocol_status === "unavailable" ? <p>评价协议登记证据不可核实，计划数量未知。</p> : null}
      {data.protocol ? <>
        <p>评价协议已登记，尚未激活。</p>
        <p>计划槽位：24（训练 4 / 样本外 20）；经济配对数量仍未知。</p>
        <p>每日上限：{data.protocol.daily_model_jobs} 个模型任务 / {data.protocol.daily_sdk_calls} 次 SDK 调用。当前未启动评价。</p>
        <p>每日输入预留上限 {data.protocol.daily_model_input_cap} token，入口输入上限 {data.protocol.daily_wire_input_cap} token，输出上限 {data.protocol.daily_output_cap} token。</p>
        <p>串行调用时限合计 {data.protocol.serial_invocation_ceiling_seconds} 秒；不含排队、准入和归档发布，不能保证共同执行窗口。</p>
        <p>协议哈希 <code>{data.protocol.protocol_hash}</code></p>
        <ul aria-label="经济评价激活缺口">{data.protocol.activation_blockers.map((code) => <li key={code}>{blockerLabels[code]}</li>)}</ul>
        <div className="table-scroll"><table className="paper-comparison-table" aria-label="经济评价计划槽位">
          <thead><tr><th scope="col">序号</th><th scope="col">阶段</th><th scope="col">计划决策时点</th></tr></thead>
          <tbody>{data.protocol.slots.map((slot) => <tr key={slot.ordinal}><td>{slot.ordinal}</td><td>{slot.phase === "training" ? "训练" : "样本外"}</td><td>{formatPaperTime(slot.decision_at)}</td></tr>)}</tbody>
        </table></div>
      </> : null}
      <p>经济结果尚未评估，不能证明策略有效或样本外通过。</p>
      <p>真实运行仍需完成权威绑定、双侧执行和经济比较证据；协议登记不代表策略有效。</p>
      {data.source_status === "unavailable" ? <p>普通组合复盘证据暂不可核实，数量未知。</p> : <p>普通组合复盘样本：{data.ordinary_samples.length}（最多读取 20 条）</p>}
      {data.source_status === "insufficient" ? <p>尚无符合当前复盘合同的完整组合样本。</p> : null}
      <p>普通样本属于当前冻结版本的可收集观察，不是新旧策略的经济配对；未下单与绩效可读均不证明策略有效。</p>
      {data.ordinary_samples.length > 0 ? <div className="table-scroll"><table className="paper-comparison-table" aria-label="普通组合复盘证据"><thead><tr><th scope="col">原定决策槽位</th><th scope="col">执行证据</th><th scope="col">绩效证据</th><th scope="col">来源</th></tr></thead><tbody>{data.ordinary_samples.map((sample) => <tr key={sample.cycle_id}>
        <td>{formatPaperTime(sample.decision_slot_at)}</td>
        <td>{sample.execution_state === "no_order" ? "未下单" : "关联订单已终态"}</td>
        <td>{sample.performance_state === "account_observed" ? "已有账户绩效观察（非成效结论）" : "绩效证据缺失"}</td>
        <td><a href={`/decisions/${sample.cycle_id}`}>查看关联决策</a></td>
      </tr>)}</tbody></table></div> : null}
      <p>读取时点 {formatPaperTime(data.as_of)} · 执行锁保持开启 · 页面只读</p>
    </> : null}
  </section>;
}
