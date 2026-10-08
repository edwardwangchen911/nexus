"use client";

import { useEffect, useState } from "react";
import type { components } from "../../lib/generated/schema";
import { formatPaperTime } from "../order-timeline";

type Evidence = components["schemas"]["PortfolioEconomicEvidenceResponse"];

function valid(value: unknown): value is Evidence {
  if (typeof value !== "object" || value === null) return false;
  const data = value as Evidence;
  return data.execution_locked === true && data.protocol_status === "not_implemented"
    && data.economic_status === "not_evaluated" && data.planned_slot_count === null
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
      <p>多股票经济评价协议与双账户执行入口尚未实现。</p>
      <p>经济配对与计划槽位数量：尚未具备权威，无法统计</p>
      <p>经济结果尚未评估，不能证明策略有效或样本外通过。</p>
      <p>待确认完整评价协议：起点、训练与样本外窗口、研究共享边界、成本、基准和判定门槛。</p>
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
