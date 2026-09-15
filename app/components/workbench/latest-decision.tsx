import { ArrowRight, Bot, ShieldCheck } from "lucide-react";

import { formatDisplayTime } from "../../lib/display-format";
import { cycleDecisionPresentation, executionPresentation, riskPresentation } from "../../lib/product-status";
import type { components } from "../../lib/generated/schema";

type Cycle = components["schemas"]["AgentPaperCycleResponse"];
type Order = components["schemas"]["PaperOrderResponse"];

export function LatestDecision({ cycle, orders }: { cycle: Cycle | null; orders: Order[] }): React.ReactElement {
  if (!cycle) {
    return <section className="workbench-panel workbench-decision workbench-panel--empty" aria-label="最新决策">
      <header><div><p className="eyebrow">LATEST DECISION</p><h2>最新决策</h2></div></header>
      <p>尚无已保存的 Agent 决策。页面刷新不会自动运行决策。</p>
    </section>;
  }
  const decision = cycleDecisionPresentation(cycle);
  const risk = riskPresentation(cycle);
  const linkedOrders = orders.filter((order) => cycle.order_ids.includes(order.order_id));
  const execution = executionPresentation(cycle, linkedOrders);
  return (
    <section className="workbench-panel workbench-decision" aria-label="最新决策">
      <header className="workbench-panel__heading">
        <div><p className="eyebrow">LATEST DECISION</p><h2>最新决策</h2></div>
        <time className="numeric-data" dateTime={cycle.decision_time}>{formatDisplayTime(cycle.decision_time)}</time>
      </header>
      <div className="workbench-decision__summary">
        <Bot aria-hidden="true" size={24} />
        <div><span>Agent 结论</span><strong>{decision.summary.label}</strong><p>{decision.summary.detail}</p></div>
      </div>
      {decision.candidates.length > 0 ? <ul className="workbench-candidates" aria-label="决策候选">
        {decision.candidates.map((candidate) => <li key={candidate.instrumentId} data-tone={candidate.tone}>
          <span className="workbench-candidate__symbol numeric-data">{candidate.symbol}</span>
          <span className="workbench-candidate__stance">{candidate.label}</span>
          <span>{candidate.detail}</span>
        </li>)}
      </ul> : <p className="workbench-muted">本周期没有候选标的。</p>}
      <div className="workbench-decision__gates">
        <div data-tone={risk.tone}><ShieldCheck aria-hidden="true" size={18} /><span><small>确定性风控</small><strong>{risk.label}</strong></span></div>
        <div data-tone={execution.tone}><ArrowRight aria-hidden="true" size={18} /><span><small>执行结果</small><strong>{execution.label}</strong></span></div>
      </div>
      <footer>
        <span className="numeric-data">周期 {cycle.cycle_id.slice(0, 8)}</span>
        <a className="button button--primary" href={`/decisions/${cycle.cycle_id}`}>查看完整决策<ArrowRight aria-hidden="true" size={17} /></a>
      </footer>
    </section>
  );
}
