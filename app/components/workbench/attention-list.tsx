import { AlertTriangle, ArrowRight, CircleCheck } from "lucide-react";

import { attentionPresentation } from "../../lib/product-status";
import type { components } from "../../lib/generated/schema";

type Attention = components["schemas"]["WorkbenchAttentionResponse"];

const actionLabels: Record<Attention["safe_action"], string> = {
  view_evidence: "查看证据",
  view_decision: "查看决策",
  verify_original_result: "核对原操作结果",
  wait: "查看运行状态",
};

export function AttentionList({ items }: { items: Attention[] }): React.ReactElement {
  return (
    <section className="workbench-panel workbench-attention" aria-label="需要处理">
      <header className="workbench-panel__heading">
        <div><p className="eyebrow">ATTENTION</p><h2>需要处理</h2></div>
        {items.length > 0 ? <span className="workbench-attention__count numeric-data">{items.length}</span> : null}
      </header>
      {items.length === 0 ? <div className="workbench-attention__clear">
        <CircleCheck aria-hidden="true" size={22} /><div><strong>目前无需处理</strong><p>没有已保存的异常或待核对事项。</p></div>
      </div> : <ul>
        {items.map((item, index) => {
          const view = attentionPresentation(item);
          return <li key={`${item.kind}-${item.reason_code}-${index}`} data-tone={view.tone}>
            <AlertTriangle aria-hidden="true" size={19} />
            <div><strong>{view.label}</strong><p>{view.detail}</p><code>{view.rawCode}</code>
              <a href={view.href}>{actionLabels[item.safe_action]}<ArrowRight aria-hidden="true" size={15} /></a>
            </div>
          </li>;
        })}
      </ul>}
    </section>
  );
}
