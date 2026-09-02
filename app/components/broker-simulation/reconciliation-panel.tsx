import { statusLabel } from "../../lib/display-labels";
import { GitCompareArrows } from "lucide-react";

import type { components } from "../../lib/generated/schema";
import { formatPaperTime } from "../order-timeline";

type Reconciliation = components["schemas"]["BrokerReconciliationResponse"];

export function ReconciliationPanel({ reconciliation }: { reconciliation: Reconciliation | null }): React.ReactElement {
  return (
    <section className="broker-section broker-reconciliation" aria-labelledby="broker-reconciliation-title">
      <header className="broker-section__heading">
        <div><p className="eyebrow">权威对账</p><h2 id="broker-reconciliation-title">对账边界</h2></div>
        <GitCompareArrows aria-hidden="true" size={21} />
      </header>
      {!reconciliation ? <p className="broker-section__empty">尚无对账运行；外部账户事实不作推断。</p> : (
        <dl className="broker-reconciliation__facts">
          <div><dt>状态</dt><dd><span className="state-chip" data-state={reconciliation.status}>{statusLabel(reconciliation.status)}</span></dd></div>
          <div><dt>差异数</dt><dd className="numeric-data">{reconciliation.diff_count}</dd></div>
          <div><dt>开始</dt><dd>{formatPaperTime(reconciliation.started_at)}</dd></div>
          <div><dt>完成</dt><dd>{reconciliation.completed_at ? formatPaperTime(reconciliation.completed_at) : "未完成"}</dd></div>
        </dl>
      )}
      <p className="broker-section__guard">内部模拟盘结果与券商模拟账户回执分开保存；差异会锁定账户并停止后续动作。</p>
    </section>
  );
}
