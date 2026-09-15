import { Activity, LockKeyhole } from "lucide-react";

import { formatDisplayTime } from "../../lib/display-format";
import { loopPresentation } from "../../lib/product-status";
import type { components } from "../../lib/generated/schema";

type Workbench = components["schemas"]["WorkbenchResponse"];

export function ModeStrip({ workbench }: { workbench: Workbench }): React.ReactElement {
  const loop = loopPresentation(workbench.loop, workbench.session);
  return (
    <section className="workbench-mode" aria-label="运行模式">
      <div className="workbench-mode__identity">
        <span className="workbench-mode__badge"><Activity aria-hidden="true" size={16} />内部模拟</span>
        <span className="workbench-mode__lock"><LockKeyhole aria-hidden="true" size={16} />实盘锁定</span>
      </div>
      <div className="workbench-mode__loop" data-tone={loop.tone}>
        <span className="workbench-status-dot" aria-hidden="true" />
        <span><strong>{loop.label}</strong><small>{loop.detail}</small></span>
      </div>
      <dl>
        <div><dt>最近心跳</dt><dd className="numeric-data">{formatDisplayTime(workbench.loop.last_seen_at)}</dd></div>
        <div><dt>决策会话</dt><dd className="numeric-data">{workbench.session.decision_session ?? "尚未开始"}</dd></div>
      </dl>
    </section>
  );
}
