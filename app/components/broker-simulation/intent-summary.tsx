import { CheckCircle2, FileKey2, XCircle } from "lucide-react";

import type { components } from "../../lib/generated/schema";
import { formatPaperDecimal, formatPaperTime } from "../order-timeline";

type Workbench = components["schemas"]["BrokerSimulationResponse"];

export function IntentSummary({
  workbench,
  mutationsAllowed,
  busy,
  onApprove,
  onReject,
}: {
  workbench: Workbench;
  mutationsAllowed: boolean;
  busy: boolean;
  onApprove: () => void;
  onReject: () => void;
}): React.ReactElement | null {
  const { intent, actions } = workbench;
  if (!intent) return null;
  return (
    <section className="broker-section broker-intent" aria-labelledby="broker-intent-title">
      <header className="broker-section__heading">
        <div>
          <p className="eyebrow">IMMUTABLE INTENT</p>
          <h2 id="broker-intent-title">服务端批次意图</h2>
        </div>
        <span className="state-chip" data-state={intent.status}>{intent.status}</span>
      </header>
      <div className="broker-intent__body">
        <div className="broker-intent__notional">
          <span>批次名义金额</span>
          <strong className="numeric-data">USD {formatPaperDecimal(intent.total_notional, 2)}</strong>
          <small>{intent.orders.length} 笔服务端生成的限价单</small>
        </div>
        <dl className="broker-intent__facts">
          <div><dt>版本</dt><dd className="numeric-data">v{intent.version}</dd></div>
          <div><dt>准备时间</dt><dd>{formatPaperTime(intent.prepared_at)}</dd></div>
          <div><dt>批准截止</dt><dd>{formatPaperTime(intent.expires_at)}</dd></div>
          <div><dt>证据封印</dt><dd>{intent.evidence_hashes.length} 项 · {shortHash(intent.body_hash)}</dd></div>
        </dl>
      </div>
      <p className="broker-intent__difference">
        <FileKey2 aria-hidden="true" size={17} />
        策略提案只表达目标。本批次的数量、限价、账户绑定与风控证据均由后端重建并封印。
      </p>
      {intent.status === "prepared" ? (
        <div className="broker-intent__actions" aria-label="离线批次决策">
          {actions.can_approve ? (
            <button className="button button--primary" type="button" disabled={!mutationsAllowed || busy} onClick={onApprove}>
              <CheckCircle2 aria-hidden="true" size={17} />审阅并批准离线批次
            </button>
          ) : null}
          {actions.can_reject ? (
            <button className="button button--danger" type="button" disabled={!mutationsAllowed || busy} onClick={onReject}>
              <XCircle aria-hidden="true" size={17} />拒绝离线批次
            </button>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

function shortHash(value: string): string {
  return `${value.slice(0, 8)}…${value.slice(-8)}`;
}

