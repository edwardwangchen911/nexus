import { Bot, DatabaseZap, Eye, LockKeyhole, Scale } from "lucide-react";

type AuthorityLedgerProps = {
  snapshotStatus: string;
  displayReadiness: string;
  decisionReadiness: string;
};

export function AuthorityLedger({ snapshotStatus, displayReadiness, decisionReadiness }: AuthorityLedgerProps): React.ReactElement {
  const displayReady = displayReadiness === "DISPLAY_READY";
  const decisionReady = decisionReadiness === "DECISION_READY";
  const decisionAttention = decisionReadiness === "RECONCILIATION_REQUIRED" || decisionReadiness === "DATA_STALE";
  const decisionLabel: Record<string, string> = { DECISION_READY: "决策可用", RECONCILIATION_PENDING: "决策待对账", DATA_STALE: "决策数据已过期", RECONCILIATION_REQUIRED: "决策对账失败" };

  return (
    <section className="authority-ledger" aria-label="数据与决策权威边界">
      <header>
        <div>
          <p className="eyebrow">NEXUS AUTHORITY CIRCUIT</p>
          <h2>证据如何抵达决策</h2>
        </div>
        <span className="authority-ledger__mode"><Bot aria-hidden="true" size={15} />Agent 只提案</span>
      </header>
      <ol>
        <LedgerStep icon={DatabaseZap} label="冻结事实" value={snapshotStatus} state={snapshotStatus === "READY" ? "verified" : "attention"} />
        <LedgerStep icon={Eye} label={displayReady ? "资产可展示" : "暂无资产事实"} value={displayReadiness} state={displayReady ? "verified" : "attention"} />
        <LedgerStep icon={Scale} label={decisionLabel[decisionReadiness] ?? "决策不可用"} value={decisionReadiness} state={decisionReady ? "verified" : decisionAttention ? "attention" : "current"} />
        <LedgerStep icon={LockKeyhole} label="执行锁定" value="HUMAN APPROVAL" state="locked" />
      </ol>
    </section>
  );
}

function LedgerStep({ icon: Icon, label, value, state }: {
  icon: typeof DatabaseZap;
  label: string;
  value: string;
  state: "verified" | "current" | "attention" | "locked";
}): React.ReactElement {
  return (
    <li data-state={state}>
      <span className="authority-ledger__icon"><Icon aria-hidden="true" size={17} strokeWidth={1.8} /></span>
      <span><small>{label}</small><strong>{value}</strong></span>
    </li>
  );
}
