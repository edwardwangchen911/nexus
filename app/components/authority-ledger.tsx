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
        <LedgerStep icon={DatabaseZap} label="Provider fact" value={snapshotStatus} state={snapshotStatus === "READY" ? "verified" : "attention"} />
        <LedgerStep icon={Eye} label="Display gate" value={displayReadiness} state={displayReady ? "verified" : "attention"} />
        <LedgerStep icon={Scale} label="Decision gate" value={decisionReadiness} state={decisionReady ? "verified" : decisionAttention ? "attention" : "current"} />
        <LedgerStep icon={LockKeyhole} label="Execution" value="HUMAN APPROVAL" state="locked" />
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
