import { statusLabel } from "../lib/display-labels";
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
  const decisionLabel: Record<string, string> = { DECISION_READY: "账户快照核验通过", RECONCILIATION_PENDING: "账户快照等待对账", DATA_STALE: "账户快照已过期", RECONCILIATION_REQUIRED: "账户快照需重新对账" };

  return (
    <section className="authority-ledger" aria-label="数据与决策权威边界">
      <header>
        <h2>数据与权限</h2>
        <span className="authority-ledger__mode"><Bot aria-hidden="true" size={15} />AI 仅辅助研究</span>
      </header>
      <ol>
        <LedgerStep icon={DatabaseZap} label="快照质量" value={snapshotStatus} state={snapshotStatus === "READY" ? "verified" : "attention"} />
        <LedgerStep icon={Eye} label={displayReady ? "资产可展示" : "暂无资产事实"} value={displayReadiness} state={displayReady ? "verified" : "attention"} />
        <LedgerStep icon={Scale} label="账户快照核验" value={decisionLabel[decisionReadiness] ?? "账户快照尚未核验"} state={decisionReady ? "verified" : decisionAttention ? "attention" : "current"} />
        <LedgerStep icon={LockKeyhole} label="执行状态" value="当前不可执行" state="locked" />
      </ol>
      <p>快照核验、研究结论与执行条件分别判断；当前实盘执行关闭。</p>
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
      <span><small>{label}</small><strong title={value}>{statusLabel(value)}</strong></span>
    </li>
  );
}
