"use client";

import { AlertTriangle, Check, FlaskConical, RefreshCw, X } from "lucide-react";
import { useState } from "react";
import type { components } from "../lib/generated/schema";

type ProposalGroup = components["schemas"]["OptimizationProposalGroupResponse"];
type Proposal = ProposalGroup["proposals"][number];
type Variant = Proposal["variant"];

type Props = {
  group: ProposalGroup;
  actionsEnabled?: boolean;
  onStartShadow: (groupId: string, version: number) => Promise<void>;
  onReject: (groupId: string, version: number) => Promise<void>;
};

const variants: Variant[] = ["hold_current", "minimum_change", "strategy_target"];
const variantLabels: Record<Variant, string> = {
  hold_current: "保持现状",
  minimum_change: "最小调整",
  strategy_target: "策略目标",
};
const metricRows = [
  ["hard_violations", "硬违例", "count"],
  ["top1", "最大单项权重", "ratio"],
  ["top3", "前三项权重", "ratio"],
  ["hhi", "证券 HHI", "ratio"],
  ["sector_exposure", "最大行业暴露", "ratio"],
  ["currency_exposure", "最大币种暴露", "ratio"],
  ["volatility", "历史波动", "ratio"],
  ["drawdown", "回撤情景", "ratio"],
  ["correlation", "相关性", "ratio"],
  ["liquidity", "流动性因子", "factor"],
  ["turnover", "单边换手", "ratio"],
  ["cost", "成本", "ratio"],
  ["tax_impact", "税务影响", "state"],
] as const;

export function ProposalComparison({ group, actionsEnabled = true, onStartShadow, onReject }: Props): React.ReactElement {
  const [confirming, setConfirming] = useState(false);
  const [pending, setPending] = useState<"shadow" | "reject" | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const ordered = variants.map((variant) => group.proposals.find((proposal) => proposal.variant === variant));
  const complete = ordered.every((proposal): proposal is Proposal => proposal !== undefined);
  const shadowReason = group.shadow_eligibility.eligible
    ? null
    : group.shadow_eligibility.reasons.join("、") || "服务器未授予影子验证资格。";
  const terminal = ["expired", "rejected", "validated"].includes(group.status);
  const canShadow = actionsEnabled && group.status === "compared" && shadowReason === null;
  const canReject = actionsEnabled && ["draft", "compared"].includes(group.status);
  const blockedReason = !actionsEnabled
    ? "当前数据状态不允许发起操作；请先重新读取方案。"
    : group.status !== "compared"
      ? `当前状态 ${group.status} 不允许新的生命周期动作。`
      : shadowReason;

  async function startShadow(): Promise<void> {
    if (!canShadow) return;
    setPending("shadow"); setNotice(null);
    try {
      await onStartShadow(group.proposal_group_id, group.version);
      setConfirming(false);
      setNotice("影子验证已启动；不会创建券商订单。");
    } catch (error) {
      setConfirming(false);
      setNotice(String(error).includes("VERSION_CONFLICT")
        ? "方案版本已变化。保留当前比较，请重新加载方案。"
        : "影子验证未启动。请检查资格原因后重试。");
    } finally {
      setPending(null);
    }
  }

  async function reject(): Promise<void> {
    if (!canReject) return;
    setPending("reject"); setNotice(null);
    try {
      await onReject(group.proposal_group_id, group.version);
      setNotice("方案组已拒绝；如需恢复，请创建新的方案组。");
    } catch (error) {
      setNotice(String(error).includes("VERSION_CONFLICT")
        ? "方案版本已变化。保留当前比较，请重新加载方案。"
        : "方案组未拒绝，请重试。");
    } finally {
      setPending(null);
    }
  }

  if (!complete) {
    return <section className="proposal-comparison"><p role="alert">方案组不完整，无法比较。</p></section>;
  }

  return (
    <section className="proposal-comparison" aria-labelledby="proposal-comparison-title">
      <header className="proposal-comparison__heading">
        <div>
          <p className="eyebrow">SAME BASELINE / THREE VARIANTS</p>
          <h2 id="proposal-comparison-title">三方案同口径比较</h2>
          <p>所有值来自冻结的后端比较，不在浏览器重算或排序。</p>
        </div>
        <span className="state-chip" data-state={terminal ? "stale" : "ready"}>{group.status}</span>
      </header>

      {/* The bounded scroll region must be keyboard-focusable on narrow screens. */}
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex */}
      <div className="table-scroll proposal-comparison__scroll" role="region" aria-label="三方案指标表，可横向滚动" tabIndex={0}>
        <table className="proposal-table">
          <caption>保持现状、最小调整与策略目标的确定性比较</caption>
          <thead>
            <tr>
              <th scope="col">指标</th>
              {ordered.map((proposal) => (
                <th scope="col" key={proposal.variant}>{variantLabels[proposal.variant]}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            <tr>
              <th scope="row">方案状态</th>
              {ordered.map((proposal) => <td key={proposal.variant}><ProposalBadges proposal={proposal} /></td>)}
            </tr>
            {metricRows.map(([metric, label, unit]) => (
              <tr key={metric}>
                <th scope="row">{label}</th>
                {ordered.map((proposal) => (
                  <td key={proposal.variant} className="numeric-data">
                    <MetricValue proposal={proposal} metric={metric} unit={unit} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {notice ? (
        <div className="proposal-comparison__notice" role="status" aria-live="polite">
          <span>{notice}</span>
          {notice.includes("版本已变化") ? (
            <button className="button button--quiet" type="button" onClick={() => globalThis.location.reload()}>
              <RefreshCw aria-hidden="true" size={17} />重新加载方案
            </button>
          ) : null}
        </div>
      ) : null}

      <div className="proposal-comparison__actions">
        {canShadow ? (
          <button className="button button--primary" type="button" disabled={pending !== null} onClick={() => setConfirming(true)}>
            <FlaskConical aria-hidden="true" size={17} />启动影子验证
          </button>
        ) : (
          <p className="proposal-comparison__blocked"><AlertTriangle aria-hidden="true" size={17} />{blockedReason}</p>
        )}
        {canReject ? (
          <div className="proposal-comparison__reject">
            <p>拒绝后只能通过创建新方案组恢复。</p>
            <button className="button button--danger" type="button" disabled={pending !== null} onClick={() => void reject()}>
              <X aria-hidden="true" size={17} />{pending === "reject" ? "正在拒绝" : "拒绝方案组"}
            </button>
          </div>
        ) : null}
      </div>

      {confirming && canShadow ? (
        <div className="confirmation-dialog" role="dialog" aria-modal="true" aria-labelledby="shadow-confirm-title">
          <div>
            <h3 id="shadow-confirm-title">确认启动影子验证</h3>
            <p>三个方案将从同一起点开始前向观察；不会创建券商订单。</p>
            <div className="confirmation-dialog__actions">
              <button className="button button--quiet" type="button" onClick={() => setConfirming(false)}>取消</button>
              <button className="button button--primary" type="button" disabled={pending === "shadow"} onClick={() => void startShadow()}>
                <Check aria-hidden="true" size={17} />{pending === "shadow" ? "正在启动" : "确认启动影子验证"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}

function ProposalBadges({ proposal }: { proposal: Proposal }): React.ReactElement {
  return (
    <span className="proposal-badges">
      {proposal.analysis_only ? <span className="mode-badge">仅分析</span> : null}
      <span className="mode-badge">{proposal.comparison.non_executable ? "不可执行" : "可进入影子验证"}</span>
      {!proposal.strategy_evidence_complete && proposal.variant === "strategy_target"
        ? <span className="mode-badge">证据未完成</span>
        : null}
    </span>
  );
}

function MetricValue({ proposal, metric, unit }: { proposal: Proposal; metric: typeof metricRows[number][0]; unit: string }): React.ReactElement {
  const dimensions = proposal.comparison.metrics.dimensions;
  if (metric === "hard_violations") return <>{dimensions.hard_violations} 项</>;
  const value = dimensions[metric];
  if (value.status === "unknown") return <span className="metric-unknown">未知 · {value.reason}</span>;
  return <>{value.value} {unit === "ratio" ? "ratio" : unit}</>;
}
