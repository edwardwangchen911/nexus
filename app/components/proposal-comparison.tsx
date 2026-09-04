"use client";

import { formatMetric, formatPercent } from "../lib/display-format";
import { statusLabel, reasonText, reasonLabel } from "../lib/display-labels";
import { AlertTriangle, Check, FlaskConical, RefreshCw, X } from "lucide-react";
import { useState } from "react";
import type { components } from "../lib/generated/schema";
import { ProposalResearchContext } from "./proposal-research-context";

type ProposalGroup = components["schemas"]["OptimizationProposalGroupResponse"];
type Proposal = ProposalGroup["proposals"][number];
type Variant = Proposal["variant"];

type Props = {
  group: ProposalGroup;
  startShadowEnabled?: boolean;
  rejectEnabled?: boolean;
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
  ["hard_violations", "硬性约束不达标数", "count"],
  ["top1", "最大单项权重", "ratio"],
  ["top3", "前三项权重", "ratio"],
  ["hhi", "持仓集中度指数（HHI）", "ratio"],
  ["sector_exposure", "最大行业暴露", "ratio"],
  ["currency_exposure", "最大币种暴露", "ratio"],
  ["volatility", "历史波动", "ratio"],
  ["drawdown", "回撤情景", "ratio"],
  ["correlation", "相关系数", "ratio"],
  ["liquidity", "流动性系数（越高越易退出）", "coefficient"],
  ["turnover", "单边换手", "ratio"],
  ["cost", "成本占比", "ratio"],
  ["tax_impact", "税务影响", "state"],
] as const;
const rankedDimensionNames = [
  "top1", "top3", "hhi", "sector_exposure", "currency_exposure", "volatility",
  "drawdown", "correlation", "liquidity", "turnover", "cost",
] as const;

export function ProposalComparison({ group, startShadowEnabled = true, rejectEnabled = true, onStartShadow, onReject }: Props): React.ReactElement {
  const [confirming, setConfirming] = useState(false);
  const [pending, setPending] = useState<"shadow" | "reject" | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const ordered = variants.map((variant) => group.proposals.find((proposal) => proposal.variant === variant));
  const complete = ordered.every((proposal): proposal is Proposal => proposal !== undefined);
  const shadowReason = group.shadow_eligibility.eligible
    ? null
    : group.shadow_eligibility.reasons.map(reasonText).join("；") || "服务器未授予影子验证资格。";
  const terminal = ["expired", "rejected", "validated"].includes(group.status);
  const comparisonUsable = complete && ordered.every(proposalIsComparable);
  const canShadow = startShadowEnabled && comparisonUsable && group.status === "compared" && shadowReason === null;
  const canReject = rejectEnabled && ["draft", "compared", "shadowing"].includes(group.status);
  const blockedReason = !startShadowEnabled
    ? "当前数据状态不允许发起操作；请先重新读取方案。"
    : group.status !== "compared"
      ? `当前状态 ${statusLabel(group.status)} 不允许再次启动影子验证。`
      : !comparisonUsable
        ? "至少一个方案不具备排名或执行适用性；只能核对已保存内容。"
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
          <p className="eyebrow">同一基线 · 三种方案</p>
          <h2 id="proposal-comparison-title">三方案同口径比较</h2>
          <p>占比和收益率显示为百分比，集中度指数和相关系数保留原量纲。最大单项和前三项权重以含现金总资产为分母；HHI 仅按证券持仓归一化计算。数值来自已保存的方案比较。</p>
        </div>
        <span className="state-chip" data-state={terminal ? "stale" : "ready"}>{statusLabel(group.status)}</span>
      </header>

      <EconomicComparisonSummary comparison={group.economic_comparison} usable={comparisonUsable} />
      <ProposalResearchContext context={group.research_context} />

      <section className="proposal-allocations" aria-labelledby="proposal-allocations-title">
        <header>
          <p className="eyebrow">已保存仓位目标</p>
          <h3 id="proposal-allocations-title">各方案调整内容与适用边界</h3>
          <p>权重和变动均来自已保存方案；标的名称未知时不使用当前代码回填。非可执行或证据不完整的内容仅供核对。</p>
        </header>
        <div className="proposal-allocations__grid">
          {ordered.map((proposal) => (
            <ProposalAllocation
              key={proposal.proposal_id}
              proposal={proposal}
              hold={ordered[0]}
            />
          ))}
        </div>
      </section>

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
              {ordered.map((proposal) => <td key={proposal.variant}><ProposalBadges proposal={proposal} shadowEligible={canShadow} /></td>)}
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

const economicReasons = new Map([
  ["COST_EVIDENCE_INCOMPLETE", "费用证据不完整，需补齐费用依据后再比较。"],
  ["COMPARISON_EVIDENCE_INCOMPLETE", "方案比较证据不完整，需补齐相关指标与依据后再比较。"],
  ["ECONOMIC_COMPARISON_POLICY_MISSING", "尚未明确风险改善与新增成本如何权衡，需先确定比较判据。"],
]);

function EconomicComparisonSummary({ comparison, usable }: { comparison: ProposalGroup["economic_comparison"]; usable: boolean }): React.ReactElement {
  const compared = usable && comparison?.status === "compared" && comparison.recommendation !== null;
  const label = !comparison
    ? "经济比较结论未报告"
    : compared
      ? `比较结果：${variantLabels[comparison.recommendation!]}`
      : comparison.status === "policy_required" ? "待明确比较判据" : "暂不可比";

  return (
    <section aria-label="经济比较结论">
      <h3>{label}</h3>
      <p>{!comparison
        ? "当前响应没有报告经济比较结论，不据此判断哪种方案更优。"
        : compared
          ? "这是基于已保存证据计算的比较结果，不代表已作出人工选择，也不授予执行权限。"
          : !usable
            ? "尚未形成经济比较结论；至少一个方案不具备排名或执行适用性，不能展示推荐，也不能作为后续操作依据。"
            : "尚未形成经济比较结论，不能据此认定保持现状或调整更优。"}</p>
      {comparison?.reasons.length ? (
        <>
          <p>{[...new Set(comparison.reasons.map((reason) => economicReasons.get(reason)
            ?? "存在未分类的比较限制，请查看原因代码"))].join(" ")}</p>
          <details>
            <summary>查看原因代码</summary>
            {comparison.reasons.map((reason, index) => <p key={`${index}:${reason}`}><code>{reason}</code></p>)}
          </details>
        </>
      ) : null}
    </section>
  );
}

const actionLabels = {
  none: "无动作", reduce: "减持", maintain: "维持", increase: "增持",
} satisfies Record<Proposal["positions"][number]["action"], string>;

function ProposalAllocation({ proposal, hold }: { proposal: Proposal; hold: Proposal }): React.ReactElement {
  const metrics = proposal.comparison.metrics;
  const comparable = proposalIsComparable(proposal);
  const strategyPending = proposal.variant === "strategy_target"
    && !proposal.strategy_evidence_complete
    && sameTargets(proposal, hold);
  const unknownReasons = [...new Set(Object.values(metrics.dimensions)
    .filter((item): item is { status: "unknown"; value: null; reason: string | null } =>
      typeof item === "object" && item !== null && "status" in item && item.status === "unknown")
    .map((item) => item.reason).filter((reason): reason is string => Boolean(reason)))];
  return (
    <article className="proposal-allocation" role="region" aria-label={`${variantLabels[proposal.variant]}仓位目标与完整性`}>
      <header>
        <h4>{variantLabels[proposal.variant]}</h4>
        <span className="mode-badge">{strategyPending
          ? "策略目标尚未形成"
          : proposal.comparison.non_executable
            ? "非可执行"
            : comparable ? "可比较" : "暂不可比较"}</span>
      </header>
      {strategyPending ? <p>策略研究证据未完成，保存内容与保持现状相同；不能视为第三个已形成候选。</p> : null}
      {/* The bounded table is intentionally keyboard-focusable on narrow screens. */}
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex */}
      <div className="table-scroll proposal-allocation__scroll" role="region" aria-label={`${variantLabels[proposal.variant]}仓位表，可横向滚动`} tabIndex={0}>
        <table>
          <thead><tr><th scope="col">标的</th><th scope="col">当前</th><th scope="col">目标</th><th scope="col">变动</th><th scope="col">动作与依据</th></tr></thead>
          <tbody>{proposal.positions.map((position) => (
            <tr key={position.instrument_id}>
              <th scope="row">{position.display_symbol ?? "标的名称未知"}</th>
              <td className="numeric-data">{formatPercent(position.current_weight)}</td>
              <td className="numeric-data">{formatPercent(position.target_weight)}</td>
              <td className="numeric-data">{formatPercent(decimalDifference(position.target_weight, position.current_weight), true)}</td>
              <td>{actionLabels[position.action]}<br /><code>{position.reason_code}</code></td>
            </tr>
          ))}</tbody>
        </table>
      </div>
      <dl className="proposal-allocation__checks">
        <CheckState label="约束核验" ok={metrics.constraint_complete} />
        <CheckState label="适用性核验" ok={metrics.eligibility_complete} />
        <CheckState label="比较证据" ok={metrics.evidence_complete} />
        <CheckState label="成本依据" ok={proposal.comparison.cost_complete} />
        <CheckState label="排名资格" ok={metrics.ranking_eligible} />
        <div><dt>数据时效</dt><dd>{metrics.stale ? "已过期" : "未标记过期"}</dd></div>
        <div><dt>外汇换算</dt><dd>{proposal.comparison.requires_fx ? "需要换算" : "不需要换算"}</dd></div>
        <div><dt>执行边界</dt><dd>{proposal.comparison.non_executable ? "非可执行" : "仍需独立许可"}</dd></div>
      </dl>
      {metrics.unknown_metrics.length ? <p className="proposal-allocation__unknown">未完整指标：{metrics.unknown_metrics.map((metric) => <code key={metric}>{metric}</code>)}</p> : null}
      {unknownReasons.length ? <div className="proposal-allocation__unknown"><p>未知指标原因</p>{unknownReasons.map((reason) => <p key={reason}>{reasonLabel(reason)} · <code>{reason}</code></p>)}</div> : null}
    </article>
  );
}

function CheckState({ label, ok }: { label: string; ok: boolean }): React.ReactElement {
  return <div><dt>{label}</dt><dd>{ok ? "完整" : `${label}不完整`}</dd></div>;
}

function proposalIsComparable(proposal: Proposal): boolean {
  const metrics = proposal.comparison.metrics;
  return (
    (proposal.variant !== "strategy_target" || proposal.strategy_evidence_complete)
    && metrics.constraint_complete
    && metrics.eligibility_complete
    && metrics.evidence_complete
    && proposal.comparison.cost_complete
    && !metrics.stale
    && !proposal.comparison.requires_fx
    && metrics.unknown_metrics.length === 0
    && rankedDimensionNames.every((name) => metrics.dimensions[name].status === "known")
    && metrics.ranking_eligible
    && !proposal.comparison.non_executable
  );
}

function sameTargets(left: Proposal, right: Proposal): boolean {
  const values = (proposal: Proposal) => [...proposal.positions].sort((a, b) =>
    a.instrument_id < b.instrument_id ? -1 : a.instrument_id > b.instrument_id ? 1 : 0);
  const leftValues = values(left); const rightValues = values(right);
  return leftValues.length === rightValues.length && leftValues.every((item, index) => (
    item.instrument_id === rightValues[index]?.instrument_id
    && decimalDifference(item.target_weight, rightValues[index].target_weight) === "0"
  ));
}

function decimalDifference(left: string, right: string): string {
  const parts = (value: string) => {
    const [whole, fraction = ""] = value.split(".");
    return { whole, fraction };
  };
  const a = parts(left); const b = parts(right);
  const scale = Math.max(a.fraction.length, b.fraction.length);
  const units = (value: ReturnType<typeof parts>) => BigInt(`${value.whole}${value.fraction.padEnd(scale, "0")}`);
  const difference = units(a) - units(b);
  const negative = difference < BigInt(0); const digits = (negative ? -difference : difference).toString().padStart(scale + 1, "0");
  const whole = scale ? digits.slice(0, -scale) : digits;
  const fraction = scale ? digits.slice(-scale).replace(/0+$/, "") : "";
  return `${negative ? "-" : ""}${whole}${fraction ? `.${fraction}` : ""}`;
}

function ProposalBadges({ proposal, shadowEligible }: { proposal: Proposal; shadowEligible: boolean }): React.ReactElement {
  return (
    <span className="proposal-badges">
      {proposal.analysis_only ? <span className="mode-badge">仅分析</span> : null}
      <span className="mode-badge">{proposal.comparison.non_executable ? "不可执行" : shadowEligible ? "可进入影子验证" : "仅供比较"}</span>
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
  if (value.status === "unknown") return <span className="metric-unknown" title={value.reason ?? undefined}>未知 · {value.reason ? reasonLabel(value.reason) : "原因未提供"}</span>;
  return <>{formatMetric(value.value, unit, metric)}</>;
}
