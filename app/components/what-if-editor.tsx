"use client";

import { formatDisplayDecimal } from "../lib/display-decimal";
import { Calculator, TriangleAlert } from "lucide-react";
import { useMemo, useState } from "react";
import type { components } from "../lib/generated/schema";

type ProposalGroup = components["schemas"]["OptimizationProposalGroupResponse"];
type WhatIfRequest = components["schemas"]["WhatIfRequest"];
type WhatIfResponse = components["schemas"]["WhatIfResponse"];

type Props = {
  group: ProposalGroup;
  proposalId: string;
  actionsEnabled?: boolean;
  onEvaluate: (request: WhatIfRequest) => Promise<WhatIfResponse>;
};

export function WhatIfEditor({ group, proposalId, actionsEnabled = true, onEvaluate }: Props): React.ReactElement {
  const proposal = useMemo(
    () => group.proposals.find((candidate) => candidate.proposal_id === proposalId) ?? null,
    [group.proposals, proposalId],
  );
  if (!proposal) {
    return <section className="what-if-editor"><p role="alert">URL 中的方案不存在，无法开始情景试算。</p></section>;
  }
  if (proposal.positions.length > 30) {
    return <section className="what-if-editor"><p role="alert">方案仍可只读比较；情景试算最多支持 30 个标的。</p></section>;
  }

  return <WhatIfProposalEditor key={proposal.proposal_id} group={group} proposal={proposal} actionsEnabled={actionsEnabled} onEvaluate={onEvaluate} />;
}

function WhatIfProposalEditor({ group, proposal, actionsEnabled, onEvaluate }: { group: ProposalGroup; proposal: ProposalGroup["proposals"][number]; actionsEnabled: boolean; onEvaluate: Props["onEvaluate"] }): React.ReactElement {
  const [weights, setWeights] = useState<Record<string, string>>(() => Object.fromEntries(proposal.positions.map((position) => [position.instrument_id, position.target_weight])));
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<WhatIfResponse | null>(null);

  async function evaluate(): Promise<void> {
    const positions = proposal.positions.map((position) => ({
      instrument_id: position.instrument_id,
      weight: weights[position.instrument_id] ?? "",
    }));
    if (!actionsEnabled) return;
    if (positions.some((position) => !isDecimalWeight(position.weight))) {
      setError("请输入 0 到 1 之间的十进制权重（最多 18 位小数）；不支持指数或 NaN。");
      setResult(null);
      return;
    }
    setPending(true); setError(null); setResult(null);
    try {
      const response = await onEvaluate({
        proposal_group_id: group.proposal_group_id,
        proposal_id: proposal.proposal_id,
        policy_version: "optimizer_v1",
        positions,
      });
      setResult(response);
    } catch {
      setError("情景评估失败。输入尚未保存，请检查方案版本后重试。");
    } finally {
      setPending(false);
    }
  }

  return (
    <section className="what-if-editor" aria-labelledby="what-if-title">
      <header>
        <div>
          <p className="eyebrow">无副作用评估</p>
          <h2 id="what-if-title">有限权重情景</h2>
          <p>允许范围冻结自所选方案；仅分析，不持久化，不生成订单。</p>
        </div>
        <span className="mode-badge">{proposal.variant}</span>
      </header>
      <div className="what-if-editor__fields">
        {proposal.positions.map((position, index) => {
          const inputId = `what-if-weight-${position.instrument_id}`;
          return (
            <div className="what-if-field" key={position.instrument_id}>
              <label htmlFor={inputId}>目标权重 {index + 1}</label>
              <code>{position.instrument_id}</code>
              <input
                id={inputId}
                type="text"
                inputMode="decimal"
                maxLength={20}
                disabled={!actionsEnabled}
                value={weights[position.instrument_id] ?? ""}
                aria-describedby={`${inputId}-help`}
                onChange={(event) => {
                  setWeights((current) => ({ ...current, [position.instrument_id]: event.target.value }));
                  setError(null); setResult(null);
                }}
              />
              <small id={`${inputId}-help`}>当前 {position.current_weight} · 方案 {position.target_weight} · 最多 18 位小数</small>
            </div>
          );
        })}
      </div>
      {!actionsEnabled ? <p className="inline-notice" role="status">当前数据状态不允许试算；已缓存权重仅供查看，请先重新读取方案。</p> : null}
      {error ? <p className="inline-notice inline-notice--error" role="alert"><TriangleAlert aria-hidden="true" size={17} />{error}</p> : null}
      {result ? <WhatIfResult result={result} /> : null}
      <button className="button button--primary" type="button" disabled={pending || !actionsEnabled} onClick={() => void evaluate()}>
        <Calculator aria-hidden="true" size={17} />{pending ? "正在评估" : "评估情景"}
      </button>
    </section>
  );
}

function WhatIfResult({ result }: { result: WhatIfResponse }): React.ReactElement {
  const violations = result.result.constraints.violation_count;
  const rows = [
    ...Object.entries(result.result.diagnostics).map(([name, metric]) => ({ group: "诊断", name, metric })),
    ...Object.entries(result.result.constraints).map(([name, metric]) => ({ group: "约束", name, metric })),
  ];
  return (
    <section className="what-if-result" aria-labelledby="what-if-result-title">
      <h3 id="what-if-result-title">后端确定性结果</h3>
      <dl>
        <div><dt>标的数量</dt><dd className="numeric-data">{result.instrument_count}</dd></div>
        <div><dt>约束命中</dt><dd className="numeric-data">{violations.status === "known" ? violations.value : violations.reason}</dd></div>
        <div><dt>策略</dt><dd>{result.policy_version}</dd></div>
      </dl>
      {/* The bounded scroll region must be keyboard-focusable on narrow screens. */}
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex */}
      <div className="table-scroll" role="region" aria-label="情景诊断与约束结果，可横向滚动" tabIndex={0}>
        <table className="what-if-result__table">
          <caption>后端返回的全部指标；未知项保留原因</caption>
          <thead><tr><th scope="col">分组</th><th scope="col">指标</th><th scope="col">结果</th></tr></thead>
          <tbody>{rows.map(({ group: metricGroup, name, metric }) => <tr key={`${metricGroup}:${name}`}><td>{metricGroup}</td><th scope="row"><code>{name}</code></th><td className="numeric-data">{metric.status === "known" ? `${formatDisplayDecimal(metric.value)} ${metric.unit}` : `未知 · ${metric.reason}`}</td></tr>)}</tbody>
        </table>
      </div>
      <p>结果仅存在于当前页面；离开页面即丢弃。</p>
    </section>
  );
}

function isDecimalWeight(value: string): boolean {
  return /^(?:0(?:\.\d{1,18})?|1(?:\.0{1,18})?)$/.test(value);
}
