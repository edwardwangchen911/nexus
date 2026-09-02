"use client";

import { formatDisplayDecimal } from "../lib/display-decimal";
import { FileSearch, ShieldAlert } from "lucide-react";
import { useRef, useState } from "react";

import type { components } from "../lib/generated/schema";
import { EvidenceDrawer } from "./evidence-drawer";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";

type Research = components["schemas"]["ResearchRunResponse"];
type Evidence = components["schemas"]["EvidenceRef"];

const stanceLabels = {
  buy_candidate: "买入候选",
  reduce_candidate: "减仓候选",
  hold: "保持观察",
  watch: "仅观察",
} as const;

const voteLabels = { support: "支持", oppose: "反对", abstain: "弃权" } as const;

export function DecisionCard({ research }: { research: Research }): React.ReactElement {
  const [selected, setSelected] = useState<{ evidence: Evidence; excerpt: string | null } | null>(null);
  const openerRef = useRef<HTMLButtonElement>(null);
  const intent = research.intent;
  const isHongKongGap = intent?.display_symbol === "00656.HK";
  const terminalReason = research.budget_reason ?? research.abstention_reason;

  function openEvidence(evidence: Evidence, excerpt: string | null, opener: HTMLButtonElement): void {
    openerRef.current = opener;
    setSelected({ evidence, excerpt });
  }

  return (
    <article className="decision-card" aria-labelledby="decision-card-title">
      <header className="decision-card__heading">
        <div>
          <p className="eyebrow">委员会决策 · 证据卷宗</p>
          <h2 id="decision-card-title">委员会决策记录</h2>
        </div>
        <div className="decision-card__badges">
          <Badge data-state={intent?.quality_status === "complete" ? "ready" : "partial"}>{intent?.quality_status ?? research.status}</Badge>
          <Badge variant="secondary">{intent ? stanceLabels[intent.stance] : "未形成研究意图"}</Badge>
        </div>
      </header>

      <dl className="decision-card__authority">
        <div><dt>标的</dt><dd>{intent?.display_symbol ?? research.instrument_id}</dd></div>
        <div><dt>截止时间</dt><dd className="numeric-data">{formatUtc(research.decision_time)}</dd></div>
        <div><dt>运行状态</dt><dd>{research.status}</dd></div>
        <div><dt>证据完整度</dt><dd>{research.quality_status ?? "未形成"}</dd></div>
      </dl>

      {research.billing_mode === "chatgpt_subscription" ? (
        <p className="research-page__gate" role="note">
          模型来源：Codex SDK · 使用 ChatGPT 套餐额度，不按 API 美元费用结算。
          输入 / 输出 tokens：{research.input_tokens ?? "未知"} / {research.output_tokens ?? "未知"}。
          {research.abstention_reason === "model_usage_uncertain"
            ? " 部分模型调用未返回用量；上述仅为已知下限，不会自动重试。"
            : null}
          仅生成研究，交易保持锁定。
        </p>
      ) : null}

      {isHongKongGap ? (
        <div className="decision-card__warning" role="note">
          <ShieldAlert aria-hidden="true" size={19} />
          <p>香港交易所财报证据尚未准入；00656.HK 仅展示价格与组合暴露，不能形成基本面买入或减仓立场。</p>
        </div>
      ) : null}

      {terminalReason ? (
        <div className="decision-card__warning" role="note">
          <ShieldAlert aria-hidden="true" size={19} />
          <div><p>研究未完成：<code>{terminalReason}</code>。保留已验证证据供排查，此状态不能支持策略目标。</p>
          {research.failed_stage_input ? <details><summary>失败调用的输入记录</summary><p>阶段：{({ research: "研究", critique: "反证", synthesis: "综合" } as const)[research.failed_stage_input.stage]}</p><p>请求摘要：<code>{research.failed_stage_input.request_hash}</code></p><p>同源输入：<code>{research.failed_stage_input.frozen_inputs_hash}</code></p></details> : null}
          </div>
        </div>
      ) : null}

      {research.stage_trace ? (
        <section className="decision-card__section" aria-label="三阶段研究过程">
          <h3>研究 → 反证 → 综合</h3>
          <p>实验流程 · 仅展示已保存的阶段。各阶段使用同一冻结证据，阶段用量仅包含已保存输出，失败调用以总用量说明为准。</p>
          {research.stage_trace.status === "invalid" ? <p role="note">阶段记录校验失败，暂不展示论点。</p> : research.stage_trace.stages.map((stage) => (
            <details key={stage.stage} open={stage.stage === "synthesis"}>
              <summary>{({ research: "研究", critique: "反证", synthesis: "综合" } as const)[stage.stage]} · {({ complete: "已保存完整输出", partial: "仅保存部分输出", unavailable: "未保存有效输出" } as const)[stage.status]}</summary>
              <p>已保存输入 / 输出 tokens：{stage.input_tokens} / {stage.output_tokens}</p>
              {stage.input_hash ? <details><summary>本阶段输入摘要</summary><p>请求与输出合同：<code>{stage.input_hash}</code></p><p>同源输入：<code>{stage.frozen_inputs_hash}</code></p></details> : <p>未留存本阶段提交输入摘要。</p>}
              {stage.roles.map((role) => <section key={role.role} aria-label={`${roleLabel(role.role)}阶段论点`}>
                <h4>{roleLabel(role.role)}</h4>
                {role.reviewed_claim_ids.length ? <p>回应论点：{role.reviewed_claim_ids.map((id) => {
                  const [reviewedRole, index] = id.split(":");
                  return `${roleLabel(reviewedRole as components["schemas"]["RoleVote"]["role"])} #${Number(index) + 1}`;
                }).join("、")}</p> : null}
                <ol>{role.claims.map((claim, index) => <li key={index}><p>{claim.text}</p><EvidenceButtons evidenceRefs={claim.evidence_refs} excerpt={claim.text} onOpen={openEvidence} /></li>)}</ol>
              </section>)}
            </details>
          ))}
          <details><summary>核对冻结输入版本</summary><p>证据摘要：<code>{research.stage_trace.evidence_hash}</code></p><p>数据快照：<code>{research.data_snapshot_id}</code></p></details>
        </section>
      ) : null}

      {intent ? (
        <>
          <div className="decision-card__arguments">
            <ClaimList title="支持论点" claims={intent.theses} onOpen={openEvidence} />
            <ClaimList title="反方论点" claims={intent.counter_theses} onOpen={openEvidence} />
          </div>
          <section className="decision-card__section" aria-labelledby="committee-votes-title">
            <h3 id="committee-votes-title">角色投票与分歧</h3>
            <ul className="decision-card__votes">
              {research.role_votes.map((vote) => <li key={vote.role}><span>{roleLabel(vote.role)}</span><strong data-vote={vote.vote}>{voteLabels[vote.vote]}</strong></li>)}
            </ul>
            <p>{research.disagreement}</p>
          </section>
          <section className="decision-card__section" aria-labelledby="invalidation-title">
            <h3 id="invalidation-title">失效条件</h3>
            {research.invalidation_conditions.length ? (
              <ul className="decision-card__conditions">
                {research.invalidation_conditions.map((condition, index) => (
                  <li key={`${condition.signal}:${index}`}>
                    <code>{condition.signal}</code>
                    <span>{condition.operator} {typeof condition.threshold === "boolean" ? String(condition.threshold) : formatDisplayDecimal(condition.threshold)} · {condition.unit}</span>
                    <EvidenceButtons evidenceRefs={condition.evidence_refs} excerpt={`${condition.signal} ${condition.operator} ${String(condition.threshold)}`} onOpen={openEvidence} />
                  </li>
                ))}
              </ul>
            ) : <p>未提供可机器判断的失效条件；当前研究不能支持策略目标。</p>}
          </section>
          <dl className="decision-card__versions">
            <div><dt>策略版本</dt><dd>{intent.strategy_version}</dd></div>
            <div><dt>模型版本</dt><dd>{intent.model_version}</dd></div>
            <div><dt>提示版本</dt><dd>{intent.prompt_version}</dd></div>
            <div><dt>数据版本</dt><dd>{intent.data_version}</dd></div>
          </dl>
        </>
      ) : (
        <p className="decision-card__empty">委员会没有输出可消费的研究意图；系统保持观望。</p>
      )}

      <EvidenceDrawer
        evidence={selected?.evidence ?? null}
        excerpt={selected?.excerpt ?? null}
        open={selected !== null}
        onClose={() => setSelected(null)}
        returnFocusRef={openerRef}
      />
    </article>
  );
}

function ClaimList({ title, claims, onOpen }: {
  title: string;
  claims: components["schemas"]["ResearchClaim"][];
  onOpen: (evidence: Evidence, excerpt: string | null, opener: HTMLButtonElement) => void;
}): React.ReactElement {
  return (
    <section aria-label={title}>
      <h3>{title}</h3>
      {claims.length ? <ol>{claims.map((claim, index) => <li key={`${title}:${index}`}><p>{claim.text}</p><EvidenceButtons evidenceRefs={claim.evidence_refs} excerpt={claim.text} onOpen={onOpen} /></li>)}</ol> : <p>未形成带证据的论点。</p>}
    </section>
  );
}

function EvidenceButtons({ evidenceRefs, excerpt, onOpen }: {
  evidenceRefs: Evidence[];
  excerpt: string | null;
  onOpen: (evidence: Evidence, excerpt: string | null, opener: HTMLButtonElement) => void;
}): React.ReactElement {
  return <div className="evidence-links">{evidenceRefs.map((evidence, index) => <Button key={`${evidence.source}:${evidence.object_id}:${evidence.content_hash}`} variant="ghost" size="sm" aria-haspopup="dialog" onClick={(event) => onOpen(evidence, excerpt, event.currentTarget)}><FileSearch aria-hidden="true" size={16} />{index === 0 ? "查看证据定位" : `查看证据定位 ${index + 1}`}</Button>)}</div>;
}

function roleLabel(role: components["schemas"]["RoleVote"]["role"]): string {
  return ({ orchestrator: "编排", fundamental: "基本面", market_state: "市场状态", macro_sector: "宏观行业", bull: "多头", bear: "空头", evidence_audit: "证据审计", portfolio: "组合" } as const)[role];
}

function formatUtc(value: string): string {
  return `${value.replace("T", " ").replace(/(?:\.\d+)?Z$/, "")} UTC`;
}
