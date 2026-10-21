"use client";

import { formatDisplayTime } from "../lib/display-format";

import { formatMetric } from "../lib/display-format";
import { statusLabel, reasonText, operatorLabel, signalLabel } from "../lib/display-labels";
import { FileSearch, ShieldAlert } from "lucide-react";
import { useRef, useState } from "react";

import type { components } from "../lib/generated/schema";
import { EvidenceDrawer } from "./evidence-drawer";
import { ResearchConcentrationObservations } from "./research-concentration-observations";
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
          <Badge data-state={intent?.quality_status === "complete" ? "ready" : "partial"}>{statusLabel(intent?.quality_status ?? research.quality_status ?? research.status)}</Badge>
          <Badge variant="secondary">{intent ? stanceLabels[intent.stance] : "未形成研究意图"}</Badge>
        </div>
      </header>

      <dl className="decision-card__authority">
        <div><dt>标的</dt><dd>{intent?.display_symbol ?? research.instrument_id}</dd></div>
        <div><dt>截止时间</dt><dd className="numeric-data">{formatUtc(research.decision_time)}</dd></div>
        <div><dt>运行状态</dt><dd>{statusLabel(research.status)}</dd></div>
        <div><dt>保存的研究状态</dt><dd>{research.quality_status === "complete" ? "完整输出" : research.quality_status === "partial" ? "有限输出" : "未形成"}</dd></div>
      </dl>

      <p role="note">完整输出不代表所有证据充分；请结合逐项审查中的缺口、受阻结论与不确定性阅读。</p>

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
          <div><p>{terminalReason === "staged_action_semantics_unverified" ? "研究流程已完成，意图一致性未通过：" : "研究未完成："}<span>{reasonText(terminalReason)}</span>。保留已验证证据供排查，此状态不能支持策略目标。</p>
          {research.failed_stage_input ? <details><summary>失败调用的输入记录</summary><p>阶段：{({ research: "研究", critique: "反证", synthesis: "综合" } as const)[research.failed_stage_input.stage]}</p><p>请求摘要：<code>{research.failed_stage_input.request_hash}</code></p><p>同源输入：<code>{research.failed_stage_input.frozen_inputs_hash}</code></p></details> : null}
          </div>
        </div>
      ) : null}

      {research.stage_trace ? (
        <section className="decision-card__section" aria-label="三阶段研究过程">
          <h3>研究 → 反证 → 综合</h3>
          <p>当前三阶段结果仅供研究观察，不能直接用于正式方案优化。模型自评“充分”不会改变这一使用范围。</p>
          <p>仅展示已保存的阶段。各阶段使用同一冻结证据，阶段用量仅包含已保存输出，失败调用以总用量说明为准。</p>
          {research.stage_trace.status === "invalid" ? <p role="note">阶段记录校验失败，暂不展示论点。</p> : research.stage_trace.stages.map((stage) => (
            <details key={stage.stage} open={stage.stage === "synthesis"}>
              <summary>{({ research: "研究", critique: "反证", synthesis: "综合" } as const)[stage.stage]} · {({ complete: "已保存完整输出", partial: "仅保存部分输出", unavailable: "未保存有效输出" } as const)[stage.status]}</summary>
              <p>已保存输入 / 输出 tokens：{stage.input_tokens} / {stage.output_tokens}</p>
              {stage.input_hash ? <details><summary>本阶段输入摘要</summary><p>请求与输出格式版本：<code>{stage.input_hash}</code></p><p>同源输入：<code>{stage.frozen_inputs_hash}</code></p></details> : <p>未留存本阶段提交输入摘要。</p>}
              {stage.roles.map((role) => <section key={role.role} aria-label={`${roleLabel(role.role)}阶段论点`}>
                <h4>{roleLabel(role.role)}</h4>
                {role.reviewed_claim_ids.length ? <p>回应论点：{role.reviewed_claim_ids.map((id) => {
                  const [reviewedRole, index] = id.split(":");
                  return `${roleLabel(reviewedRole as components["schemas"]["RoleVote"]["role"])} #${Number(index) + 1}`;
                }).join("、")}</p> : null}
                <ol>{role.claims.map((claim, index) => <li key={index}><p>{claim.text}</p><EvidenceButtons evidenceRefs={claim.evidence_refs} excerpt={claim.text} onOpen={openEvidence} /></li>)}</ol>
                {stage.stage === "synthesis" && role.role === "portfolio" && role.synthesis_assessment
                  ? <SynthesisReviews assessment={role.synthesis_assessment} systemStance={intent?.stance} onOpen={openEvidence} /> : null}
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
          {intent.condition_evaluations?.length ? <ResearchConcentrationObservations evaluations={intent.condition_evaluations} onOpenEvidence={openEvidence} /> : null}
          <SavedConditions title="进入条件" id="entry-title" conditions={intent.entry_conditions} onOpen={openEvidence} />
          <SavedConditions title="退出条件" id="exit-title" conditions={intent.exit_conditions} onOpen={openEvidence} />
          <SavedConditions title="失效条件" id="invalidation-title" conditions={research.invalidation_conditions} onOpen={openEvidence} />
          <dl className="decision-card__versions">
            <div><dt>策略版本</dt><dd>{intent.strategy_version}</dd></div>
            <div><dt>模型版本</dt><dd>{intent.model_version}</dd></div>
            <div><dt>提示版本</dt><dd>{intent.prompt_version}</dd></div>
            <div><dt>数据版本</dt><dd>{intent.data_version}</dd></div>
          </dl>
        </>
      ) : (
        <p className="decision-card__empty">尚未形成可供后续分析的研究结论；系统保持观望。</p>
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

function SavedConditions({ title, id, conditions, onOpen }: {
  title: "进入条件" | "退出条件" | "失效条件";
  id: string;
  conditions: Research["invalidation_conditions"];
  onOpen: (evidence: Evidence, excerpt: string | null, opener: HTMLButtonElement) => void;
}): React.ReactElement {
  return (
    <section className="decision-card__section" aria-labelledby={id}>
      <h3 id={id}>{title}</h3>
      <p role="note">以下是已保存的研究表达式（待核验），这些{title}尚无系统求值结果；列出表达式或附有证据，不代表进入、退出或失效条件已经成立，也不授予执行权限。</p>
      {conditions.length ? (
        <ul className="decision-card__conditions">
          {conditions.map((condition, index) => {
            const comparison = typeof condition.threshold === "boolean"
              ? `运算符 ${condition.operator} · 阈值 ${String(condition.threshold)}`
              : `${operatorLabel(condition.operator)} ${formatMetric(condition.threshold, condition.unit, condition.signal)}`;
            return (
              <li key={`${condition.signal}:${index}`}>
                <span title={condition.signal}>{signalLabel(condition.signal)}</span>
                <span>{comparison}</span>
                <EvidenceButtons evidenceRefs={condition.evidence_refs} excerpt={`待核验条件：${signalLabel(condition.signal)} ${comparison}。这是已保存的研究表达式，不是求值结果。`} onOpen={onOpen} />
              </li>
            );
          })}
        </ul>
      ) : <p>研究未提供{title}；{title === "失效条件" ? "不代表不存在失效风险，当前研究不能支持策略目标。" : "不代表条件已满足或获得执行许可。"}</p>}
    </section>
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

function SynthesisReviews({ assessment, systemStance, onOpen }: {
  assessment: components["schemas"]["SynthesisAssessment"];
  systemStance: components["schemas"]["SynthesisAssessment"]["stance"] | undefined;
  onOpen: (evidence: Evidence, excerpt: string | null, opener: HTMLButtonElement) => void;
}): React.ReactElement {
  return (
    <>
      <p>模型综合判断（仅供研究）：{stanceLabels[assessment.stance]}</p>
      <p>系统最终研究状态：{systemStance ? stanceLabels[systemStance] : "未形成研究意图"}</p>
      <p role="note">模型方向与审查充分性均不代表独立核验通过，不改变系统使用限制或执行许可；“仅观察”也不等于已有充分证据支持不调整。</p>
    <details className="decision-card__reviews">
      <summary>逐项审查（模型自评）</summary>
      <p role="note">以下是已保存的模型自评，不是独立核验结论；认可异议不等于解决缺口，不改变系统证据状态或执行许可。</p>
      <p>模型自评的审查充分性：{({ sufficient: "充分", limited: "有限", insufficient: "不足" } as const)[assessment.review_conclusion]}</p>
      {assessment.reviews.map((review) => {
        const [role, index] = review.claim_id.split(":");
        const label = `${roleLabel(role as "bear" | "evidence_audit")} #${Number(index) + 1}`;
        return <section key={review.claim_id} aria-label={`${label} 审查`}>
          <h5>{label}</h5>
          <p>模型态度：{({ accepted: "认可异议", rejected: "不认可异议", unresolved: "仍有疑问" } as const)[review.status]}</p>
          <p>模型自评：{review.resolved ? "已解决" : "未解决"}</p>
          <p className="decision-card__review-label">审查理由</p>
          <p>{review.rationale}</p>
          <EvidenceButtons evidenceRefs={review.evidence_refs} excerpt={review.rationale} onOpen={onOpen} />
          <p className="decision-card__review-label">模型列出的缺失证据</p>
          {review.missing_evidence.length
            ? <ul>{review.missing_evidence.map((item, itemIndex) => <li key={itemIndex}>{item}</li>)}</ul>
            : <p>模型未列出缺失证据（不等于没有缺口）</p>}
          <p className="decision-card__review-label">模型列出的受阻结论</p>
          <p>{review.prevented_conclusion === null ? "模型未列出受阻结论" : review.prevented_conclusion}</p>
        </section>;
      })}
      <p className="decision-card__review-label">模型列出的不确定性</p>
      {assessment.uncertainties.length
        ? <ul>{assessment.uncertainties.map((item, index) => <li key={index}>{item}</li>)}</ul>
        : <p>模型未列出额外不确定性</p>}
    </details>
    </>
  );
}

function formatUtc(value: string | null | undefined): string { return formatDisplayTime(value); }
