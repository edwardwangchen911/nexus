import type { components } from "../lib/generated/schema";
import { formatDisplayTime } from "../lib/display-format";

type Context = components["schemas"]["FrozenDailyResearchContext"];

const unavailable = {
  RESEARCH_FAILED: "研究运行失败，未形成可用研究意图。",
  RESEARCH_ABSTAINED: "研究已弃权，未形成可用研究意图。",
  STAGED_ACTION_SEMANTICS_UNVERIFIED: "研究流程已完成，但行动语义尚未通过校验，不能据此认定应当调整。",
} as const;

export function ProposalResearchContext({ context }: { context: Context | null | undefined }): React.ReactElement | null {
  if (!context) return null;
  const intent = context.intent;
  return (
    <section className="proposal-research" aria-label="冻结研究依据">
      <header>
        <h3>冻结研究依据</h3>
        <span className="state-chip" data-state={context.quality_status === "partial" ? "partial" : "ready"}>
          {context.quality_status === "partial" ? "证据仍有缺口" : "研究证据完整"} · 仅供阅读
        </span>
      </header>
      <p>仅辅助阅读，不参与本草稿目标权重计算，也不会授予执行权限。估值仍使用原组合快照，后来生成的研究没有回填为旧时点证据。</p>
      <dl className="proposal-research__times">
        {([
          ["原组合估值时间", context.valuation_time],
          ["研究资料截止", context.research_as_of],
          ["研究结果实际可用时间", context.output_available_at],
          ["本次引用截止", context.context_as_of],
        ] as const).map(([label, value]) => <div key={label}><dt>{label}</dt><dd><time dateTime={value}>{formatDisplayTime(value)}</time></dd></div>)}
      </dl>
      {context.unavailable_reason ? <p>{unavailable[context.unavailable_reason]}</p> : null}
      {intent ? (
        <details>
          <summary>查看当时保存的研究论点与缺口</summary>
          {([
            ["支持论点", intent.theses], ["反方论点", intent.counter_theses],
          ] as const).map(([label, claims]) => <section key={label}>
            <h4>{label}</h4>
            {claims.length ? <ul>{claims.map((claim, index) => <li key={index}>{claim.text}</li>)}</ul> : <p>未报告。</p>}
          </section>)}
          <h4>未解决的不确定性</h4>
          {intent.uncertainties.length ? <ul>{intent.uncertainties.map((value, index) => <li key={index}>{value}</li>)}</ul> : <p>未列出；不代表已具备完整方案决策依据。</p>}
          <details>
            <summary>核对引用来源</summary>
            <ul>{intent.evidence_refs.map((ref, index) => <li key={index}>
              <p>{ref.source} · {formatDisplayTime(ref.available_at)}</p>
              <p>对象：<code>{ref.object_id}</code></p>
              <p>位置：<code>{ref.location}</code></p>
              <p>内容摘要：<code>{ref.content_hash}</code></p>
            </li>)}</ul>
          </details>
        </details>
      ) : null}
      <details>
        <summary>核对冻结研究版本</summary>
        <dl>
          {([
            ["日流程", context.root_id], ["研究运行", context.run_id],
            ["输出记录", context.output_id], ["输出内容摘要", context.output_hash],
            ["证据集合摘要", context.evidence_hash],
            ["研究输入快照", context.research_data_snapshot_id],
          ] as const).map(([label, value]) => <div key={label}><dt>{label}</dt><dd><code>{value}</code></dd></div>)}
        </dl>
      </details>
    </section>
  );
}
