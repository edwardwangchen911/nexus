import { FileSearch } from "lucide-react";
import { formatDisplayTime } from "../lib/display-format";
import type { components } from "../lib/generated/schema";
import { Button } from "./ui/button";

type Evaluation = components["schemas"]["ResearchConditionEvaluation"];
type Evidence = components["schemas"]["EvidenceRef"];
const labels: Record<string, string> = {
  "portfolio.top1_warning_exceeded": "单一持仓预警",
  "portfolio.top1_critical_exceeded": "单一持仓严重预警",
};
const results = { true: "已触发", false: "未触发", unknown: "无法核验" } as const;

export function ResearchConcentrationObservations({ evaluations, onOpenEvidence }: {
  evaluations: Evaluation[];
  onOpenEvidence: (evidence: Evidence, excerpt: string, opener: HTMLButtonElement) => void;
}): React.ReactElement {
  return <section className="decision-card__section" aria-label="组合预警核验">
    <h3>组合预警核验</h3>
    <p>按本次研究冻结的组合资料核验，口径为同币种证券内最大单项占比，不含现金；对应原估值时点，不是当前实时状态。</p>
    <p>结果仅供研究观察。触发预警不等于应买卖，未触发也不是安全保证；不代表研究充分，执行保持锁定。</p>
    {evaluations.map((evaluation) => {
      const label = labels[evaluation.condition.signal] ?? evaluation.condition.signal;
      const result = results[evaluation.result];
      const observedAt = formatDisplayTime(evaluation.observed_at);
      const excerpt = `${label}：${result}。估值时点 ${observedAt}；研究观察，不是买卖结论。`;
      return <section key={evaluation.condition.signal} className="decision-card__section" aria-label={label}>
        <h4>{label}</h4>
        <dl className="decision-card__authority">
          <div><dt>冻结时点核验结果</dt><dd><strong>{result}</strong></dd></div>
          <div><dt>原估值时点</dt><dd>{observedAt}</dd></div>
        </dl>
        {evaluation.result === "unknown" ? <p>冻结集中度资料不足，无法核验。</p> : null}
        <div className="evidence-links">{evaluation.condition.evidence_refs.map((evidence, index) => <Button
          key={`${evidence.source}:${evidence.object_id}:${evidence.content_hash}`} variant="ghost" size="sm" aria-haspopup="dialog"
          onClick={(event) => onOpenEvidence(evidence, excerpt, event.currentTarget)}>
          <FileSearch aria-hidden="true" size={16} />{index === 0 ? "查看核验证据" : `查看核验证据 ${index + 1}`}
        </Button>)}</div>
      </section>;
    })}
  </section>;
}
