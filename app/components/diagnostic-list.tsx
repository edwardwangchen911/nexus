import { AlertTriangle, Ban, CircleCheck, Search } from "lucide-react";
import type { components } from "../lib/generated/schema";

type DiagnosticReport = components["schemas"]["PortfolioDiagnosticResponse"];
type DiagnosticItem = DiagnosticReport["items"][number];

const needCopy = {
  BLOCKED: {
    title: "当前无法判断",
    detail: "快照、对账或来源权威未通过。先恢复可靠事实，再比较方案。",
    icon: Ban,
  },
  RESEARCH_REQUIRED: {
    title: "需要补充证据",
    detail: "已发现必须补齐的数据或论点；缺失值不会被当作低风险。",
    icon: Search,
  },
  OPTIMIZATION_CANDIDATE: {
    title: "值得比较优化方案",
    detail: "确定性规则已触发比较条件，但这不代表应该卖出或执行。",
    icon: AlertTriangle,
  },
  NO_ACTION: {
    title: "保持现状",
    detail: "现有证据没有证明调整能带来明确、可接受的改善。",
    icon: CircleCheck,
  },
} satisfies Record<DiagnosticReport["optimization_need"], {
  title: string;
  detail: string;
  icon: typeof Ban;
}>;

const severityLabels: Record<DiagnosticItem["severity"], string> = {
  info: "信息",
  warning: "警示",
  critical: "关键",
};

const actionLabels: Record<DiagnosticItem["actionability"], string> = {
  monitor: "持续监控",
  research: "补充研究",
  reduce_only: "仅可研究减压",
  blocked: "阻断",
};

export function OptimizationNeedPanel({ report }: { report: DiagnosticReport }): React.ReactElement {
  const copy = needCopy[report.optimization_need];
  const Icon = copy.icon;
  return (
    <section className="optimization-need" data-need={report.optimization_need} aria-labelledby="optimization-need-title">
      <Icon aria-hidden="true" size={22} />
      <div>
        <p className="eyebrow">DETERMINISTIC DIAGNOSIS</p>
        <h2 id="optimization-need-title">{copy.title}</h2>
        <p>{copy.detail}</p>
      </div>
      <span className="state-chip" data-state={needState(report.optimization_need)}>
        {report.optimization_need}
      </span>
    </section>
  );
}

export function DiagnosticList({ report }: { report: DiagnosticReport }): React.ReactElement {
  return (
    <section className="diagnostic-ledger" aria-labelledby="diagnostic-ledger-title">
      <header className="diagnostic-ledger__heading">
        <div>
          <p className="eyebrow">RISK THRESHOLD LEDGER</p>
          <h2 id="diagnostic-ledger-title">组合诊断账簿</h2>
        </div>
        <p className="numeric-data">截至 {formatUtc(report.snapshot_as_of)}</p>
      </header>
      {report.items.length === 0 ? (
        <p className="diagnostic-ledger__empty">当前报告没有诊断项；合法结论是保持现状。</p>
      ) : (
        <ul className="diagnostic-list">
          {report.items.map((item, index) => (
            <li
              key={`${item.diagnostic_code}:${item.policy_scope}:${index}`}
              className="diagnostic-item"
              data-severity={item.severity}
              aria-label={`${item.diagnostic_code} ${severityLabels[item.severity]}`}
            >
              <header>
                <div>
                  <h3>{item.diagnostic_code}</h3>
                  <p>{item.policy_scope} · {actionLabels[item.actionability]}</p>
                </div>
                <span className="diagnostic-item__severity">{severityLabels[item.severity]}</span>
              </header>
              <dl className="diagnostic-item__thresholds numeric-data">
                <Threshold label="当前值" value={item.metric_value} unit={item.unit} />
                <Threshold label="上限" value={item.reference_limit} unit={item.unit} />
                <Threshold label="余量" value={item.headroom} unit={item.unit} />
              </dl>
              {unknownReasons(item).length > 0 ? (
                <div className="diagnostic-item__unknown">
                  <AlertTriangle aria-hidden="true" size={17} />
                  <span>未知原因：{unknownReasons(item).map((reason, reasonIndex) => <span key={reason}>{reasonIndex > 0 ? "、" : ""}<code>{reason}</code></span>)}</span>
                </div>
              ) : null}
              <p className="diagnostic-item__evidence">
                可追溯证据 {item.evidence_refs.length} 条
              </p>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function Threshold({ label, value, unit }: { label: string; value: string | null; unit: string }): React.ReactElement {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{value ?? "未知"}{value === null ? "" : ` ${unit}`}</dd>
    </div>
  );
}

function unknownReasons(item: DiagnosticItem): string[] {
  return [...new Set([item.unknown_reason, item.headroom_reason].filter((reason): reason is string => reason !== null))];
}

function needState(need: DiagnosticReport["optimization_need"]): string {
  if (need === "BLOCKED") return "error";
  if (need === "RESEARCH_REQUIRED" || need === "OPTIMIZATION_CANDIDATE") return "partial";
  return "ready";
}

function formatUtc(value: string): string {
  return value.replace("T", " ").replace("Z", " UTC");
}
