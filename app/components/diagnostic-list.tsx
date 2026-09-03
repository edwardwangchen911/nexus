import { formatDisplayTime } from "../lib/display-format";
import { formatMetric } from "../lib/display-format";
import { reasonLabel, policyScopeLabel } from "../lib/display-labels";
import { AlertTriangle, Ban, CircleCheck, Search } from "lucide-react";
import type { components } from "../lib/generated/schema";

type DiagnosticReport = components["schemas"]["PortfolioDiagnosticResponse"];
type DiagnosticItem = DiagnosticReport["items"][number];

const needCopy = {
  BLOCKED: {
    title: "当前无法判断",
    detail: "快照、对账或数据来源核验未通过。先恢复可靠事实，再比较方案。",
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
  reduce_only: "仅研究降低仓位",
  blocked: "阻断",
};

const metricLabels: Record<string, string> = {
  VOLATILITY_20: "20 个交易日年化波动率", VOLATILITY_63: "63 个交易日年化波动率",
  BETA_126: "126 个交易日市场敏感度（Beta）", CORRELATION_126: "126 个交易日相关系数",
  CASH_RATIO: "现金占比",
  CONCENTRATION_TOP1: "单一持仓集中度",
  CONCENTRATION_TOP3: "前三大持仓占比",
  CONCENTRATION_HHI: "持仓集中度指数",
  PRELIMINARY_DIAGNOSTIC: "初步诊断使用边界",
  CASH_FLOW_RECONCILIATION: "现金流水待对账",
  MARKET_EVIDENCE: "历史风险证据待补齐",
  POSITION_MARKET_VALUE: "持仓估值完整性",
};

export function OptimizationNeedPanel({ report }: { report: DiagnosticReport }): React.ReactElement {
  const copy = needCopy[report.optimization_need];
  const Icon = copy.icon;
  return (
    <section className="optimization-need" data-need={report.optimization_need} aria-labelledby="optimization-need-title">
      <Icon aria-hidden="true" size={22} />
      <div>
        <p className="eyebrow">确定性诊断</p>
        <h2 id="optimization-need-title">{copy.title}</h2>
        <p>{copy.detail}</p>
      </div>
      <span className="state-chip" data-state={needState(report.optimization_need)}>
        {copy.title}
      </span>
    </section>
  );
}

export function DiagnosticList({ report, summary = false }: { report: DiagnosticReport; summary?: boolean }): React.ReactElement {
  const severityOrder = { critical: 0, warning: 1, info: 2 };
  const items = summary ? [...report.items].sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]).slice(0, 3) : report.items;
  return (
    <section className="diagnostic-ledger" data-summary={summary} aria-labelledby="diagnostic-ledger-title">
      <header className="diagnostic-ledger__heading">
        <div>
          <p className="eyebrow">{summary ? "按报告严重程度排序 · 最多三项" : "风险指标与阈值"}</p>
          <h2 id="diagnostic-ledger-title">{summary ? "本次诊断重点" : "组合诊断账簿"}</h2>
        </div>
        <p className="numeric-data">截至 {formatUtc(report.snapshot_as_of)}</p>
      </header>
      {report.items.length === 0 ? (
        <p className="diagnostic-ledger__empty">当前报告没有诊断明细；不等同于没有风险，请以报告结论和数据边界为准。</p>
      ) : (
        <ul className="diagnostic-list">
          {items.map((item, index) => (
            <li
              key={`${item.diagnostic_code}:${item.policy_scope}:${index}`}
              className="diagnostic-item"
              data-severity={item.severity}
              aria-label={`${item.diagnostic_code} ${severityLabels[item.severity]}`}
            >
              <header>
                <div>
                  <h3>{diagnosticLabel(item.diagnostic_code)}</h3>
                  <details className="portfolio-disclosure"><summary>诊断标识</summary><code>{item.diagnostic_code}</code></details>
                  <p><span title={item.policy_scope}>{policyScopeLabel(item.policy_scope)}</span> · {actionLabels[item.actionability]}</p>
                  {item.diagnostic_code.startsWith("CONCENTRATION_") ? <p>集中度按同币种证券市值计算，不含现金。</p> : item.diagnostic_code.split(":")[0] === "CASH_RATIO" ? <p>现金占比按同币种现金与证券总市值计算。</p> : null}
                </div>
                <span className="diagnostic-item__severity">{severityLabels[item.severity]}</span>
              </header>
              <dl className="diagnostic-item__thresholds numeric-data">
                <Threshold label="当前值" value={item.metric_value} unit={item.unit} metric={item.diagnostic_code} />
                <Threshold label="阈值" value={item.reference_limit} unit={item.unit} metric={item.diagnostic_code} />
                <Threshold label="距阈值余量" value={item.headroom} unit={item.unit} metric={item.diagnostic_code} />
              </dl>
              {item.headroom !== null ? <p>余量为负表示已越过阈值；比例指标的余量以百分点表示。</p> : null}
              {unknownReasons(item).length > 0 ? (
                <div className="diagnostic-item__unknown">
                  <AlertTriangle aria-hidden="true" size={17} />
                  <span>未知原因：{unknownReasons(item).map((reason, reasonIndex) => <span key={reason}>{reasonIndex > 0 ? "；" : ""}{reasonLabel(reason)} · <code>{reason}</code></span>)}</span>
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

function diagnosticLabel(code: string): string {
  const labels: Record<string, string> = {
    CONCENTRATION_TOP1: "最大单一持仓占比", CONCENTRATION_TOP3: "前三大持仓占比",
    CASH_RATIO: "现金占比", CORRELATION_CLUSTER: "高相关持仓合计占比", CURRENCY_EXPOSURE: "币种暴露",
    RISK_BUCKET_EXPOSURE: "风险类别暴露", LIQUIDITY_DAYS_TO_EXIT: "退出所需交易日",
    DRAWDOWN_252: "历史回撤", SOURCE_AUTHORITY: "数据来源可靠性", PRELIMINARY_DIAGNOSTIC: "初步快照诊断",
  };
  const baseCode = code.split(":")[0];
  return metricLabels[baseCode] ?? labels[baseCode] ?? code;
}

function Threshold({ label, value, unit, metric }: { label: string; value: string | null; unit: string; metric: string }): React.ReactElement {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{formatMetric(value, unit, metric, label === "距阈值余量")}</dd>
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

function formatUtc(value: string | null | undefined): string { return formatDisplayTime(value); }
