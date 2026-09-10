"use client";

import { ArrowRight, LockKeyhole } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";

import type { PortfolioSnapshot } from "../(protected)/portfolio/page";
import { reasonLabel } from "../lib/display-labels";
import { formatMetric } from "../lib/display-format";
import type { components } from "../lib/generated/schema";
import { PortfolioAgentInterpretation } from "./portfolio-agent-interpretation";
import type { PortfolioDiagnosticReport } from "./portfolio-diagnostic";

type ScenarioResponse = components["schemas"]["PortfolioScenarioResponse"];
type ScenarioMetric = components["schemas"]["ScenarioMetricResponse"];

const RATIO = /^(?:0(?:\.\d{1,38})?|1(?:\.0{1,38})?)$/;
const METRICS = [
  ["security_top1", "最大单项权重", "ratio"],
  ["security_top3", "前三项权重", "ratio"],
  ["security_hhi", "证券集中度 HHI", "coefficient"],
  ["cash_weight", "现金占比", "ratio"],
  ["one_way_turnover", "单向换手率", "ratio"],
  ["estimated_cost", "预估交易成本", "ratio"],
] as const;
const COST_COMPONENTS = [
  ["commission", "券商佣金"],
  ["spread", "买卖价差"],
  ["slippage", "滑点"],
  ["regulatory_fees", "监管费用"],
  ["tax_impact", "税务影响"],
  ["total_estimated_cost_ex_tax", "成本合计（不含税）"],
] as const;

const STATUS_LABELS: Record<ScenarioResponse["objective_status"], string> = {
  met: "目标已达到",
  improved_not_met: "有所改善，尚未达到阈值",
  unchanged: "当前约束下无需调整",
  infeasible: "当前约束下无法改善",
};

export function PortfolioScenario({ snapshot, report, stale }: {
  snapshot: PortfolioSnapshot;
  report: PortfolioDiagnosticReport | null;
  stale: boolean;
}): React.ReactElement {
  const sessionKey = [
    snapshot.portfolio_snapshot_id, snapshot.as_of, snapshot.status, snapshot.decision_readiness,
    snapshot.cash_flow_quality, snapshot.execution_locked, report?.diagnostic_report_id,
    report?.snapshot_as_of, report?.optimization_need, stale,
  ].join(":");
  return <PortfolioScenarioSession key={sessionKey} snapshot={snapshot} report={report} stale={stale} />;
}

function PortfolioScenarioSession({ snapshot, report, stale }: {
  snapshot: PortfolioSnapshot;
  report: PortfolioDiagnosticReport | null;
  stale: boolean;
}): React.ReactElement {
  const [maxTurnover, setMaxTurnover] = useState("0.15");
  const [minimumCash, setMinimumCash] = useState("0.10");
  const [runningKey, setRunningKey] = useState<string | null>(null);
  const [result, setResult] = useState<{
    binding: string;
    data: ScenarioResponse;
    request: components["schemas"]["PortfolioScenarioRequest"];
  } | null>(null);
  const [error, setError] = useState<{ binding: string; text: string } | null>(null);
  const request = useRef<AbortController | null>(null);
  const reportId = report?.diagnostic_report_id ?? null;
  const snapshotAsOf = snapshot.as_of;
  const binding = `${snapshot.portfolio_snapshot_id}:${snapshotAsOf ?? "unknown"}:${reportId ?? "preliminary"}:${stale}:${maxTurnover}:${minimumCash}`;
  const running = runningKey === binding;
  const visibleResult = result?.binding === binding ? result : null;
  const visibleError = error?.binding === binding ? error.text : null;
  const invalidInputs = !RATIO.test(maxTurnover) || !RATIO.test(minimumCash);
  const blocked = stale || snapshot.status !== "READY" || snapshot.decision_readiness !== "DECISION_READY"
    || snapshot.cash_flow_quality !== "PASS" || snapshot.execution_locked !== true
    || !snapshotAsOf || !reportId || !sameUuid(report?.portfolio_snapshot_id, snapshot.portfolio_snapshot_id)
    || !sameInstant(report?.snapshot_as_of, snapshotAsOf) || report?.optimization_need === "BLOCKED";

  useEffect(() => () => request.current?.abort(), []);

  const symbols = useMemo(() => new Map(snapshot.positions.map((position) => [
    position.instrument_id,
    position.symbol,
  ])), [snapshot.positions]);

  async function evaluate(): Promise<void> {
    if (blocked || invalidInputs || running || !reportId || !snapshotAsOf) return;
    const controller = new AbortController();
    request.current = controller;
    setRunningKey(binding);
    setResult(null);
    setError(null);
    const body: components["schemas"]["PortfolioScenarioRequest"] = {
      schema_version: "portfolio_scenario_request_v1",
      portfolio_snapshot_id: snapshot.portfolio_snapshot_id,
      diagnostic_report_id: reportId,
      as_of: snapshotAsOf,
      constraints: {
        schema_version: "adjustment_scenario_constraints_v1",
        objective: "reduce_security_concentration",
        max_turnover: maxTurnover,
        minimum_cash: minimumCash,
        universe_policy: "current_holdings_only",
        allow_new_positions: false,
      },
    };
    try {
      const response = await fetch("/api/v1/portfolio-scenarios/evaluate", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": `portfolio-scenario-${snapshot.portfolio_snapshot_id}-${reportId}-${maxTurnover}-${minimumCash}`,
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (request.current !== controller) return;
      if (!response.ok) {
        setError({ binding, text: response.status === 409
          ? "当前冻结快照尚不满足情景分析条件。请先完成页面所示的对账或证据补充。"
          : "调整情景暂时无法生成；当前组合事实不受影响，也不会自动重试。" });
        return;
      }
      const envelope = await response.json() as { data?: ScenarioResponse };
      if (request.current !== controller) return;
      const data = envelope.data;
      if (!data || !sameUuid(data.portfolio_snapshot_id, snapshot.portfolio_snapshot_id)
        || !sameUuid(data.diagnostic_report_id, reportId) || !sameInstant(data.valuation_time, snapshotAsOf)
        || data.execution_locked !== true) {
        setError({ binding, text: "调整情景与当前冻结快照不一致，已拒绝显示。" });
        return;
      }
      setResult({ binding, data, request: body });
    } catch (cause) {
      if (!(cause instanceof DOMException && cause.name === "AbortError")) {
        setError({ binding, text: "调整情景暂时无法生成；当前组合事实不受影响，也不会自动重试。" });
      }
    } finally {
      if (request.current === controller) request.current = null;
      setRunningKey((current) => current === binding ? null : current);
    }
  }

  const buttonLabel = stale ? "冻结快照已过期"
    : !reportId ? "等待正式诊断"
      : running ? "正在生成只读情景…" : "生成调整情景";
  const blockedReason = scenarioBlockedReason(snapshot, report, stale);

  function updateConstraint(kind: "turnover" | "cash", value: string): void {
    request.current?.abort();
    request.current = null;
    setRunningKey(null);
    setResult(null);
    setError(null);
    if (kind === "turnover") setMaxTurnover(value);
    else setMinimumCash(value);
  }

  return <section className="portfolio-scenario" aria-label="只读调整情景">
    <header className="portfolio-scenario__heading">
      <div>
        <p className="eyebrow">DETERMINISTIC SCENARIO · MEMORY ONLY</p>
        <h2>降低证券集中度</h2>
        <p>确定性代码只在当前持仓范围内计算一个减仓情景；结果仅在本次页面内保留，不保存订单。</p>
      </div>
      <span className="state-chip" data-state="locked"><LockKeyhole aria-hidden="true" size={15} />交易保持锁定</span>
    </header>
    <div className="portfolio-scenario__controls">
      <label>最大单向换手率<input aria-label="最大单向换手率" type="number" min="0" max="1" step="0.01"
        value={maxTurnover} onChange={(event) => updateConstraint("turnover", event.target.value)} /><small>输入 0–1，例如 0.15 表示 15%</small></label>
      <label>最低现金占比<input aria-label="最低现金占比" type="number" min="0" max="1" step="0.01"
        value={minimumCash} onChange={(event) => updateConstraint("cash", event.target.value)} /><small>币种范围固定为 USD；系统最低值仍由后端约束</small></label>
      <button className="button" type="button" disabled={blocked || invalidInputs || running} onClick={() => void evaluate()}>{buttonLabel}</button>
    </div>
    {invalidInputs ? <p className="inline-notice inline-notice--error" role="status">请输入 0 到 1 之间的小数。</p> : null}
    {blockedReason ? <p className="inline-notice" role="status">{blockedReason}</p> : null}
    {visibleError ? <p className="inline-notice inline-notice--error" role="status">{visibleError}</p> : null}
    {visibleResult ? <>
      <ScenarioResult result={visibleResult.data} symbols={symbols} />
      <PortfolioAgentInterpretation
        portfolioId={snapshot.portfolio_id}
        snapshotId={snapshot.portfolio_snapshot_id}
        diagnosticReportId={reportId!}
        scenarioRequest={visibleResult.request}
        scenarioResult={visibleResult.data}
        stale={stale}
      />
    </> : null}
  </section>;
}

function ScenarioResult({ result, symbols }: { result: ScenarioResponse; symbols: Map<string, string | null> }): React.ReactElement {
  return <div className="portfolio-scenario__result" data-status={result.objective_status} role="status" aria-live="polite">
    <header><div><p className="eyebrow">CURRENT <ArrowRight aria-hidden="true" size={13} /> CANDIDATE</p>
      <h3>{STATUS_LABELS[result.objective_status]}</h3></div><span className="state-chip" data-state="partial">分析不完整</span></header>
    <p className="portfolio-scenario__scope">Top1、Top3 与 HHI 以 USD 估值域内的证券市值为分母，不含现金；现金占比按 USD 证券与现金总值计算。</p>
    {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- keyboard users need to scroll wide metric tables */}
    <div className="table-scroll" role="region" aria-label="情景指标可横向滚动" tabIndex={0}>
      <table className="portfolio-scenario__table" aria-label="当前组合与调整情景比较">
        <thead><tr><th scope="col">指标</th><th scope="col">当前组合</th><th scope="col">调整情景</th></tr></thead>
        <tbody>{METRICS.map(([key, label, unit]) => <tr key={key}><th scope="row">{label}</th>
          <td>{metricValue(result.baseline[key], unit)}</td><td>{metricValue(result.candidate[key], unit)}</td></tr>)}</tbody>
      </table>
    </div>
    <div className="portfolio-scenario__costs">
      <h4>调整情景成本证据</h4>
      <p className="portfolio-scenario__scope">口径：{result.cost_breakdown.currency} 组合价值比例 · 冻结估值时点 {new Date(result.cost_breakdown.valuation_time).toLocaleString("zh-CN")}</p>
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- keyboard users need to scroll wide cost tables */}
      <div className="table-scroll" role="region" aria-label="成本证据可横向滚动" tabIndex={0}>
        <table className="portfolio-scenario__table" aria-label="调整情景成本证据">
          <thead><tr><th scope="col">成本项</th><th scope="col">当前证据</th></tr></thead>
          <tbody>{COST_COMPONENTS.map(([key, label]) => <tr key={key}><th scope="row">{label}</th>
            <td>{metricValue(result.cost_breakdown[key], "ratio")}</td></tr>)}</tbody>
        </table>
      </div>
    </div>
    {result.positions.length > 0 ? <div className="portfolio-scenario__positions">
      <h4>情景中的证券仓位</h4><ul>{result.positions.map((position) => <li key={position.instrument_id}>
        <strong>{symbols.get(position.instrument_id) ?? "未命名标的"}</strong>
        <span>{position.action === "reduce" ? "降低" : "保持"} · {formatMetric(position.current_weight, "ratio")} <ArrowRight aria-hidden="true" size={13} /> {formatMetric(position.target_weight, "ratio")}</span>
      </li>)}</ul>
    </div> : null}
    {result.hard_constraint_reasons.length > 0 ? <p className="inline-notice inline-notice--error">约束说明：{result.hard_constraint_reasons.map(reasonLabel).join("；")}</p> : null}
    <p className="portfolio-scenario__footnote">风险指标仍可供分析；成本证据未完成，不能进入执行或 Shadow。税务继续保持未知，交易保持锁定。</p>
  </div>;
}

function metricValue(metric: ScenarioMetric, unit: string): string {
  if (metric.status === "unknown" || metric.value === null) return metric.reason ? reasonLabel(metric.reason) : "未知";
  return formatMetric(metric.value, unit);
}

function sameUuid(left: string | undefined, right: string): boolean {
  return typeof left === "string" && left.toLowerCase() === right.toLowerCase();
}

function sameInstant(left: string | undefined, right: string): boolean {
  if (typeof left !== "string") return false;
  const leftTime = Date.parse(left);
  const rightTime = Date.parse(right);
  return Number.isFinite(leftTime) && leftTime === rightTime;
}

function scenarioBlockedReason(
  snapshot: PortfolioSnapshot,
  report: PortfolioDiagnosticReport | null,
  stale: boolean,
): string | null {
  if (stale || snapshot.status === "STALE") return "冻结快照已过期；刷新并获得合格快照后才能重新计算。";
  if (snapshot.status !== "READY") return "当前快照未达到 READY，不能作为情景基线。";
  if (snapshot.decision_readiness !== "DECISION_READY") return "当前快照尚未通过决策基线核验。";
  if (snapshot.cash_flow_quality !== "PASS") return "现金流水尚未完成对账，不能计算调整情景。";
  if (snapshot.execution_locked !== true) return "交易锁状态异常，已停止情景计算。";
  if (!snapshot.as_of) return "冻结快照缺少有效时点，不能计算调整情景。";
  if (!report?.diagnostic_report_id) return "当前只有初步诊断；正式诊断生成后才可计算调整情景。";
  if (!sameUuid(report.portfolio_snapshot_id, snapshot.portfolio_snapshot_id)
    || !sameInstant(report.snapshot_as_of, snapshot.as_of)) return "正式诊断与当前冻结快照不一致。";
  if (report.optimization_need === "BLOCKED") return "正式诊断已阻止情景计算；请先处理诊断中的硬阻塞。";
  return null;
}
