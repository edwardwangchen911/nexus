"use client";

import { Bot, LockKeyhole } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { v7 as uuidv7, validate as validateUuid } from "uuid";

import { formatDisplayTime } from "../lib/display-format";
import type { components } from "../lib/generated/schema";
import { sameUuid } from "../lib/optimization/uuid";

type DecisionRun = components["schemas"]["PortfolioDecisionRunResponse"];
type InvalidationCondition = NonNullable<
  NonNullable<DecisionRun["output"]>["intent"]
>["invalidation_conditions"][number];
type ScenarioRequest = components["schemas"]["PortfolioScenarioRequest"];
type ScenarioResponse = components["schemas"]["PortfolioScenarioResponse"];
type StoredRun = { idempotencyKey: string; runId: string | null };
type Activity = "idle" | "discovering" | "submitting" | "polling" | "paused";

const STORAGE_PREFIX = "nexus:portfolio-agent:";
const MAX_POLL_MS = 45_000;
const FIRST_POLL_MS = 500;
const MAX_POLL_INTERVAL_MS = 3_000;

export type PortfolioAgentInterpretationProps = {
  portfolioId: string;
  snapshotId: string;
  diagnosticReportId: string;
  scenarioRequest: ScenarioRequest;
  scenarioResult: ScenarioResponse;
  stale: boolean;
};

export function portfolioDecisionStorageKey(
  portfolioId: string,
  snapshotId: string,
  diagnosticReportId: string,
  scenarioResultHash: string,
): string {
  return `${STORAGE_PREFIX}${portfolioId}:${snapshotId}:${diagnosticReportId}:${scenarioResultHash}`;
}

export function PortfolioAgentInterpretation({
  ...props
}: PortfolioAgentInterpretationProps): React.ReactElement {
  const scopeKey = [
    props.portfolioId,
    props.snapshotId,
    props.diagnosticReportId,
    props.scenarioResult.result_hash,
    props.stale,
  ].join(":");
  return <PortfolioAgentSession key={scopeKey} {...props} />;
}

function PortfolioAgentSession({
  portfolioId,
  snapshotId,
  diagnosticReportId,
  scenarioRequest,
  scenarioResult,
  stale,
}: PortfolioAgentInterpretationProps): React.ReactElement {
  const storageKey = portfolioDecisionStorageKey(
    portfolioId,
    snapshotId,
    diagnosticReportId,
    scenarioResult.result_hash,
  );
  const [activity, setActivity] = useState<Activity>("discovering");
  const [run, setRun] = useState<DecisionRun | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hasStoredRun, setHasStoredRun] = useState(false);
  const [discoveryUncertain, setDiscoveryUncertain] = useState(false);
  const timer = useRef<ReturnType<typeof globalThis.setTimeout> | null>(null);
  const request = useRef<AbortController | null>(null);
  const generation = useRef(0);
  const terminal = run?.status === "completed" || run?.status === "abstained" || run?.status === "failed";
  const busy = activity === "discovering" || activity === "submitting" || activity === "polling";

  const scopeQuery = useMemo(() => new URLSearchParams({
    portfolio_snapshot_id: snapshotId,
    diagnostic_report_id: diagnosticReportId,
    expected_scenario_result_hash: scenarioResult.result_hash,
  }).toString(), [snapshotId, diagnosticReportId, scenarioResult.result_hash]);

  useEffect(() => {
    if (stale) return undefined;
    const stored = readStoredRun(storageKey);
    const recoveryTimer = globalThis.setTimeout(() => {
      if (stored?.runId) {
        setHasStoredRun(true);
        void observe(stored.runId);
      } else {
        void discoverLatest();
      }
    }, 0);
    return () => {
      globalThis.clearTimeout(recoveryTimer);
      stopActiveWork();
    };
    // The complete frozen scope is encoded by storageKey and scopeQuery.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [storageKey, scopeQuery, stale]);

  function stopActiveWork(): void {
    generation.current += 1;
    request.current?.abort();
    request.current = null;
    if (timer.current !== null) globalThis.clearTimeout(timer.current);
    timer.current = null;
  }

  function accepts(candidate: DecisionRun, expectedRunId?: string): boolean {
    return candidate.execution_locked === true
      && sameUuid(candidate.portfolio_id, portfolioId)
      && sameUuid(candidate.portfolio_snapshot_id, snapshotId)
      && sameUuid(candidate.diagnostic_report_id, diagnosticReportId)
      && candidate.expected_scenario_result_hash === scenarioResult.result_hash
      && (expectedRunId === undefined || sameUuid(candidate.run_id, expectedRunId))
      && (candidate.output?.scenario_result_hash == null
        || candidate.output.scenario_result_hash === scenarioResult.result_hash);
  }

  async function discoverLatest(): Promise<void> {
    stopActiveWork();
    const token = generation.current;
    const controller = new AbortController();
    request.current = controller;
    setActivity("discovering");
    setError(null);
    try {
      const response = await fetch(`/api/v1/portfolio-decision-runs/latest?${scopeQuery}`, {
        method: "GET",
        signal: controller.signal,
      });
      if (generation.current !== token) return;
      if (response.status === 404) {
        setDiscoveryUncertain(false);
        setActivity("idle");
        return;
      }
      if (!response.ok) {
        setDiscoveryUncertain(true);
        setActivity("paused");
        setError("暂时无法确认是否已有保存的研究解读；再次点击只会重新查找，不会新建运行。");
        return;
      }
      const envelope = await response.json() as { data?: DecisionRun };
      if (!envelope.data || !accepts(envelope.data)) {
        setDiscoveryUncertain(true);
        setActivity("paused");
        setError("返回的研究解读与当前调整情景不一致，已拒绝显示；再次点击只会重新查找。");
        return;
      }
      const discovered = envelope.data;
      browserStorage()?.setItem(storageKey, JSON.stringify({
        idempotencyKey: uuidv7(),
        runId: discovered.run_id,
      }));
      setDiscoveryUncertain(false);
      setHasStoredRun(true);
      setRun(discovered);
      if (["completed", "abstained", "failed"].includes(discovered.status)) {
        setActivity("idle");
      } else {
        scheduleObservation(discovered.run_id);
      }
    } catch (cause) {
      if (generation.current === token && !(cause instanceof DOMException && cause.name === "AbortError")) {
        setDiscoveryUncertain(true);
        setActivity("paused");
        setError("暂时无法确认是否已有保存的研究解读；再次点击只会重新查找，不会新建运行。");
      }
    } finally {
      if (request.current === controller) request.current = null;
    }
  }

  async function readRun(runId: string, token: number, startedAt: number, attempt: number): Promise<void> {
    const remaining = MAX_POLL_MS - (Date.now() - startedAt);
    if (remaining <= 0) {
      setActivity("paused");
      setError("本次查看已暂停；后台运行不受影响，可稍后继续查看。");
      return;
    }
    const controller = new AbortController();
    request.current = controller;
    const deadline = globalThis.setTimeout(() => {
      if (generation.current !== token) return;
      generation.current += 1;
      controller.abort();
      if (request.current === controller) request.current = null;
      setActivity("paused");
      setError("本次查看已暂停；后台运行不受影响，可稍后继续查看。");
    }, remaining);
    try {
      const response = await fetch(`/api/v1/portfolio-decision-runs/${runId}?${scopeQuery}`, {
        method: "GET",
        signal: controller.signal,
      });
      if (generation.current !== token) return;
      if (!response.ok) {
        setActivity("paused");
        setError("暂时无法读取研究解读。运行编号已保留，可稍后继续查看。");
        return;
      }
      const envelope = await response.json() as { data?: DecisionRun };
      if (!envelope.data || !accepts(envelope.data, runId)) {
        browserStorage()?.removeItem(storageKey);
        setRun(null);
        setHasStoredRun(false);
        setActivity("idle");
        setError("返回的研究解读与当前调整情景不一致，已拒绝显示。");
        return;
      }
      setRun(envelope.data);
      setError(null);
      if (["completed", "abstained", "failed"].includes(envelope.data.status)) {
        setActivity("idle");
        return;
      }
      const elapsed = Date.now() - startedAt;
      if (elapsed >= MAX_POLL_MS) {
        setActivity("paused");
        setError("本次查看已暂停；后台运行不受影响，可稍后继续查看。");
        return;
      }
      const delay = Math.min(FIRST_POLL_MS * 2 ** attempt, MAX_POLL_INTERVAL_MS);
      timer.current = globalThis.setTimeout(
        () => void readRun(runId, token, startedAt, attempt + 1),
        Math.min(delay, MAX_POLL_MS - elapsed),
      );
    } catch (cause) {
      if (generation.current === token && !(cause instanceof DOMException && cause.name === "AbortError")) {
        setActivity("paused");
        setError("暂时无法读取研究解读。运行编号已保留，可稍后继续查看。");
      }
    } finally {
      globalThis.clearTimeout(deadline);
      if (request.current === controller) request.current = null;
    }
  }

  async function observe(runId: string): Promise<void> {
    stopActiveWork();
    const token = generation.current;
    setActivity("polling");
    setDiscoveryUncertain(false);
    setError(null);
    await readRun(runId, token, Date.now(), 0);
  }

  function scheduleObservation(runId: string): void {
    stopActiveWork();
    const token = generation.current;
    const startedAt = Date.now();
    setActivity("polling");
    timer.current = globalThis.setTimeout(
      () => void readRun(runId, token, startedAt, 1),
      FIRST_POLL_MS,
    );

  }

  async function createOrContinue(): Promise<void> {
    if (stale || busy || terminal) return;
    if (discoveryUncertain) {
      await discoverLatest();
      return;
    }
    const stored = readStoredRun(storageKey);
    if (stored?.runId) {
      await observe(stored.runId);
      return;
    }
    const idempotencyKey = stored?.idempotencyKey ?? uuidv7();
    browserStorage()?.setItem(storageKey, JSON.stringify({ idempotencyKey, runId: null }));
    stopActiveWork();
    const token = generation.current;
    const controller = new AbortController();
    request.current = controller;
    setActivity("submitting");
    setDiscoveryUncertain(false);
    setError(null);
    try {
      const response = await fetch("/api/v1/portfolio-decision-runs", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": idempotencyKey,
        },
        body: JSON.stringify({
          schema_version: "portfolio_decision_create_v1",
          portfolio_snapshot_id: snapshotId,
          diagnostic_report_id: diagnosticReportId,
          expected_scenario_result_hash: scenarioResult.result_hash,
          scenario_request: scenarioRequest,
        }),
        signal: controller.signal,
      });
      if (generation.current !== token) return;
      if (!response.ok) {
        setActivity("idle");
        setError(response.status === 409
          ? "当前情景无法登记研究解读；请确认预算和冻结证据仍然有效。"
          : "研究解读登记结果尚未确认；再次点击会复用同一请求，不会新建重复运行。");
        return;
      }
      const envelope = await response.json() as { data?: DecisionRun };
      if (!envelope.data || !accepts(envelope.data)) {
        browserStorage()?.removeItem(storageKey);
        setHasStoredRun(false);
        setActivity("idle");
        setError("返回的研究解读与当前调整情景不一致，已拒绝显示。");
        return;
      }
      const created = envelope.data;
      browserStorage()?.setItem(storageKey, JSON.stringify({ idempotencyKey, runId: created.run_id }));
      setHasStoredRun(true);
      setRun(created);
      if (["completed", "abstained", "failed"].includes(created.status)) {
        setActivity("idle");
      } else {
        scheduleObservation(created.run_id);
      }
    } catch (cause) {
      if (generation.current === token && !(cause instanceof DOMException && cause.name === "AbortError")) {
        setActivity("idle");
        setError("研究解读登记结果尚未确认；再次点击会复用同一请求，不会新建重复运行。");
      }
    } finally {
      if (request.current === controller) request.current = null;
    }
  }

  if (stale) {
    return <section className="portfolio-agent" aria-label="Agent 研究解读">
      <p className="inline-notice">调整情景已过期，不能生成或恢复 Agent 解读。</p>
    </section>;
  }

  const buttonLabel = activity === "discovering"
    ? "正在查找已保存解读…"
    : activity === "submitting" || activity === "polling"
      ? "正在生成研究解读…"
    : activity === "paused" || hasStoredRun
      ? "继续查看 Agent 解读"
      : "让 Agent 解读";

  return <section className="portfolio-agent" aria-label="Agent 研究解读" aria-busy={busy}>
    <header className="portfolio-agent__heading">
      <div>
        <p className="eyebrow"><Bot aria-hidden="true" size={14} /> AGENT RESEARCH · READ ONLY</p>
        <h3>研究解读，不是投资建议</h3>
        <p>Agent 只解释上方确定性情景，不生成权重，也不能解锁交易。</p>
      </div>
      <span className="state-chip" data-state="locked"><LockKeyhole aria-hidden="true" size={15} />交易保持锁定</span>
    </header>
    {!terminal ? <button className="button" type="button" disabled={busy}
      onClick={() => void createOrContinue()}>{buttonLabel}</button> : null}
    {busy ? <p className="portfolio-agent__progress" role="status" aria-live="polite">{activity === "discovering"
      ? "正在查找与当前情景完全匹配的已保存解读。"
      : "研究运行已登记，正在读取持久化结果；离开页面不会取消后台运行。"}</p> : null}
    {error ? <p className="inline-notice inline-notice--error" role="status">{error}</p> : null}
    {run ? <DecisionResult run={run} /> : null}
  </section>;
}

function DecisionResult({ run }: { run: DecisionRun }): React.ReactElement {
  if (run.status === "prepared" || run.status === "running") {
    return <p className="inline-notice" role="status">研究解读正在处理中。</p>;
  }
  if (run.status === "abstained" || run.status === "failed" || !run.output?.intent) {
    return <div className="portfolio-agent__result" data-status={run.status}>
      <h4>{run.status === "abstained" ? "Agent 未形成可用解读" : "Agent 解读失败"}</h4>
      <p>当前确定性情景仍然有效，未产生任何订单或执行许可。</p>
      {run.output?.error_code ? <p className="portfolio-agent__code">原因：{run.output.error_code}</p> : null}
    </div>;
  }
  const intent = run.output.intent;
  return <div className="portfolio-agent__result" data-status="completed" role="status" aria-live="polite">
    <header><div><p className="eyebrow">{intent.assessment === "explore_adjustment" ? "建议继续研究调整" : "建议继续观察"}</p>
      <h4>{intent.confidence === "high" ? "较高" : intent.confidence === "medium" ? "中等" : "较低"}置信度 · {intent.horizon === "medium_term" ? "中期" : intent.horizon === "long_term" ? "长期" : "短期"}</h4></div>
      <span className="state-chip" data-state="partial">研究结论</span></header>
    <div className="portfolio-agent__grid">
      <section aria-label="Agent 结论"><h5>为什么</h5><ul>{intent.rationale.map((claim, index) =>
        <li key={`${claim.text}:${index}`}>{claim.text}</li>)}</ul></section>
      <section aria-label="失效条件"><h5>何时需要重看</h5><ul>{intent.invalidation_conditions.map((condition, index) =>
        <li key={`${condition.signal}:${index}`}>{conditionLabel(condition)}</li>)}</ul></section>
      <section aria-label="仍未知"><h5>仍未知</h5><ul>{intent.limitations.map((limitation) =>
        <li key={limitation.code}>{limitation.text}</li>)}</ul></section>
    </div>
    <section className="portfolio-agent__evidence" aria-label="Agent 引用证据">
      <h5>引用证据</h5>
      <ul>{run.citations.map((citation) => <li key={citation.citation_id}>
        <strong>{citation.evidence.source}</strong>
        <span>{formatDisplayTime(citation.evidence.available_at)} · {citation.evidence.object_id}</span>
      </li>)}</ul>
    </section>
    <p className="portfolio-agent__footnote">研究截止 {formatDisplayTime(run.decision_time)}。成本和税务未知仍会阻止执行；交易保持锁定。</p>
  </div>;
}

function conditionLabel(
  condition: InvalidationCondition,
): string {
  const signal = condition.signal === "portfolio.scenario_objective_status" ? "情景目标状态"
    : condition.signal === "portfolio.security_top1" ? "最大单项权重"
      : condition.signal === "portfolio.cash_weight" ? "现金占比" : "预估成本状态";
  const operator = condition.operator === "eq" ? "变为"
    : ({ lt: "低于", lte: "不高于", gt: "高于", gte: "不低于" } as const)[condition.operator];
  const threshold = condition.threshold === "unchanged" ? "无变化" : condition.threshold;
  return `${signal}${operator}${threshold}`;
}

function readStoredRun(key: string): StoredRun | null {
  const storage = browserStorage();
  if (!storage) return null;
  try {
    const value: unknown = JSON.parse(storage.getItem(key) ?? "null");
    if (!isPlain(value) || typeof value.idempotencyKey !== "string"
      || value.idempotencyKey.length === 0 || value.idempotencyKey.length > 255
      || !(value.runId === null || typeof value.runId === "string" && validateUuid(value.runId))
      || Object.keys(value).some((field) => !["idempotencyKey", "runId"].includes(field))) {
      if (value !== null) storage.removeItem(key);
      return null;
    }
    return { idempotencyKey: value.idempotencyKey, runId: value.runId };
  } catch {
    storage.removeItem(key);
    return null;
  }
}

function browserStorage(): Storage | null {
  if (typeof window === "undefined") return null;
  try {
    const storage = window.localStorage;
    return typeof storage?.getItem === "function" && typeof storage?.setItem === "function"
      && typeof storage?.removeItem === "function" && typeof storage?.key === "function"
      ? storage : null;
  } catch {
    return null;
  }
}

function isPlain(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
