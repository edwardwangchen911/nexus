"use client";

import { LockKeyhole } from "lucide-react";
import { useEffect, useState } from "react";

import type { components } from "../../lib/generated/schema";

type LiveEvaluation = components["schemas"]["LiveEvaluationResponse"];
type LoadState = "loading" | "ready" | "forbidden" | "error";

const expectedGateKeys = [
  "internal_paper_execution",
  "oos_agent_version",
  "broker_simulation_lifecycle",
  "broker_fee_variance",
  "broker_recovery_drill",
] as const;

const gateLabels: Record<LiveEvaluation["gates"][number]["key"], string> = {
  internal_paper_execution: "内部模拟成交证据",
  oos_agent_version: "样本外版本验证",
  broker_simulation_lifecycle: "券商模拟生命周期",
  broker_fee_variance: "费用与偏差证据",
  broker_recovery_drill: "异常恢复演练",
};

const gateStatusLabels: Record<LiveEvaluation["gates"][number]["status"], string> = {
  ready: "证据已具备",
  not_ready: "证据未具备",
  unknown: "当前不可判定",
};

export default function LiveApprovalsPage(): React.ReactElement {
  const [state, setState] = useState<LoadState>("loading");
  const [evaluation, setEvaluation] = useState<LiveEvaluation | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    void loadEvaluation(controller.signal).then(({ state: nextState, evaluation: nextEvaluation }) => {
      if (controller.signal.aborted) return;
      setEvaluation(nextEvaluation);
      setState(nextState);
    });
    return () => controller.abort();
  }, []);

  return (
    <section className="locked-page" aria-labelledby="live-approvals-title">
      <LockKeyhole aria-hidden="true" size={28} />
      <p className="eyebrow">实盘能力 · 逐笔批准</p>
      <h1 id="live-approvals-title">实盘评估门禁</h1>
      <p className="state-chip" data-state="locked">实盘交易未启用</p>
      <p><strong>执行锁定 · 实盘写入关闭</strong></p>
      <p>这里只读取已持久化证据。即使全部门禁满足，也只表示可以提交用户评估，不会自动解锁或创建订单。</p>

      {state === "loading" ? <p role="status">正在读取评估证据…</p> : null}
      {state === "forbidden" ? <p role="status">无权读取评估证据；执行继续保持锁定。</p> : null}
      {state === "error" ? <p role="status">评估证据读取失败；按未就绪处理，执行继续保持锁定。</p> : null}
      {state === "ready" && evaluation ? <EvaluationFacts evaluation={evaluation} /> : null}

      <a className="button button--quiet" href="/integrations">返回集成状态</a>
    </section>
  );
}

function EvaluationFacts({ evaluation }: { evaluation: LiveEvaluation }): React.ReactElement {
  return (
    <div aria-label="实盘评估证据">
      <h2>{evaluation.status === "not_ready" ? "尚未具备实盘评估条件" : "仅达到提交用户评估条件"}</h2>
      <p>{evaluation.user_decision_required ? "任何后续启用都仍需用户另行决定。" : "执行继续保持锁定。"}</p>
      <div className="card-grid">
        {evaluation.gates.map((gate) => (
          <article className="surface-card" key={gate.key}>
            <h3>{gateLabels[gate.key]}</h3>
            <p className="state-chip" data-state={gate.status === "ready" ? "ready" : "partial"}>
              {gateStatusLabels[gate.status]}
            </p>
            <p><code>{gate.reason_code}</code></p>
            <dl>
              {gate.evidence.map((item) => (
                <div key={item.key}><dt>{item.key}</dt><dd>{item.count}</dd></div>
              ))}
            </dl>
          </article>
        ))}
      </div>
    </div>
  );
}

async function loadEvaluation(signal: AbortSignal): Promise<{ state: LoadState; evaluation: LiveEvaluation | null }> {
  try {
    const response = await fetch("/api/v1/live-evaluation", { signal });
    if (response.status === 401 || response.status === 403) return { state: "forbidden", evaluation: null };
    if (!response.ok) return { state: "error", evaluation: null };
    const payload = await response.json() as { data?: unknown };
    return validEvaluation(payload.data)
      ? { state: "ready", evaluation: payload.data }
      : { state: "error", evaluation: null };
  } catch {
    return { state: "error", evaluation: null };
  }
}

function validEvaluation(value: unknown): value is LiveEvaluation {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Partial<LiveEvaluation>;
  if (candidate.execution_locked !== true || candidate.live_write !== false || candidate.user_decision_required !== true) return false;
  if (candidate.status !== "not_ready" && candidate.status !== "eligible_for_user_review") return false;
  if (!Array.isArray(candidate.gates) || candidate.gates.length !== expectedGateKeys.length) return false;
  const keys = new Set(candidate.gates.map((gate) => gate?.key));
  return expectedGateKeys.every((key) => keys.has(key));
}
