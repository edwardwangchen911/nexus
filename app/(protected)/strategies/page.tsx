"use client";

import { formatDisplayDecimal } from "../../lib/display-decimal";
import {
  Bot,
  FlaskConical,
  GitBranch,
  History,
  LockKeyhole,
  PauseCircle,
  PlayCircle,
  RefreshCw,
  RotateCcw,
  ShieldCheck,
  XCircle,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import type { components } from "../../lib/generated/schema";

type EvolutionOverview = components["schemas"]["StrategyEvolutionData"];
type EvolutionOverviewEnvelope = components["schemas"]["StrategyEvolutionEnvelope"];
type ExperimentDetail = components["schemas"]["ExperimentDetailResponse"];
type ExperimentDetailEnvelope = components["schemas"]["ExperimentDetailEnvelope"];
type EvolutionActionEnvelope = components["schemas"]["EvolutionActionEnvelope"];
type RegistryAction = "pause_auto_promotion" | "resume_auto_promotion";
type BusyAction = "start" | "control" | "veto" | "rollback" | null;
type EvolutionCommand = {
  action: Exclude<BusyAction, null>;
  path: string;
  body: Record<string, string>;
  completedMessage: string;
  pendingMessage: string;
  idempotencyKey: string;
  etag: string;
};

type Props = {
  state: DataStateKind;
  overview: EvolutionOverview | null;
  detail: ExperimentDetail | null;
  etag: string | null;
  busyAction: BusyAction;
  errorMessage: string | null;
  successMessage: string | null;
  unknownResult: boolean;
  onRefresh: () => void;
  onSelectExperiment: (experimentId: string) => void;
  onStartExperiment: () => void;
  onControlRegistry: (action: RegistryAction) => void;
  onVetoExperiment: (experimentId: string) => void;
  onRollbackRegistry: () => void;
  onRetryUnknown: () => void;
};

let actionSequence = 0;

export default function StrategiesPage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [overview, setOverview] = useState<EvolutionOverview | null>(null);
  const [detail, setDetail] = useState<ExperimentDetail | null>(null);
  const [etag, setEtag] = useState<string | null>(null);
  const [busyAction, setBusyAction] = useState<BusyAction>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [unknownAction, setUnknownAction] = useState<EvolutionCommand | null>(null);
  const authorizedRef = useRef(false);
  const selectedExperimentIdRef = useRef<string | null>(null);
  const evidenceRef = useRef<{ overview: EvolutionOverview; detail: ExperimentDetail | null; etag: string | null } | null>(null);

  const revokeEvidence = useCallback(() => {
    authorizedRef.current = false;
    evidenceRef.current = null;
    setOverview(null);
    setDetail(null);
    setEtag(null);
    setUnknownAction(null);
    selectedExperimentIdRef.current = null;
    setState("forbidden");
  }, []);

  const invalidateDetailEvidence = useCallback((message: string | null = null) => {
    setDetail(null);
    setEtag(null);
    setState("partial");
    if (message !== null) setErrorMessage(message);
    if (evidenceRef.current) {
      evidenceRef.current = { ...evidenceRef.current, detail: null, etag: null };
    }
  }, []);

  const refresh = useCallback(async (keepFeedback = false) => {
    if (!keepFeedback) {
      setErrorMessage(null);
      setSuccessMessage(null);
    }
    setState("loading");
    try {
      const response = await fetch("/api/v1/strategy-evolution", { headers: { accept: "application/json" } });
      if (response.status === 401 || response.status === 403) {
        revokeEvidence();
        return;
      }
      if (!response.ok) {
        setState(response.status === 409 || response.status === 422 || response.status === 503 ? "partial" : "error");
        return;
      }
      const nextOverview = (await response.json() as EvolutionOverviewEnvelope).data;
      const nextEtag = response.headers.get("etag");
      const selectedExperimentId = selectedExperimentIdRef.current;
      const preferredExperiment = selectedExperimentId && nextOverview.experiments.some((item) => item.experiment_id === selectedExperimentId)
        ? selectedExperimentId
        : nextOverview.experiments[0]?.experiment_id ?? null;
      let nextDetail: ExperimentDetail | null = null;
      let detailAuthorityBound = nextEtag !== null;
      if (preferredExperiment) {
        const detailResponse = await fetch(`/api/v1/strategy-evolution/experiments/${preferredExperiment}`, { headers: { accept: "application/json" } });
        if (detailResponse.status === 401 || detailResponse.status === 403) {
          revokeEvidence();
          return;
        }
        if (detailResponse.ok) {
          const candidateDetail = (await detailResponse.json() as ExperimentDetailEnvelope).data;
          const detailEtag = detailResponse.headers.get("etag");
          detailAuthorityBound = nextEtag !== null
            && detailEtag !== null
            && detailEtag === nextEtag
            && candidateDetail.experiment.experiment_id === preferredExperiment;
          nextDetail = detailAuthorityBound ? candidateDetail : null;
        } else {
          detailAuthorityBound = false;
        }
      }
      const boundEtag = detailAuthorityBound ? nextEtag : null;
      authorizedRef.current = true;
      evidenceRef.current = { overview: nextOverview, detail: nextDetail, etag: boundEtag };
      setOverview(nextOverview);
      setDetail(nextDetail);
      setEtag(boundEtag);
      selectedExperimentIdRef.current = preferredExperiment;
      setState(!detailAuthorityBound ? "partial" : nextOverview.status === "empty" ? "empty" : nextOverview.status);
    } catch {
      const fallback = authorizedRef.current ? evidenceRef.current : null;
      setOverview(fallback?.overview ?? null);
      setDetail(fallback?.detail ?? null);
      setEtag(fallback?.etag ?? null);
      setState(fallback ? "stale" : "error");
    }
  }, [revokeEvidence]);

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);

  const selectExperiment = useCallback(async (experimentId: string) => {
    selectedExperimentIdRef.current = experimentId;
    setErrorMessage(null);
    if (
      etag === null
      || evidenceRef.current === null
      || !evidenceRef.current.overview.experiments.some((item) => item.experiment_id === experimentId)
    ) {
      invalidateDetailEvidence("无法将该实验绑定到当前策略权威。");
      return;
    }
    try {
      const response = await fetch(`/api/v1/strategy-evolution/experiments/${experimentId}`, { headers: { accept: "application/json" } });
      if (response.status === 401 || response.status === 403) {
        revokeEvidence();
        return;
      }
      if (!response.ok) {
        invalidateDetailEvidence("无法读取该实验的安全证据。");
        return;
      }
      const nextDetail = (await response.json() as ExperimentDetailEnvelope).data;
      const detailEtag = response.headers.get("etag");
      if (
        detailEtag === null
        || detailEtag !== etag
        || nextDetail.experiment.experiment_id !== experimentId
      ) {
        invalidateDetailEvidence("无法将该实验绑定到当前策略权威。");
        return;
      }
      setDetail(nextDetail);
      if (evidenceRef.current) evidenceRef.current = { ...evidenceRef.current, detail: nextDetail };
    } catch {
      invalidateDetailEvidence("无法读取该实验的安全证据。");
    }
  }, [etag, invalidateDetailEvidence, revokeEvidence]);

  const executeAction = useCallback(async (command: EvolutionCommand) => {
    if (state !== "ready") return;
    setBusyAction(command.action);
    setErrorMessage(null);
    setSuccessMessage(null);
    try {
      const response = await fetch(command.path, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": command.idempotencyKey,
          "if-match": command.etag,
        },
        body: JSON.stringify(command.body),
      });
      if (response.status === 401 || response.status === 403) {
        revokeEvidence();
        return;
      }
      if (response.status === 409) {
        setUnknownAction(null);
        setErrorMessage("资源已变化，请刷新后重试。");
        await refresh(true);
        return;
      }
      if (!response.ok) {
        if (response.status >= 500) {
          setUnknownAction(command);
          setErrorMessage("操作结果未知；在使用同一幂等键确认前，其他写操作已关闭。");
        } else {
          setUnknownAction(null);
          setErrorMessage(response.status === 422
            ? "确定性策略进化规则阻止了该操作。"
            : "操作已被拒绝，未更改原状态。");
        }
        return;
      }
      const envelope = await response.json() as EvolutionActionEnvelope;
      setUnknownAction(null);
      setSuccessMessage(response.status === 202 || ["QUEUED", "PENDING_BOUNDARY", "PENDING_ORDERS"].includes(envelope.data.status)
        ? command.pendingMessage
        : command.completedMessage);
      await refresh(true);
    } catch {
      setUnknownAction(command);
      setErrorMessage("操作结果未知；在使用同一幂等键确认前，其他写操作已关闭。");
    } finally {
      setBusyAction(null);
    }
  }, [refresh, revokeEvidence, state]);

  const performAction = useCallback((
    action: Exclude<BusyAction, null>,
    path: string,
    body: Record<string, string>,
    completedMessage: string,
    pendingMessage: string,
  ) => {
    if (!etag || state !== "ready" || unknownAction) return;
    actionSequence += 1;
    void executeAction({
      action,
      path,
      body,
      completedMessage,
      pendingMessage,
      idempotencyKey: `strategy-evolution-${Date.now()}-${actionSequence}`,
      etag,
    });
  }, [etag, executeAction, state, unknownAction]);

  const startExperiment = useCallback(() => void performAction(
    "start", "/api/v1/strategy-evolution/experiments", {}, "已启动受限策略实验。", "实验已入队，等待确定性工作流。",
  ), [performAction]);
  const controlRegistry = useCallback((action: RegistryAction) => void performAction(
    "control", "/api/v1/strategy-evolution/registry/control", { action },
    action === "pause_auto_promotion" ? "已暂停自动晋级。" : "已恢复自动晋级。",
    "控制变更已接受，等待注册表确认。",
  ), [performAction]);
  const vetoExperiment = useCallback((experimentId: string) => void performAction(
    "veto", `/api/v1/strategy-evolution/experiments/${experimentId}/veto`, { reason: "USER_VETO" }, "已记录人工否决。", "否决已接受，等待实验投影更新。",
  ), [performAction]);
  const rollbackRegistry = useCallback(() => void performAction(
    "rollback", "/api/v1/strategy-evolution/registry/rollback", { reason: "USER_REQUESTED" }, "已回滚到上一个兼容冠军。", "回滚已接受，正在等待确定性边界生效。",
  ), [performAction]);

  return <StrategiesPanel
    state={state}
    overview={overview}
    detail={detail}
    etag={etag}
    busyAction={busyAction}
    errorMessage={errorMessage}
    successMessage={successMessage}
    unknownResult={unknownAction !== null}
    onRefresh={() => void refresh()}
    onSelectExperiment={(experimentId) => void selectExperiment(experimentId)}
    onStartExperiment={startExperiment}
    onControlRegistry={controlRegistry}
    onVetoExperiment={vetoExperiment}
    onRollbackRegistry={rollbackRegistry}
    onRetryUnknown={() => { if (unknownAction) void executeAction(unknownAction); }}
  />;
}

export function StrategiesPanel(props: Props): React.ReactElement {
  const {
    state, overview, detail, etag, busyAction, errorMessage, successMessage, unknownResult,
    onRefresh, onSelectExperiment, onStartExperiment, onControlRegistry, onVetoExperiment, onRollbackRegistry, onRetryUnknown,
  } = props;
  const [rollbackOpen, setRollbackOpen] = useState(false);
  const errorRef = useRef<HTMLElement>(null);
  const rollbackTriggerRef = useRef<HTMLButtonElement>(null);
  const rollbackDialogRef = useRef<HTMLDialogElement>(null);
  const rollbackConfirmRef = useRef<HTMLButtonElement>(null);
  const restoreRollbackFocusAfterActionRef = useRef(false);
  const hasEvidence = overview !== null && ["ready", "partial", "stale"].includes(state);
  const canWrite = state === "ready" && Boolean(etag) && overview?.write_enabled === true && busyAction === null && !unknownResult;

  useEffect(() => {
    if (errorMessage) errorRef.current?.focus();
  }, [errorMessage]);
  useEffect(() => {
    if (!rollbackOpen) return;
    const dialog = rollbackDialogRef.current;
    if (dialog !== null && !dialog.open) {
      if (typeof dialog.showModal === "function") dialog.showModal();
      else dialog.setAttribute("open", "");
    }
    rollbackConfirmRef.current?.focus();
  }, [rollbackOpen]);
  useEffect(() => {
    if (
      restoreRollbackFocusAfterActionRef.current
      && !rollbackOpen
      && busyAction === null
    ) {
      restoreRollbackFocusAfterActionRef.current = false;
      globalThis.queueMicrotask(() => rollbackTriggerRef.current?.focus());
    }
  }, [busyAction, rollbackOpen]);

  function closeRollback(restoreAfterAction = false): void {
    const dialog = rollbackDialogRef.current;
    if (dialog?.open) {
      if (typeof dialog.close === "function") dialog.close();
      else dialog.removeAttribute("open");
    }
    restoreRollbackFocusAfterActionRef.current = restoreAfterAction;
    setRollbackOpen(false);
    if (!restoreAfterAction) {
      globalThis.queueMicrotask(() => rollbackTriggerRef.current?.focus());
    }
  }

  function onRollbackDialogKeyDown(event: React.KeyboardEvent<HTMLDialogElement>): void {
    if (event.key === "Escape") {
      event.preventDefault();
      closeRollback();
      return;
    }
    if (event.key !== "Tab") return;
    const controls = [...event.currentTarget.querySelectorAll<HTMLButtonElement>("button:not([disabled])")];
    if (controls.length === 0) return;
    const current = controls.indexOf(document.activeElement as HTMLButtonElement);
    const next = event.shiftKey
      ? (current <= 0 ? controls.length - 1 : current - 1)
      : (current < 0 || current === controls.length - 1 ? 0 : current + 1);
    event.preventDefault();
    controls[next].focus();
  }

  return (
    <section className="page-stack strategies-page evolution-workbench" aria-label="策略进化工作台" data-state={state}>
      <header className="page-heading evolution-heading">
        <div>
          <p className="eyebrow">策略进化 · 证据账簿</p>
          <h1>策略进化工作台</h1>
          <p>Agent 只提出可登记假设；候选生成、回测、Shadow、晋级与回滚均由确定性规则完成。</p>
        </div>
        <div className="page-actions">
          <span className="mode-badge evolution-live-guard"><LockKeyhole aria-hidden="true" size={15} />实盘关闭</span>
          <span className="state-chip" data-state={state}>{dataStateLabels[state]}</span>
          <button className="button button--quiet" type="button" aria-label="刷新策略进化证据" onClick={onRefresh}>
            <RefreshCw aria-hidden="true" size={17} />刷新证据
          </button>
        </div>
      </header>

      {errorMessage ? <section ref={errorRef} className="inline-notice inline-notice--error evolution-feedback" role="alert" tabIndex={-1}>{errorMessage}</section> : null}
      {unknownResult ? (
        <section className="inline-notice evolution-feedback" aria-label="未知操作恢复">
          <p>仅重放原请求：路径、命令、权威版本和幂等键均保持不变。</p>
          <button className="button button--quiet" type="button" disabled={busyAction !== null || state !== "ready"} onClick={onRetryUnknown}>
            <RefreshCw aria-hidden="true" size={17} />使用同一幂等键继续确认
          </button>
        </section>
      ) : null}
      <p className="inline-notice inline-notice--success evolution-feedback" role="status" aria-live="polite">{successMessage ?? ""}</p>

      {!hasEvidence ? (
        <DataState
          state={state}
          title={state === "loading" ? "正在读取策略进化权威" : "策略进化证据暂不可用"}
          detail="不使用浏览器默认值补齐冠军、门禁或 Shadow 结果。"
        />
      ) : (
        <>
          {state === "partial" || state === "stale" ? (
            <p className="inline-notice inline-notice--error" role="status">仅显示最后已知证据；当前状态失败关闭，所有写操作已禁用。</p>
          ) : null}

          <ChampionSection overview={overview} canWrite={canWrite} busyAction={busyAction} onStartExperiment={onStartExperiment} onControlRegistry={onControlRegistry} />
          <ExperimentSection overview={overview} detail={detail} canWrite={canWrite} busyAction={busyAction} onSelectExperiment={onSelectExperiment} onVetoExperiment={onVetoExperiment} />
          <ComparisonSection detail={detail} />
          <ShadowSection detail={detail} />
          <GatesSection detail={detail} />
          <HistorySection overview={overview} canWrite={canWrite} busyAction={busyAction} rollbackTriggerRef={rollbackTriggerRef} onOpenRollback={() => setRollbackOpen(true)} />
        </>
      )}

      {rollbackOpen ? (
        <div className="confirmation-dialog">
          <dialog
            ref={rollbackDialogRef}
            aria-modal="true"
            aria-labelledby="rollback-dialog-title"
            onCancel={(event) => { event.preventDefault(); closeRollback(); }}
            onKeyDown={onRollbackDialogKeyDown}
          >
            <h3 id="rollback-dialog-title">确认回滚策略冠军</h3>
            <p>回滚只影响未来内部模拟决策，不会撤销历史成交，也不会创建任何真实订单。</p>
            <p className="inline-notice">确定性注册表会选择上一个兼容且已验证的冠军；页面不能指定任意版本。</p>
            <div className="confirmation-dialog__actions">
              <button className="button button--quiet" type="button" onClick={() => closeRollback()}>取消</button>
              <button ref={rollbackConfirmRef} className="button button--danger" type="button" onClick={() => { closeRollback(true); onRollbackRegistry(); }}>确认回滚</button>
            </div>
          </dialog>
        </div>
      ) : null}
    </section>
  );
}

function ChampionSection({ overview, canWrite, busyAction, onStartExperiment, onControlRegistry }: {
  overview: EvolutionOverview;
  canWrite: boolean;
  busyAction: BusyAction;
  onStartExperiment: () => void;
  onControlRegistry: (action: RegistryAction) => void;
}): React.ReactElement {
  const champion = overview.champion;
  const controlAction = champion.auto_promotion_paused ? "resume_auto_promotion" : "pause_auto_promotion";
  return (
    <section className="evolution-section evolution-champion" aria-labelledby="champion-title">
      <header className="evolution-section__heading">
        <div><p className="eyebrow">01 / 冠军权威</p><h2 id="champion-title">{champion.strategy_version}</h2></div>
        <div className="strategy-policy-card__badges"><span className="mode-badge">内部模拟冠军</span><span className="mode-badge">generation {champion.generation}</span></div>
      </header>
      <dl className="evolution-ledger numeric-data">
        <div><dt>注册版本</dt><dd>v{champion.registry_version}</dd></div>
        <div><dt>事件序列</dt><dd>{champion.event_sequence}</dd></div>
        <div><dt>生效 session</dt><dd>{champion.effective_session ?? "GENESIS"}</dd></div>
        <div><dt>自动晋级</dt><dd>{champion.auto_promotion_paused ? "已暂停" : "已启用"}</dd></div>
        <div><dt>待生效切换</dt><dd>{champion.pending_transition ? `${champion.pending_transition} · ${champion.pending_effective_session ?? "等待边界"}` : "无"}</dd></div>
      </dl>
      <div className="evolution-actions">
        <button className="button button--primary" type="button" disabled={!canWrite || !overview.experiment_start_enabled} title={overview.experiment_start_reason} onClick={onStartExperiment}>
          <FlaskConical aria-hidden="true" size={17} />{busyAction === "start" ? "启动中…" : "启动实验"}
        </button>
        <button className="button button--quiet" type="button" disabled={!canWrite} aria-label={champion.auto_promotion_paused ? "恢复自动晋级" : "暂停自动晋级"} onClick={() => onControlRegistry(controlAction)}>
          {champion.auto_promotion_paused ? <PlayCircle aria-hidden="true" size={17} /> : <PauseCircle aria-hidden="true" size={17} />}
          {busyAction === "control" ? "更新中…" : champion.auto_promotion_paused ? "恢复自动晋级" : "暂停自动晋级"}
        </button>
      </div>
      <p className="evolution-impact"><ShieldCheck aria-hidden="true" size={18} />暂停只阻止新的自动晋级，不中断已运行的回测或 Strategy Shadow。</p>
    </section>
  );
}

function ExperimentSection({ overview, detail, canWrite, busyAction, onSelectExperiment, onVetoExperiment }: {
  overview: EvolutionOverview;
  detail: ExperimentDetail | null;
  canWrite: boolean;
  busyAction: BusyAction;
  onSelectExperiment: (experimentId: string) => void;
  onVetoExperiment: (experimentId: string) => void;
}): React.ReactElement {
  const family = overview.active_family;
  return (
    <section className="evolution-section" aria-labelledby="experiment-title">
      <header className="evolution-section__heading">
        <div><p className="eyebrow">02 / 已登记假设</p><h2 id="experiment-title">活动实验</h2></div>
        {family ? <span className="state-chip">{family.state}</span> : null}
      </header>
      {!family ? <p className="evolution-empty">当前没有活动 family；Agent 不会为空证据生成默认候选。</p> : (
        <>
          <div className="evolution-hypothesis"><Bot aria-hidden="true" size={22} /><div><h3>Agent 假设</h3><p>{family.hypothesis ?? "未登记；本次不生成候选。"}</p><small>{family.expected_mechanism ?? "缺少机制说明"}</small></div></div>
          <ul className="evolution-axis-list" aria-label="已登记搜索轴">
            {family.search_axes.map((axis) => <li key={axis.axis}>{axisLabel(axis.axis)} · {axis.candidate_value_count} 个候选值</li>)}
          </ul>
          <dl className="evolution-ledger numeric-data">
            <div><dt>候选数</dt><dd>{family.candidate_count}</dd></div>
            <div><dt>累计 OOS 试验</dt><dd>{family.cumulative_oos_trial_count}</dd></div>
            <div><dt>投影版本</dt><dd>{family.projection_version}</dd></div>
            <div><dt>下一步</dt><dd>{family.next_step}</dd></div>
          </dl>
          {overview.experiments.length > 1 ? <div className="evolution-experiment-selector" role="group" aria-label="实验证据选择">{overview.experiments.map((experiment) => <button className="button button--quiet" type="button" key={experiment.experiment_id} aria-pressed={detail?.experiment.experiment_id === experiment.experiment_id} onClick={() => onSelectExperiment(experiment.experiment_id)}>{experiment.candidate_ordinal} · {experiment.strategy_version}</button>)}</div> : null}
          {detail ? <div className="evolution-actions"><button className="button button--danger" type="button" disabled={!canWrite || !detail.write_enabled} onClick={() => onVetoExperiment(detail.experiment.experiment_id)}><XCircle aria-hidden="true" size={17} />{busyAction === "veto" ? "记录中…" : "否决本次晋级"}</button></div> : null}
          <HashRail values={[["权威", family.authority_hash], ["协议", family.protocol_hash], ["代码", family.code_hash]]} />
        </>
      )}
    </section>
  );
}

function ComparisonSection({ detail }: { detail: ExperimentDetail | null }): React.ReactElement {
  if (!detail) return <EmptyEvidence index="03" title="证据比较" />;
  const comparison = detail.comparison;
  const rows = [
    ["年化收益", comparison.annualized_return],
    ["Sharpe", comparison.sharpe],
    ["最大回撤", comparison.max_drawdown],
    ["双倍成本收益", comparison.double_cost_return],
  ] as const;
  return (
    <section className="evolution-section" aria-labelledby="comparison-title">
      <header className="evolution-section__heading"><div><p className="eyebrow">03 / 滚动窗口证据</p><h2 id="comparison-title">证据比较</h2></div><span className="state-chip">{comparison.status}</span></header>
      <div className="evolution-comparison-badges"><span className="mode-badge">{comparison.spy_comparison_complete ? "SPY 比较完整" : "SPY 比较不完整"}</span><span className="mode-badge">{comparison.equal_weight_comparison_complete ? "等权白名单比较完整" : "等权白名单比较不完整"}</span><span className="mode-badge">{comparison.data_mode}</span></div>
      {/* Bounded horizontal evidence must remain keyboard-scrollable on narrow screens. */}
      {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex */}
      <div className="table-scroll" role="region" aria-label="挑战者与冠军比较表" tabIndex={0}>
        <table className="evolution-table numeric-data"><caption>挑战者、当前冠军与差值；所有数值均来自服务端时点正确证据。</caption><thead><tr><th scope="col">指标</th><th scope="col">挑战者</th><th scope="col">冠军</th><th scope="col">差值</th></tr></thead><tbody>{rows.map(([label, metric]) => <tr key={label}><th scope="row">{label}</th><td>{formatMetric(metric.challenger)}</td><td>{formatMetric(metric.champion)}</td><td>{formatMetric(metric.delta)}</td></tr>)}</tbody></table>
      </div>
      <dl className="evolution-ledger numeric-data"><div><dt>有效 OOS 窗口</dt><dd>{comparison.valid_oos_window_count ?? "UNKNOWN"}</dd></div><div><dt>累计试验</dt><dd>{comparison.cumulative_oos_trial_count ?? "UNKNOWN"}</dd></div></dl>
      <HashRail values={[["比较", comparison.comparison_hash], ["权威", comparison.authority_hash], ["成本政策", comparison.cost_policy_hash]]} />
    </section>
  );
}

function ShadowSection({ detail }: { detail: ExperimentDetail | null }): React.ReactElement {
  if (!detail) return <EmptyEvidence index="04" title="Strategy Shadow" />;
  const shadow = detail.shadow;
  return (
    <section className="evolution-section evolution-shadow" aria-labelledby="shadow-title">
      <header className="evolution-section__heading"><div><p className="eyebrow">04 / 前向模拟证据</p><h2 id="shadow-title">Strategy Shadow</h2></div><span className="state-chip">{shadow.status}</span></header>
      <div className="evolution-progress" aria-label={`Shadow 评估已完成 ${shadow.completed_sessions} / ${shadow.evaluation_sessions} 个 session`}><div style={{ inlineSize: `${Math.min(100, shadow.completed_sessions / shadow.evaluation_sessions * 100)}%` }} /><span>{shadow.completed_sessions} / {shadow.evaluation_sessions} sessions</span></div>
      <div className="evolution-comparison-badges" aria-label="Shadow 宽限进度"><span className="mode-badge">宽限期 {shadow.grace_sessions} sessions</span><span className="mode-badge">已等待 {shadow.grace_elapsed_sessions} sessions</span></div>
      <ol className="evolution-checkpoints" aria-label="Shadow 检查点">{[5, 21, 63].map((checkpoint) => <li key={checkpoint} data-complete={shadow.checkpoints_completed.includes(checkpoint as 5 | 21 | 63)}>检查点 {checkpoint} · {shadow.checkpoints_completed.includes(checkpoint as 5 | 21 | 63) ? "已完成" : "等待"}</li>)}</ol>
      <dl className="evolution-ledger numeric-data"><div><dt>完全成交</dt><dd>{shadow.full_fill_count}</dd></div><div><dt>成本后超额</dt><dd>{formatMetric(shadow.after_cost_excess_return)}</dd></div><div><dt>回撤差</dt><dd>{formatMetric(shadow.drawdown_difference)}</dd></div><div><dt>硬风控违规</dt><dd>{shadow.hard_risk_violation_count}</dd></div><div><dt>数据完整性</dt><dd>{shadow.data_complete ? "PASS" : "FAIL"}</dd></div></dl>
      <HashRail values={[["Shadow 权威", shadow.authority_hash]]} />
    </section>
  );
}

function GatesSection({ detail }: { detail: ExperimentDetail | null }): React.ReactElement {
  if (!detail) return <EmptyEvidence index="05" title="晋级解释" />;
  return (
    <section className="evolution-section" aria-labelledby="gates-title">
      <header className="evolution-section__heading"><div><p className="eyebrow">05 / 确定性门禁</p><h2 id="gates-title">晋级解释</h2></div><span className="mode-badge">{detail.integrity_status}</span></header>
      <ol className="evolution-gates">{detail.gates.map((gate, index) => <li key={`${gate.stage}-${gate.gate}`} data-status={gate.status}><span className="evolution-gates__index">{String(index + 1).padStart(2, "0")}</span><div><header><h3>{gate.gate}</h3><span>{gateStatusLabel(gate.status)}</span></header><dl className="numeric-data"><div><dt>阶段</dt><dd>{gate.stage}</dd></div><div><dt>观测</dt><dd>{gate.observed.join(" · ") || "UNKNOWN"}</dd></div><div><dt>阈值</dt><dd>{gate.thresholds.join(" · ") || "UNKNOWN"}</dd></div><div><dt>理由</dt><dd>{gate.reason_code}</dd></div></dl></div></li>)}</ol>
    </section>
  );
}

function HistorySection({ overview, canWrite, busyAction, rollbackTriggerRef, onOpenRollback }: {
  overview: EvolutionOverview;
  canWrite: boolean;
  busyAction: BusyAction;
  rollbackTriggerRef: React.RefObject<HTMLButtonElement | null>;
  onOpenRollback: () => void;
}): React.ReactElement {
  return (
    <section className="evolution-section" aria-labelledby="history-title">
      <header className="evolution-section__heading"><div><p className="eyebrow">06 / 不可变注册表</p><h2 id="history-title">冠军历史</h2></div><History aria-hidden="true" size={22} /></header>
      <ol className="evolution-history">{overview.registry_events.map((event) => <li key={event.sequence}><GitBranch aria-hidden="true" size={18} /><div><strong>{event.event_type} · {event.target_strategy_version}</strong><span>generation {event.champion_generation} · {event.status} · {formatUtc(event.occurred_at)}</span><small>{shortHash(event.event_hash)}</small></div></li>)}</ol>
      <div className="evolution-actions"><button ref={rollbackTriggerRef} className="button button--danger" type="button" disabled={!canWrite} aria-label="回滚上一个冠军" onClick={onOpenRollback}><RotateCcw aria-hidden="true" size={17} />{busyAction === "rollback" ? "回滚处理中…" : "回滚上一个冠军"}</button></div>
    </section>
  );
}

function EmptyEvidence({ index, title }: { index: string; title: string }): React.ReactElement {
  return <section className="evolution-section evolution-empty" aria-label={title}><p className="eyebrow">{index} / 证据不可用</p><h2>{title}</h2><p>当前没有可验证证据；不使用 0 或浏览器默认值代替。</p></section>;
}

function HashRail({ values }: { values: Array<[string, string | null | undefined]> }): React.ReactElement {
  return <div className="evolution-hash-rail" aria-label="证据链">{values.map(([label, value]) => <span key={label}><small>{label} hash</small><code title={value ?? "UNKNOWN"}>{value ? shortHash(value) : "UNKNOWN"}</code></span>)}</div>;
}

function axisLabel(axis: components["schemas"]["SearchAxisSummaryResponse"]["axis"]): string {
  return ({ min_adv20_usd: "ADV20 最低成交额", min_close: "最低收盘价", target_weight: "目标权重", ranking_feature: "排名特征", positive_return_filter_enabled: "正收益过滤" })[axis];
}
function gateStatusLabel(status: components["schemas"]["GateResultResponse"]["status"]): string {
  return ({ PASS: "通过", FAIL: "失败", UNKNOWN: "未知", STALE: "过期" })[status];
}
function formatMetric(value: string | null): string { return value === null ? "UNKNOWN" : formatDisplayDecimal(value); }
function shortHash(value: string): string { return `${value.slice(0, 10)}…${value.slice(-8)}`; }
function formatUtc(value: string): string { return value.replace("T", " ").replace("Z", " UTC"); }
