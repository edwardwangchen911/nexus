"use client";

import { FlaskConical, LockKeyhole, Power, ShieldCheck } from "lucide-react";
import { useState } from "react";
import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { useIntegrationStatuses } from "../../lib/integrations/browser";
import type { IntegrationMutationEnvelope, IntegrationStatus } from "../../lib/integrations/providers";

type PendingAction = `${IntegrationStatus["provider"]}:${"test" | "mode" | "reconcile"}` | null;
type Notice = { kind: "error" | "success"; text: string } | null;
type MutationAction = "test" | "mode";
type PendingMutation = {
  status: IntegrationStatus;
  action: MutationAction;
  mode?: "disabled" | "fixture";
  idempotencyKey: string;
  body: string;
};
type IntegrationsPanelProps = {
  state: DataStateKind;
  integrations: IntegrationStatus[];
  pendingAction: PendingAction;
  notice: Notice;
  unresolvedProvider?: IntegrationStatus["provider"] | null;
  onTest: (status: IntegrationStatus) => void;
  onModeChange: (status: IntegrationStatus, mode: "disabled" | "fixture") => void;
  onResolveUnknown?: () => void;
};

const MUTATION_CONFIRM_TIMEOUT_MS = 35_000;

const providerLabels: Record<IntegrationStatus["provider"], string> = {
  fosun: "复星",
  alpaca: "Alpaca",
  sec: "SEC EDGAR",
  fred: "FRED / ALFRED",
  openai: "OpenAI",
};

export default function IntegrationsPage(): React.ReactElement {
  const resource = useIntegrationStatuses();
  const [pendingAction, setPendingAction] = useState<PendingAction>(null);
  const [notice, setNotice] = useState<Notice>(null);
  const [unresolved, setUnresolved] = useState<PendingMutation | null>(null);

  async function confirmMutation(mutation: PendingMutation) {
    setPendingAction(`${mutation.status.provider}:${mutation.action}`);
    setUnresolved(null);
    setNotice(null);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const envelope = await requestIntegrationMutation(mutation);
        resource.replaceStatus(envelope.data);
        setPendingAction(null);
        setNotice({
          kind: "success",
          text: attempt === 0
            ? mutation.action === "test" ? "非交易连接测试已完成。" : "Provider 模式已更新。"
            : "首次结果未知；已使用同一幂等键确认最终结果。",
        });
        return;
      } catch (error) {
        if (error instanceof KnownMutationError) {
          setPendingAction(null);
          setNotice({
            kind: "error",
            text: `操作未完成（${error.code}）。状态保持安全默认值，请刷新后重试。`,
          });
          return;
        }
        setPendingAction(`${mutation.status.provider}:reconcile`);
        if (attempt === 0) {
          setNotice({ kind: "error", text: "请求结果未知；正在使用同一幂等键确认最终结果。" });
          continue;
        }
        await resource.refresh();
        setUnresolved(mutation);
        setNotice({
          kind: "error",
          text: "请求结果仍未知。当前快照仅供参考，相关操作保持锁定；请使用同一幂等键继续确认。",
        });
        return;
      }
    }
  }

  function runAction(status: IntegrationStatus, action: MutationAction, mode?: "disabled" | "fixture") {
    const mutation: PendingMutation = {
      status,
      action,
      mode,
      idempotencyKey: crypto.randomUUID(),
      body: action === "test" ? "{}" : JSON.stringify({ mode }),
    };
    void confirmMutation(mutation);
  }

  return (
    <IntegrationsPanel
      state={resource.state}
      integrations={resource.integrations}
      pendingAction={pendingAction}
      notice={notice ?? resource.notice}
      unresolvedProvider={unresolved?.status.provider}
      onTest={(status) => void runAction(status, "test")}
      onModeChange={(status, mode) => void runAction(status, "mode", mode)}
      onResolveUnknown={() => { if (unresolved) void confirmMutation(unresolved); }}
    />
  );
}

export function IntegrationsPanel({ state, integrations, pendingAction, notice, unresolvedProvider = null, onTest, onModeChange, onResolveUnknown = () => undefined }: IntegrationsPanelProps): React.ReactElement {
  const hasData = ["ready", "partial", "stale"].includes(state) && integrations.length > 0;
  return (
    <section className="page-stack integrations-page" aria-label="数据与能力状态页面" data-state={state}>
      <header className="page-heading">
        <div>
          <p className="eyebrow">SYSTEM / INTEGRATIONS</p>
          <h1>数据与能力状态</h1>
          <p>复星优先，补充源按数据域启用。这里只能测试非交易连接、切换安全模式，不会显示凭据或开启写能力。</p>
        </div>
        <span className="state-chip" data-state={state}>{dataStateLabels[state]}</span>
      </header>

      {notice ? <p className={`inline-notice inline-notice--${notice.kind}`}>{notice.text}</p> : null}
      {unresolvedProvider ? (
        <button className="button button--primary" type="button" onClick={onResolveUnknown}>
          使用同一幂等键继续确认
        </button>
      ) : null}

      {!hasData ? (
        <DataState state={state} title="集成状态" detail={stateDetail(state)} announce={false} />
      ) : (
        <div className="integration-grid">
          {integrations.map((status) => (
            <IntegrationCard
              key={status.provider}
              status={status}
              pendingAction={pendingAction}
              onTest={onTest}
              onModeChange={onModeChange}
            />
          ))}
        </div>
      )}

      <aside className="locked-links" aria-label="后续阶段入口">
        <LockKeyhole aria-hidden="true" size={18} />
        <div>
          <p>以下路由已存在，但当前阶段保持锁定。</p>
          <ul className="locked-links__actions">
            <li><a className="button button--quiet" href="/broker-simulation">复星模拟盘验证</a></li>
            <li><a className="button button--quiet" href="/live-approvals">实盘逐笔批准</a></li>
          </ul>
        </div>
      </aside>
    </section>
  );
}

class KnownMutationError extends Error {
  constructor(readonly code: string) { super(code); }
}

async function requestIntegrationMutation(mutation: PendingMutation): Promise<IntegrationMutationEnvelope> {
  const controller = new AbortController();
  const timeout = globalThis.setTimeout(() => controller.abort(), MUTATION_CONFIRM_TIMEOUT_MS);
  try {
    const response = await fetch(`/api/v1/integrations/${mutation.status.provider}/${mutation.action}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": mutation.idempotencyKey,
        "if-match": `"${mutation.status.version}"`,
      },
      body: mutation.body,
      signal: controller.signal,
    });
    const payload = await response.json();
    if (!response.ok) {
      const code = typeof payload?.error?.code === "string" ? payload.error.code : "INTERNAL_ERROR";
      if (code === "INTERNAL_ERROR") throw new Error("Mutation result is unknown");
      throw new KnownMutationError(code);
    }
    return payload as IntegrationMutationEnvelope;
  } finally {
    globalThis.clearTimeout(timeout);
  }
}

function IntegrationCard({ status, pendingAction, onTest, onModeChange }: Omit<IntegrationsPanelProps, "state" | "integrations" | "notice"> & { status: IntegrationStatus }): React.ReactElement {
  const readOnly = status.mode !== "disabled";
  const reconciling = pendingAction === `${status.provider}:reconcile`;
  const testing = pendingAction === `${status.provider}:test` || reconciling;
  const changing = pendingAction === `${status.provider}:mode` || reconciling;
  const label = providerLabels[status.provider];
  return (
    <section className="integration-card" aria-label={`${status.provider} 集成（${readOnly ? "只读" : "已禁用"}）`} data-read-only={readOnly ? "true" : "false"}>
      <div className="integration-card__heading">
        <div><p className="eyebrow">{status.provider.toUpperCase()}</p><h2>{label}</h2></div>
        <span className="mode-badge"><ShieldCheck aria-hidden="true" size={15} />{status.mode.toUpperCase()}</span>
      </div>
      <dl className="integration-card__facts">
        <Fact term="质量" value={status.quality} />
        <Fact term="最后成功" value={formatDate(status.as_of)} />
        <Fact term="权限到期" value={formatDate(status.capability_expires_at)} />
        <Fact term="契约哈希" value={status.contract_hash ?? "未准入"} />
        <Fact term="资源版本" value={String(status.version)} />
      </dl>
      <p className="integration-card__next">{status.next_action ?? "无需操作"}</p>
      <div className="integration-card__actions">
        {status.provider === "fosun" && status.mode === "fixture" ? (
          <button className={`button ${reconciling ? "button--quiet" : "button--primary"}`} type="button" onClick={() => onTest(status)} disabled={testing}>
            <FlaskConical aria-hidden="true" size={17} />
            {reconciling ? "正在对账权威状态" : testing ? "正在测试复星非交易连接" : "测试非交易连接"}
          </button>
        ) : null}
        {status.provider === "fosun" ? (
          <button className="button button--quiet" type="button" onClick={() => onModeChange(status, status.mode === "disabled" ? "fixture" : "disabled")} disabled={changing} aria-label={status.mode === "disabled" ? "启用复星 fixture" : "禁用复星 Provider"}>
            <Power aria-hidden="true" size={17} />
            {reconciling ? "正在对账" : changing ? "正在更新" : status.mode === "disabled" ? "启用 fixture" : "禁用 Provider"}
          </button>
        ) : null}
      </div>
    </section>
  );
}

function Fact({ term, value }: { term: string; value: string }): React.ReactElement { return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>; }
function formatDate(value: string | null): string { if (!value) return "无"; const date = new Date(value); return Number.isNaN(date.valueOf()) ? "无效时间" : date.toLocaleString("zh-CN", { hour12: false }); }
function stateDetail(state: DataStateKind): string { const details: Record<DataStateKind, string> = { loading: "正在读取引擎的脱敏 Provider 状态。", empty: "尚无可展示的 Provider 状态。", ready: "Provider 状态已就绪。", partial: "部分 Provider 尚未启用或能力未准入。", stale: "显示最后已知状态；截止时间已过，请刷新。", error: "状态读取失败；未开启任何额外能力。", forbidden: "当前身份无权读取集成状态。" }; return details[state]; }
