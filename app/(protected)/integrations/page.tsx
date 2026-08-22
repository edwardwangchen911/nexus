"use client";

import { FlaskConical, LockKeyhole, Power, ShieldCheck } from "lucide-react";
import { useState } from "react";
import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { useIntegrationStatuses } from "../../lib/integrations/browser";
import type { IntegrationMutationEnvelope, IntegrationStatus } from "../../lib/integrations/providers";

type PendingAction = `${IntegrationStatus["provider"]}:${"test" | "mode"}` | null;
type Notice = { kind: "error" | "success"; text: string } | null;
type IntegrationsPanelProps = {
  state: DataStateKind;
  integrations: IntegrationStatus[];
  pendingAction: PendingAction;
  notice: Notice;
  onTest: (status: IntegrationStatus) => void;
  onModeChange: (status: IntegrationStatus, mode: "disabled" | "fixture") => void;
};

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

  async function runAction(status: IntegrationStatus, action: "test" | "mode", mode?: "disabled" | "fixture") {
    const actionKey: PendingAction = `${status.provider}:${action}`;
    setPendingAction(actionKey);
    setNotice(null);
    const controller = new AbortController();
    const timeout = globalThis.setTimeout(() => controller.abort(), 30_000);
    const body = action === "test" ? "{}" : JSON.stringify({ mode });
    try {
      const response = await fetch(`/api/v1/integrations/${status.provider}/${action}`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": crypto.randomUUID(),
          "if-match": `"${status.version}"`,
        },
        body,
        signal: controller.signal,
      });
      const payload = await response.json();
      if (!response.ok) {
        const code = typeof payload?.error?.code === "string" ? payload.error.code : "INTERNAL_ERROR";
        throw new Error(code);
      }
      const envelope = payload as IntegrationMutationEnvelope;
      resource.replaceStatus(envelope.data);
      setNotice({ kind: "success", text: action === "test" ? "非交易连接测试已完成。" : "Provider 模式已更新。" });
    } catch (error) {
      const timedOut = error instanceof DOMException && error.name === "AbortError";
      setNotice({
        kind: "error",
        text: timedOut
          ? "连接测试超时。未更改 Provider 模式，请重试。"
          : `操作未完成（${error instanceof Error ? error.message : "INTERNAL_ERROR"}）。状态保持安全默认值，请刷新后重试。`,
      });
    } finally {
      globalThis.clearTimeout(timeout);
      setPendingAction(null);
    }
  }

  return (
    <IntegrationsPanel
      state={resource.state}
      integrations={resource.integrations}
      pendingAction={pendingAction}
      notice={notice ?? resource.notice}
      onTest={(status) => void runAction(status, "test")}
      onModeChange={(status, mode) => void runAction(status, "mode", mode)}
    />
  );
}

export function IntegrationsPanel({ state, integrations, pendingAction, notice, onTest, onModeChange }: IntegrationsPanelProps): React.ReactElement {
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
        <p><a href="/broker-simulation">复星模拟盘验证</a>与<a href="/live-approvals">实盘逐笔批准</a>路由已存在，但当前阶段保持锁定。</p>
      </aside>
    </section>
  );
}

function IntegrationCard({ status, pendingAction, onTest, onModeChange }: Omit<IntegrationsPanelProps, "state" | "integrations" | "notice"> & { status: IntegrationStatus }): React.ReactElement {
  const readOnly = status.mode !== "disabled";
  const testing = pendingAction === `${status.provider}:test`;
  const changing = pendingAction === `${status.provider}:mode`;
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
          <button className="button button--primary" type="button" onClick={() => onTest(status)} disabled={testing}>
            <FlaskConical aria-hidden="true" size={17} />
            {testing ? "正在测试复星非交易连接" : "测试非交易连接"}
          </button>
        ) : null}
        {status.provider === "fosun" ? (
          <button className="button button--quiet" type="button" onClick={() => onModeChange(status, status.mode === "disabled" ? "fixture" : "disabled")} disabled={changing} aria-label={status.mode === "disabled" ? "启用复星 fixture" : "禁用复星 Provider"}>
            <Power aria-hidden="true" size={17} />
            {changing ? "正在更新" : status.mode === "disabled" ? "启用 fixture" : "禁用 Provider"}
          </button>
        ) : null}
      </div>
    </section>
  );
}

function Fact({ term, value }: { term: string; value: string }): React.ReactElement { return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>; }
function formatDate(value: string | null): string { if (!value) return "无"; const date = new Date(value); return Number.isNaN(date.valueOf()) ? "无效时间" : date.toLocaleString("zh-CN", { hour12: false }); }
function stateDetail(state: DataStateKind): string { const details: Record<DataStateKind, string> = { loading: "正在读取引擎的脱敏 Provider 状态。", empty: "尚无可展示的 Provider 状态。", ready: "Provider 状态已就绪。", partial: "部分 Provider 尚未启用或能力未准入。", stale: "显示最后已知状态；截止时间已过，请刷新。", error: "状态读取失败；未开启任何额外能力。", forbidden: "当前身份无权读取集成状态。" }; return details[state]; }
