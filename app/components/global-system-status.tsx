"use client";

import { RefreshCw } from "lucide-react";
import { dataStateLabels, type DataStateKind } from "./data-state";
import { useIntegrationStatuses } from "../lib/integrations/browser";
import type { IntegrationStatus as GeneratedIntegrationStatus } from "../lib/integrations/providers";

export type IntegrationStatus = GeneratedIntegrationStatus;

type GlobalSystemStatusViewProps = {
  state: DataStateKind;
  integrations: IntegrationStatus[];
  refreshing: boolean;
  onRefresh: () => void;
  notice?: string | null;
};

export function GlobalSystemStatus(): React.ReactElement {
  const resource = useIntegrationStatuses();
  return (
    <GlobalSystemStatusView
      state={resource.state}
      integrations={resource.integrations}
      refreshing={resource.refreshing}
      onRefresh={() => void resource.refresh()}
      notice={resource.notice?.text}
    />
  );
}

export function GlobalSystemStatusView({ state, integrations, refreshing, onRefresh, notice = null }: GlobalSystemStatusViewProps): React.ReactElement {
  const selected = integrations.find((integration) => integration.provider === "fosun") ?? integrations[0];
  const source = selected ? `${selected.provider.toUpperCase()} · ${selected.mode.toUpperCase()}` : "NO SOURCE";

  return (
    <section
      className="global-status"
      role="status"
      aria-label="全局系统状态"
      aria-live="polite"
      aria-atomic="true"
      data-state={state}
    >
      <div className="global-status__heading">
        <div>
          <p className="eyebrow">系统状态</p>
          <p className="global-status__source">{source}</p>
        </div>
        <button className="button button--quiet" type="button" onClick={onRefresh} disabled={refreshing}>
          <RefreshCw aria-hidden="true" size={16} />
          {refreshing ? "正在刷新" : "刷新状态"}
        </button>
      </div>
      <dl className="global-status__facts">
        <StatusFact term="数据状态" value={dataStateLabels[state]} />
        <StatusFact term="执行模式" value="RESEARCH_ONLY" />
        <StatusFact term="质量" value={selected?.quality ?? "UNKNOWN"} />
        <StatusFact term="数据截止" value={formatTimestamp(selected?.as_of)} />
        <StatusFact term="权限到期" value={formatTimestamp(selected?.capability_expires_at)} />
        <StatusFact term="契约哈希" value={selected?.contract_hash ?? "未准入"} />
      </dl>
      {selected?.next_action ? <p className="global-status__next">下一步：{selected.next_action}</p> : null}
      {notice ? <p className="inline-notice inline-notice--error">{notice}</p> : null}
    </section>
  );
}

function StatusFact({ term, value }: { term: string; value: string }): React.ReactElement {
  return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>;
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "无";
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? "无效时间" : parsed.toLocaleString("zh-CN", { hour12: false });
}
