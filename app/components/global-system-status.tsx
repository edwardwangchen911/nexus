"use client";

import { useEffect, useRef } from "react";
import { Activity, ChevronDown, RefreshCw } from "lucide-react";
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
  const sourceCode = selected ? `${selected.provider.toUpperCase()} · ${selected.mode.toUpperCase()}` : "NO SOURCE";
  const sourceLabel = selected ? `${providerLabel(selected.provider)} · ${modeLabel(selected.mode)}` : "尚未连接数据源";
  const shouldOpenEvidence = state === "error" || state === "forbidden" || Boolean(notice);
  const detailsRef = useRef<HTMLDetailsElement>(null);

  useEffect(() => {
    if (shouldOpenEvidence && detailsRef.current) detailsRef.current.open = true;
  }, [shouldOpenEvidence]);

  return (
    <section
      className="global-status"
      role="status"
      aria-label="全局系统状态"
      aria-live="polite"
      aria-atomic="true"
      data-state={state}
    >
      <div className="global-status__summary">
        <div className="global-status__identity">
          <span className="global-status__indicator" aria-hidden="true"><Activity size={17} strokeWidth={2} /></span>
          <div>
            <p className="eyebrow">系统状态</p>
            <p className="global-status__source">
              <span>{sourceLabel}</span>
              <code>{sourceCode}</code>
            </p>
          </div>
        </div>
        <div className="global-status__headline-facts">
          <span className="state-chip" data-state={state}>{dataStateLabels[state]}</span>
          <span>截止 {formatTimestamp(selected?.as_of)}</span>
        </div>
      </div>
      {notice ? <p className="inline-notice inline-notice--error">{notice}</p> : null}
      <details
        ref={detailsRef}
        className="global-status__details"
        suppressHydrationWarning
        onToggle={(event) => {
          if (shouldOpenEvidence && !event.currentTarget.open) {
            event.currentTarget.open = true;
          }
        }}
      >
        <summary>
          <span>查看系统证据</span>
          <ChevronDown aria-hidden="true" size={17} strokeWidth={1.8} />
        </summary>
        <div className="global-status__details-body">
          <div className="global-status__details-heading">
            <p>状态证据与权限边界</p>
            <button className="button button--quiet" type="button" onClick={onRefresh} disabled={refreshing}>
              <RefreshCw aria-hidden="true" size={16} />
              {refreshing ? "正在刷新" : "刷新状态"}
            </button>
          </div>
          <dl className="global-status__facts">
            <StatusFact term="数据状态" value={dataStateLabels[state]} />
            <StatusFact term="执行模式" value={<><span>仅研究</span> <code>RESEARCH_ONLY</code></>} />
            <StatusFact term="质量" value={<><span>{qualityLabel(selected?.quality)}</span> <code>{selected?.quality ?? "UNKNOWN"}</code></>} />
            <StatusFact term="数据截止" value={formatTimestamp(selected?.as_of)} />
            <StatusFact term="权限到期" value={formatTimestamp(selected?.capability_expires_at)} />
            <StatusFact term="契约哈希" value={selected?.contract_hash ?? "未准入"} />
          </dl>
          {selected?.next_action ? <p className="global-status__next">下一步：{selected.next_action}</p> : null}
        </div>
      </details>
    </section>
  );
}

function StatusFact({ term, value }: { term: string; value: React.ReactNode }): React.ReactElement {
  return <div><dt>{term}</dt><dd className="numeric-data">{value}</dd></div>;
}

function providerLabel(provider: string): string {
  return provider === "fosun" ? "复星" : provider.toUpperCase();
}

function modeLabel(mode: IntegrationStatus["mode"]): string {
  if (mode === "read_only") return "只读";
  if (mode === "fixture") return "演示数据";
  return "已禁用";
}

function qualityLabel(quality: IntegrationStatus["quality"] | undefined): string {
  if (quality === "PASS") return "通过";
  if (quality === "PARTIAL") return "不完整";
  if (quality === "FAIL") return "未通过";
  return "未知";
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "无";
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? "无效时间" : parsed.toLocaleString("zh-CN", { hour12: false });
}
