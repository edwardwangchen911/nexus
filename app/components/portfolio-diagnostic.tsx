"use client";

import { useEffect, useState } from "react";
import type { components } from "../lib/generated/schema";
import { DiagnosticList, OptimizationNeedPanel } from "./diagnostic-list";

export type PortfolioDiagnosticReport = components["schemas"]["PortfolioDiagnosticResponse"];

export function PortfolioDiagnostic({ portfolioId, snapshotId, onReport }: {
  portfolioId: string;
  snapshotId: string;
  onReport?: (report: PortfolioDiagnosticReport | null) => void;
}): React.ReactElement {
  const [report, setReport] = useState<PortfolioDiagnosticReport | null>(null);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    onReport?.(null);
    async function load(): Promise<void> {
      try {
        const response = await fetch(`/api/v1/portfolios/${encodeURIComponent(portfolioId)}/diagnostics/latest`, {
          signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10_000)]),
        });
        if (!response.ok) throw new Error("diagnostic read failed");
        const envelope = await response.json() as { data: PortfolioDiagnosticReport };
        if (envelope.data?.portfolio_snapshot_id !== snapshotId || !Array.isArray(envelope.data.items)) {
          throw new Error("diagnostic snapshot mismatch");
        }
        if (active) {
          setReport(envelope.data);
          onReport?.(envelope.data);
        }
      } catch {
        if (active) {
          setFailed(true);
          onReport?.(null);
        }
      }
    }
    void load();
    return () => { active = false; controller.abort(); onReport?.(null); };
  }, [onReport, portfolioId, snapshotId]);

  if (failed) return <p className="inline-notice" role="status">诊断暂不可用，资产事实仍可查看。可刷新页面重新读取，不会触发复星请求。</p>;
  if (!report) return <p className="inline-notice" role="status">正在读取快照诊断…</p>;
  const preliminary = report.items.some((item) => item.diagnostic_code === "PRELIMINARY_DIAGNOSTIC");
  return <section className="page-stack" aria-label="当前快照诊断">
    {preliminary ? <header className="page-heading"><div>
      <p className="eyebrow">SNAPSHOT RESEARCH · READ ONLY</p>
      <h2>只读初步诊断</h2>
      <p>基于当前冻结快照计算现金占比与持仓集中度。这不是正式决策诊断；未完成的对账和历史风险证据仍保留为未知，不能据此生成订单。</p>
    </div></header> : <OptimizationNeedPanel report={report} />}
    <DiagnosticList report={report} />
  </section>;
}
