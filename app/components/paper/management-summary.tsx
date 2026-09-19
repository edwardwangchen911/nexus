"use client";
import { useEffect, useState } from "react";
import type { components } from "../../lib/generated/schema";
import { formatDisplayTime } from "../../lib/display-format";
export type ManagementSummary = components["schemas"]["PaperManagementSummaryResponse"];
export function ManagementSummaryCard({ hideUnavailable = false }: { hideUnavailable?: boolean }): React.ReactElement | null {
  const [summary, setSummary] = useState<ManagementSummary | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    void fetch("/api/v1/paper/management/portfolio", { cache: "no-store", signal: controller.signal }).then(async (response) => {
      if (!response.ok) throw new Error("summary unavailable");
      const value = await response.json() as { data?: ManagementSummary };
      if (!value.data?.read_as_of) throw new Error("invalid summary");
      if (!controller.signal.aborted) setSummary(value.data);
    }).catch(() => { if (!controller.signal.aborted) setUnavailable(true); });
    return () => controller.abort();
  }, []);
  if (!summary) return hideUnavailable ? null : <p className="pm-summary-pending">{unavailable ? "内部模拟目标与计划摘要暂不可用" : "正在读取内部模拟目标与计划"}</p>;
  return <section className="pm-panel pm-summary" aria-label="内部模拟目标与计划摘要"><div><h2>内部模拟目标与计划</h2><p>独立于真实账户 · 实盘保持锁定</p></div><div><strong>{summary.current_target ? `目标 V${summary.current_target.version_seq} · 生效` : "尚未建立目标"}</strong><p>{summary.occupying_plan ? `当前计划：${summary.occupying_plan.occupation_state === "occupied" ? "资源仍占用" : summary.occupying_plan.occupation_state === "released" ? "资源已释放" : "资源尚未查明"}` : "暂无资源占用计划"}</p><small>读取时点 {formatDisplayTime(summary.read_as_of)}</small></div><a className="button button--quiet" href={summary.occupying_plan ? `/paper?plan=${encodeURIComponent(summary.occupying_plan.batch_id)}` : "/paper"}>{summary.occupying_plan ? "查看调整计划" : "查看目标/实际详情"}</a></section>;
}
