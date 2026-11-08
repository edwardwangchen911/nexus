"use client";

import { lazy, Suspense, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";
import { validate as validateUuid } from "uuid";
import { DataState } from "../data-state";

const EconomicLanePage = lazy(() => import("./economic-context").then((module) => ({ default: module.EconomicLanePage })));
type View = "workspace" | "decisions" | "paper" | "portfolio" | "detail";

export function EconomicContextBoundary({ view, cycleId, children }: { view: View; cycleId?: string; children: ReactNode }): React.ReactElement {
  const query = useSearchParams();
  if (!query.has("pair_slot_id") && !query.has("lane")) return <>{children}</>;
  const pairId = query.get("pair_slot_id"); const lane = query.get("lane");
  if (!pairId || !validateUuid(pairId) || (lane !== "incumbent" && lane !== "challenger")
    || [...query.keys()].some((key) => !["pair_slot_id", "lane"].includes(key))
    || query.getAll("pair_slot_id").length !== 1 || query.getAll("lane").length !== 1) {
    return <DataState state="error" title="配对上下文无效" detail="请使用已登记配对的有效侧别链接。" />;
  }
  return <Suspense fallback={<DataState state="loading" title="正在读取配对事实" detail="不会回退其他账户，也不会运行研究、采集或订单。" />}>
    <EconomicLanePage key={`${pairId}:${lane}:${cycleId ?? ""}`} context={{ pairId, lane }} view={view} cycleId={cycleId} />
  </Suspense>;
}
