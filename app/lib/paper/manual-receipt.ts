import { validate as validateUuid } from "uuid";
import type { components } from "../generated/schema";
export type ManualReceipt = components["schemas"]["ManualRequestView"];
export type ManualMode = ManualReceipt["mode"];
export const manualEntryErrors: Record<string, string> = {
  MANUAL_ENTRY_DISABLED: "手动入口尚未启用。",
  MANUAL_CONFIGURATION_UNAVAILABLE: "正式候选池或研究配置尚未就绪。",
  MANUAL_HOST_NOT_READY: "同版本宿主尚未就绪。",
  MANUAL_SESSION_UNAVAILABLE: "当前不在支持的美股交易时段。",
  MANUAL_BUDGET_PROTECTED: "当前预算保护阻止新请求。",
  MANUAL_NOT_FOUND: "未找到该请求。",
  MANUAL_IDEMPOTENCY_CONFLICT: "原请求内容不一致，请保留原请求核对。",
  MANUAL_REQUEST_INVALID: "开始请求无效。",
  MANUAL_ACCOUNT_BUSY_OR_UNKNOWN: "已有任务、订单占用或未确认结果，暂不能开始。",
  MANUAL_PREPARATION_ACK_BLOCKED: "历史准备处置证据无法确认，请保留原请求核对。",
  MANUAL_RESULT_UNKNOWN: "请求结果尚未确认，请核对原请求。",
};
export function isManualReceipt(value: unknown): value is ManualReceipt {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const v = value as Record<string, unknown>;
  const keys = ["request_id", "mode", "requested_at", "status", "code", "cycle_id", "execution_locked"];
  if (Object.keys(v).some((k) => ![...keys, "preparation_disposition"].includes(k)) || keys.some((k) => !(k in v))
    || typeof v.request_id !== "string" || !validateUuid(v.request_id)
    || !["research_only", "research_and_paper"].includes(String(v.mode)) || v.execution_locked !== true
    || typeof v.requested_at !== "string" || !/(Z|\+00:00)$/.test(v.requested_at) || !Number.isFinite(Date.parse(v.requested_at))
    || v.cycle_id !== null && (typeof v.cycle_id !== "string" || !validateUuid(v.cycle_id))) return false;
  if ("preparation_disposition" in v && v.preparation_disposition !== null
    && (v.preparation_disposition !== "unknown_acknowledged" || v.status !== "unknown" || v.cycle_id !== null)) return false;
  const codes: Record<string, string> = {
    queued: "MANUAL_REQUEST_QUEUED", preparing: "MANUAL_PREPARATION_STARTED", active: "MANUAL_CYCLE_CREATED",
    completed: v.mode === "research_only" ? "MANUAL_RESEARCH_COMPLETED" : "MANUAL_PAPER_COMPLETED",
    unavailable: "MANUAL_INPUTS_UNAVAILABLE", unknown: "MANUAL_RESULT_UNKNOWN",
  };
  return typeof v.status === "string" && codes[v.status] !== undefined && codes[v.status] === v.code
    && (!["queued", "preparing"].includes(v.status) || v.cycle_id === null)
    && (!["active", "completed"].includes(v.status) || v.cycle_id !== null);
}
