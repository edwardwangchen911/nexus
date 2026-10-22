import { validate as validateUuid } from "uuid";
import type { components } from "../generated/schema";
export type ManualReceipt = components["schemas"]["ManualRequestView"];
export type ManualResearch = NonNullable<ManualReceipt["research"]>;
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
  MANUAL_PREPARATION_RECOVERY_BLOCKED: "准备收尾证据无法确认，请保留原请求核对。",
  MANUAL_PREPARATION_ACK_BLOCKED: "历史准备处置证据无法确认，请保留原请求核对。",
  MANUAL_RESULT_UNKNOWN: "请求结果尚未确认，请核对原请求。",
};
export function isManualReceipt(value: unknown): value is ManualReceipt {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const v = value as Record<string, unknown>;
  const keys = ["request_id", "mode", "requested_at", "status", "code", "cycle_id", "execution_locked"];
  if (Object.keys(v).some((k) => ![...keys, "preparation_disposition", "preparation_recovery", "research_contract", "research"].includes(k)) || keys.some((k) => !(k in v))
    || typeof v.request_id !== "string" || !validateUuid(v.request_id)
    || !["research_only", "research_and_paper"].includes(String(v.mode)) || v.execution_locked !== true
    || typeof v.requested_at !== "string" || !/(Z|\+00:00)$/.test(v.requested_at) || !Number.isFinite(Date.parse(v.requested_at))
    || v.cycle_id !== null && (typeof v.cycle_id !== "string" || !validateUuid(v.cycle_id))) return false;
  if (v.research_contract === "research_only_v2") return isPureResearchReceipt(v);
  if (v.research_contract !== undefined && v.research_contract !== null || v.research !== undefined && v.research !== null) return false;
  if ("preparation_disposition" in v && v.preparation_disposition !== null
    && (v.preparation_disposition !== "unknown_acknowledged" || v.status !== "unknown" || v.cycle_id !== null)) return false;
  const recovery = v.preparation_recovery;
  if (recovery !== undefined && recovery !== null) {
    if (typeof recovery !== "object" || Array.isArray(recovery)) return false;
    const r = recovery as Record<string, unknown>;
    if (Object.keys(r).length !== 3 || !["disposition", "external_effects", "cycle_id"].every((k) => k in r)) return false;
    if (r.disposition === "cancelled_before_call") {
      if (v.status !== "unavailable" || v.code !== "MANUAL_PREPARATION_CANCELLED" || v.cycle_id !== null || r.external_effects !== "not_started" || r.cycle_id !== null) return false;
    } else if (r.disposition === "unknown_preserved") {
      if (v.status !== "unknown" || v.cycle_id !== null || r.external_effects !== "unknown" || r.cycle_id !== null) return false;
    } else if (r.disposition === "cycle_reconciled") {
      if (!["active", "completed", "unavailable", "unknown"].includes(String(v.status)) || r.external_effects !== "cycle_recorded" || typeof r.cycle_id !== "string" || !validateUuid(r.cycle_id) || v.cycle_id !== null && v.cycle_id !== r.cycle_id) return false;
    } else return false;
    if (v.preparation_disposition) return false;
  }
  if (v.code === "MANUAL_PREPARATION_CANCELLED" && (!recovery || (recovery as Record<string, unknown>).disposition !== "cancelled_before_call")) return false;
  const codes: Record<string, string> = {
    queued: "MANUAL_REQUEST_QUEUED", preparing: "MANUAL_PREPARATION_STARTED", active: "MANUAL_CYCLE_CREATED",
    completed: v.mode === "research_only" ? "MANUAL_RESEARCH_COMPLETED" : "MANUAL_PAPER_COMPLETED",
    unavailable: v.code === "MANUAL_PREPARATION_CANCELLED" ? "MANUAL_PREPARATION_CANCELLED" : "MANUAL_INPUTS_UNAVAILABLE", unknown: "MANUAL_RESULT_UNKNOWN",
  };
  return typeof v.status === "string" && codes[v.status] !== undefined && codes[v.status] === v.code
    && (!["queued", "preparing"].includes(v.status) || v.cycle_id === null)
    && (!["active", "completed"].includes(v.status) || v.cycle_id !== null);
}

function isPureResearchReceipt(value: Record<string, unknown>): boolean {
  if (value.mode !== "research_only" || value.cycle_id !== null
    || value.preparation_disposition !== undefined && value.preparation_disposition !== null
    || value.preparation_recovery !== undefined && value.preparation_recovery !== null) return false;
  const codes: Record<string, string> = {
    queued: "MANUAL_REQUEST_QUEUED", preparing: "MANUAL_PREPARATION_STARTED", active: "MANUAL_RESEARCH_STARTED",
    completed: "MANUAL_RESEARCH_FINISHED", unavailable: "MANUAL_INPUTS_UNAVAILABLE", unknown: "MANUAL_RESULT_UNKNOWN",
  };
  if (typeof value.status !== "string" || !Object.hasOwn(codes, value.status) || codes[value.status] !== value.code) return false;
  const bound = value.research !== undefined && value.research !== null;
  if (["queued", "preparing"].includes(value.status)) return !bound;
  if (!bound) return ["unavailable", "unknown"].includes(value.status);
  return isManualResearch(value.research);
}

function isManualResearch(value: unknown): value is ManualResearch {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const research = value as Record<string, unknown>;
  const keys = ["manifest_id", "research_as_of", "manifest_hash", "members", "remote_latest_verified", "execution_locked"];
  if (Object.keys(research).length !== keys.length || !keys.every((key) => key in research)
    || typeof research.manifest_id !== "string" || !validateUuid(research.manifest_id)
    || typeof research.manifest_hash !== "string" || !/^[0-9a-f]{64}$/.test(research.manifest_hash)
    || typeof research.research_as_of !== "string" || !/(Z|\+00:00)$/.test(research.research_as_of) || !Number.isFinite(Date.parse(research.research_as_of))
    || research.remote_latest_verified !== false || research.execution_locked !== true
    || !Array.isArray(research.members) || research.members.length === 0) return false;
  const memberKeys = ["instrument_id", "run_id", "job_id", "status", "quality", "usage_known"];
  const seen = { instrument_id: new Set<string>(), run_id: new Set<string>(), job_id: new Set<string>() };
  return research.members.every((member: unknown) => {
    if (!member || typeof member !== "object" || Array.isArray(member)) return false;
    const m = member as Record<string, unknown>;
    if (Object.keys(m).length !== memberKeys.length || !memberKeys.every((key) => key in m)
      || typeof m.status !== "string" || typeof m.usage_known !== "boolean"
      || m.quality !== null && typeof m.quality !== "string") return false;
    for (const key of ["instrument_id", "run_id", "job_id"] as const) {
      const id = m[key];
      if (typeof id !== "string" || !validateUuid(id) || seen[key].has(id.toLowerCase())) return false;
      seen[key].add(id.toLowerCase());
    }
    return true;
  });
}
