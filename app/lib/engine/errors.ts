import "server-only";

import { v7 as uuidv7, validate as validateUuid, version as uuidVersion } from "uuid";

export type NexusErrorCode = "UNAUTHENTICATED" | "FORBIDDEN" | "VALIDATION_ERROR" | "NOT_FOUND" | "VERSION_CONFLICT" | "IDEMPOTENCY_CONFLICT" | "PROVIDER_UNAVAILABLE" | "INTERNAL_ERROR";
export const ERROR_STATUSES = {
  UNAUTHENTICATED: [401], FORBIDDEN: [403], VALIDATION_ERROR: [400, 422], NOT_FOUND: [404], VERSION_CONFLICT: [409], IDEMPOTENCY_CONFLICT: [409], DATA_PARTIAL: [409], DATA_STALE: [409], BROKER_STALE: [409], SOURCE_CONFLICT: [409], CAPABILITY_EXPIRED: [409], PROPOSAL_NOT_EXECUTABLE: [409], ACCOUNT_TYPE_MISMATCH: [422], APPROVAL_REQUIRED: [409], RISK_REJECTED: [422], DRAWDOWN_LOCKED: [409], RECONCILIATION_REQUIRED: [409], BUDGET_EXHAUSTED: [429], PROVIDER_UNAVAILABLE: [503], INTERNAL_ERROR: [500],
} as const;
export type StableEngineErrorCode = keyof typeof ERROR_STATUSES;

export class NexusBffError extends Error {
  constructor(readonly status: number, readonly code: NexusErrorCode, message: string) { super(message); }
}

export function errorResponse(status: number, code: NexusErrorCode, message: string): Response {
  return Response.json({ error: { code, message, request_id: uuidv7(), details: {} } }, { status });
}

export function safeBffErrorResponse(error: unknown): Response {
  if (error instanceof NexusBffError) return errorResponse(error.status, error.code, error.message);
  return errorResponse(500, "INTERNAL_ERROR", "The request could not be completed.");
}

export function rebuildAllowedEngineResponse(status: number, value: unknown, upstreamHeaders = new Headers(), requestUrl?: string, schemaPath?: string): Response | null {
  if ([200, 201, 202].includes(status) && isObject(value, ["data", "meta"])) {
    const { data, meta } = value;
    const safeData = allowedSuccessData(data, requestUrl, schemaPath);
    if (safeData !== INVALID_DATA && isObject(meta, ["request_id", "schema_version", "as_of"])
      && isV7(meta.request_id) && meta.schema_version === "1" && isUtcIso(meta.as_of)) {
      return Response.json({ data: safeData, meta }, { status, headers: safeHeaders(upstreamHeaders, requestUrl) });
    }
  }
  if ([400, 401, 403, 404, 409, 422, 429, 500, 502, 503, 504].includes(status) && isObject(value, ["error"])) {
    const { error } = value;
    if (isObject(error, ["code", "message", "request_id", "details"]) && isStableCode(error.code) && allowsStatus(error.code, status) && typeof error.message === "string" && isV7(error.request_id) && isPlain(error.details)) return Response.json(value, { status, headers: safeHeaders(upstreamHeaders, requestUrl) });
  }
  return null;
}

const INVALID_DATA = Symbol("invalid engine data");
const INTEGRATION_PROVIDERS = ["fosun", "alpaca", "sec", "fred", "openai"] as const;
const INTEGRATION_MODES = ["disabled", "fixture", "read_only"] as const;
const INTEGRATION_QUALITIES = ["PASS", "PARTIAL", "FAIL"] as const;

function allowedSuccessData(data: unknown, requestUrl?: string, schemaPath?: string): unknown | typeof INVALID_DATA {
  const pathname = enginePathname(requestUrl, schemaPath);
  if (pathname === "/internal/v1/system-status") {
    return isObject(data, ["status"]) && data.status === "ok" ? { status: "ok" } : INVALID_DATA;
  }
  if (pathname === "/internal/v1/integrations") {
    if (!Array.isArray(data)) return INVALID_DATA;
    const statuses = data.map((status) => safeIntegrationStatus(status));
    return statuses.every((status) => status !== null) ? statuses : INVALID_DATA;
  }
  const integrationAction = /^\/internal\/v1\/integrations\/(fosun|alpaca|sec|fred|openai)\/(test|mode)$/.exec(pathname);
  if (integrationAction) {
    const status = safeIntegrationStatus(data);
    return status?.provider === integrationAction[1] ? status : INVALID_DATA;
  }
  return schemaPath === undefined && isPlain(data) ? data : INVALID_DATA;
}

function safeIntegrationStatus(value: unknown): Record<string, unknown> | null {
  const keys = ["as_of", "capability_expires_at", "contract_hash", "mode", "next_action", "provider", "quality", "version"];
  if (!isObject(value, keys)) return null;
  if (!isNullableUtcIso(value.as_of) || !isNullableUtcIso(value.capability_expires_at)) return null;
  if (value.contract_hash !== null && (typeof value.contract_hash !== "string" || !/^[a-f0-9]{64}$/.test(value.contract_hash))) return null;
  if (value.next_action !== null && typeof value.next_action !== "string") return null;
  if (!includes(INTEGRATION_PROVIDERS, value.provider) || !includes(INTEGRATION_MODES, value.mode) || !includes(INTEGRATION_QUALITIES, value.quality)) return null;
  if (!Number.isInteger(value.version) || (value.version as number) < 0) return null;
  return {
    as_of: value.as_of,
    capability_expires_at: value.capability_expires_at,
    contract_hash: value.contract_hash,
    mode: value.mode,
    next_action: value.next_action,
    provider: value.provider,
    quality: value.quality,
    version: value.version,
  };
}

function enginePathname(requestUrl?: string, schemaPath?: string): string {
  if (schemaPath) return schemaPath;
  if (!requestUrl) return "";
  try {
    const pathname = new URL(requestUrl).pathname;
    return pathname.startsWith("/api/v1/") ? pathname.replace("/api/v1/", "/internal/v1/") : pathname;
  } catch { return ""; }
}

function isNullableUtcIso(value: unknown): boolean { return value === null || isUtcIso(value); }
function includes<const T extends readonly string[]>(values: T, value: unknown): value is T[number] { return typeof value === "string" && values.includes(value as T[number]); }

function isV7(value: unknown): boolean { return typeof value === "string" && validateUuid(value) && uuidVersion(value) === 7; }
function isUtcIso(value: unknown): boolean { if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/.test(value)) return false; const date = new Date(value); return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 19) === value.slice(0, 19); }
function isStableCode(value: unknown): value is StableEngineErrorCode { return typeof value === "string" && Object.hasOwn(ERROR_STATUSES, value); }
function allowsStatus(code: unknown, status: number): boolean { return isStableCode(code) && ERROR_STATUSES[code].includes(status as never); }
function safeHeaders(upstream: Headers, requestUrl?: string): Headers { const headers = new Headers(); for (const name of ["retry-after", "etag"]) { const value = upstream.get(name); if (value) headers.set(name, value); } const location = upstream.get("location"); if (location && requestUrl) try { const target = new URL(location, requestUrl); const origin = new URL(requestUrl).origin; if (target.origin === origin && !target.username && !target.password) headers.set("location", `${target.pathname}${target.search}${target.hash}`); } catch { return headers; } return headers; }
function isObject(value: unknown, keys: string[]): value is Record<string, unknown> { return isPlain(value) && Object.keys(value).length === keys.length && keys.every((key) => key in value); }
function isPlain(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
