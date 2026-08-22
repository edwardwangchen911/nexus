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

export function rebuildAllowedEngineResponse(status: number, value: unknown, upstreamHeaders = new Headers(), requestUrl?: string): Response | null {
  if ([200, 201, 202].includes(status) && isObject(value, ["data", "meta"])) {
    const { data, meta } = value;
    if (isPlain(data) && isObject(meta, ["request_id", "schema_version", "as_of"])
      && isV7(meta.request_id) && meta.schema_version === "1" && isUtcIso(meta.as_of)) return Response.json(value, { status, headers: safeHeaders(upstreamHeaders, requestUrl) });
  }
  if ([400, 401, 403, 404, 409, 422, 429, 500, 502, 503, 504].includes(status) && isObject(value, ["error"])) {
    const { error } = value;
    if (isObject(error, ["code", "message", "request_id", "details"]) && isStableCode(error.code) && allowsStatus(error.code, status) && typeof error.message === "string" && isV7(error.request_id) && isPlain(error.details)) return Response.json(value, { status, headers: safeHeaders(upstreamHeaders, requestUrl) });
  }
  return null;
}

function isV7(value: unknown): boolean { return typeof value === "string" && validateUuid(value) && uuidVersion(value) === 7; }
function isUtcIso(value: unknown): boolean { if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/.test(value)) return false; const date = new Date(value); return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 19) === value.slice(0, 19); }
function isStableCode(value: unknown): value is StableEngineErrorCode { return typeof value === "string" && Object.hasOwn(ERROR_STATUSES, value); }
function allowsStatus(code: unknown, status: number): boolean { return isStableCode(code) && ERROR_STATUSES[code].includes(status as never); }
function safeHeaders(upstream: Headers, requestUrl?: string): Headers { const headers = new Headers(); for (const name of ["retry-after", "etag"]) { const value = upstream.get(name); if (value) headers.set(name, value); } const location = upstream.get("location"); if (location && requestUrl) try { const target = new URL(location, requestUrl); const origin = new URL(requestUrl).origin; if (target.origin === origin && !target.username && !target.password) headers.set("location", `${target.pathname}${target.search}${target.hash}`); } catch { return headers; } return headers; }
function isObject(value: unknown, keys: string[]): value is Record<string, unknown> { return isPlain(value) && Object.keys(value).length === keys.length && keys.every((key) => key in value); }
function isPlain(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
