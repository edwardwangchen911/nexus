import "server-only";

import { v7 as uuidv7, validate as validateUuid, version as uuidVersion } from "uuid";
import {
  integrationStatusRuntimeSchema,
  brokerSyncRunRuntimeSchema,
  dailyBriefRuntimeSchema,
  marketPageRuntimeSchema,
  optimizationJobRuntimeSchema,
  optimizationLifecycleRuntimeSchema,
  optimizationProposalGroupRuntimeSchema,
  overviewRuntimeSchema,
  portfolioPageRuntimeSchema,
  portfolioDiagnosticRuntimeSchema,
  portfolioSnapshotRuntimeSchema,
  researchAdmissionRuntimeSchema,
  researchRunRuntimeSchema,
  runtimeContractSchemas,
  strategySummaryRuntimeSchema,
  watchlistMembershipRuntimeSchema,
  whatIfRuntimeSchema,
} from "../generated/runtime-contract";
import { integrationProviders } from "../integrations/providers";
import { sameUuid } from "../optimization/uuid";

export type NexusErrorCode = "UNAUTHENTICATED" | "FORBIDDEN" | "VALIDATION_ERROR" | "NOT_FOUND" | "VERSION_CONFLICT" | "IDEMPOTENCY_CONFLICT" | "PROVIDER_UNAVAILABLE" | "INTERNAL_ERROR";
export const ERROR_STATUSES = {
  UNAUTHENTICATED: [401], FORBIDDEN: [403], VALIDATION_ERROR: [400, 422, 429], NOT_FOUND: [404], VERSION_CONFLICT: [409], IDEMPOTENCY_CONFLICT: [409], DATA_PARTIAL: [409], DATA_STALE: [409], BROKER_STALE: [409], SOURCE_CONFLICT: [409], CAPABILITY_EXPIRED: [409], PROPOSAL_NOT_EXECUTABLE: [409], ACCOUNT_TYPE_MISMATCH: [422], APPROVAL_REQUIRED: [409], RISK_REJECTED: [422], DRAWDOWN_LOCKED: [409], RECONCILIATION_REQUIRED: [409], BUDGET_EXHAUSTED: [409, 429], PROVIDER_UNAVAILABLE: [503], INTERNAL_ERROR: [500],
} as const;
export type StableEngineErrorCode = keyof typeof ERROR_STATUSES;
export const SAFE_ENGINE_ERROR_MESSAGES: Record<StableEngineErrorCode, string> = {
  UNAUTHENTICATED: "Authentication is required.",
  FORBIDDEN: "Access is not permitted.",
  VALIDATION_ERROR: "The request is invalid.",
  NOT_FOUND: "The requested resource was not found.",
  VERSION_CONFLICT: "The resource changed. Refresh and try again.",
  IDEMPOTENCY_CONFLICT: "The idempotency key was already used for a different request.",
  DATA_PARTIAL: "Required data is incomplete.",
  DATA_STALE: "Required data is stale.",
  BROKER_STALE: "Broker data is stale.",
  SOURCE_CONFLICT: "Data sources disagree.",
  CAPABILITY_EXPIRED: "The provider capability is unavailable or expired.",
  PROPOSAL_NOT_EXECUTABLE: "The proposal is not executable.",
  ACCOUNT_TYPE_MISMATCH: "The account type is not permitted for this action.",
  APPROVAL_REQUIRED: "Human approval is required.",
  RISK_REJECTED: "The action was rejected by deterministic risk controls.",
  DRAWDOWN_LOCKED: "The drawdown lock is active.",
  RECONCILIATION_REQUIRED: "Reconciliation is required before continuing.",
  BUDGET_EXHAUSTED: "The configured budget is exhausted.",
  PROVIDER_UNAVAILABLE: "The provider is unavailable.",
  INTERNAL_ERROR: "The request could not be completed.",
};

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
      && isV7(meta.request_id) && meta.schema_version === "1" && isUtcIso(meta.as_of)
      && responseIdentityValid(safeData, meta, requestUrl)) {
      return Response.json({ data: safeData, meta }, { status, headers: safeHeaders(upstreamHeaders, schemaPath ? undefined : requestUrl) });
    }
  }
  if ([400, 401, 403, 404, 409, 422, 429, 500, 502, 503, 504].includes(status) && isObject(value, ["error"])) {
    const { error } = value;
    if (isObject(error, ["code", "message", "request_id", "details"]) && isStableCode(error.code) && allowsStatus(error.code, status) && typeof error.message === "string" && isV7(error.request_id) && isPlain(error.details)) {
      return Response.json({
        error: {
          code: error.code,
          message: SAFE_ENGINE_ERROR_MESSAGES[error.code],
          request_id: error.request_id,
          details: {},
        },
      }, { status, headers: safeHeaders(upstreamHeaders, schemaPath ? undefined : requestUrl) });
    }
  }
  return null;
}

const INVALID_DATA = Symbol("invalid engine data");

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
  const integrationAction = /^\/internal\/v1\/integrations\/([^/]+)\/(test|mode)$/.exec(pathname);
  if (integrationAction && integrationProviders.includes(integrationAction[1] as (typeof integrationProviders)[number])) {
    const status = safeIntegrationStatus(data);
    return status?.provider === integrationAction[1] ? status : INVALID_DATA;
  }
  if (pathname === "/internal/v1/market") {
    if (!matchesRuntimeSchema(data, marketPageRuntimeSchema) || !isPlain(data)) return INVALID_DATA;
    if (!Array.isArray(data.items) || data.items.length > 100) return INVALID_DATA;
    if (data.next_cursor !== null && (typeof data.next_cursor !== "string" || data.next_cursor.length > 2048)) return INVALID_DATA;
    const selected = data.selected_series;
    if (selected !== null && (!isPlain(selected) || !Array.isArray(selected.bars) || selected.bars.length > 500)) return INVALID_DATA;
    return data;
  }
  if (pathname === "/internal/v1/portfolios") {
    if (!matchesRuntimeSchema(data, portfolioPageRuntimeSchema) || !isPlain(data)) return INVALID_DATA;
    return Array.isArray(data.items) && data.items.length <= 50 ? data : INVALID_DATA;
  }
  if (pathname === "/internal/v1/overview") {
    if (!matchesRuntimeSchema(data, overviewRuntimeSchema) || !isPlain(data) || !isPlain(data.diagnostic_report)) return INVALID_DATA;
    if (data.portfolio_snapshot_id !== data.diagnostic_report.portfolio_snapshot_id) return INVALID_DATA;
    if (data.proposal_admission !== null && (!isPlain(data.proposal_admission) || data.proposal_admission.portfolio_snapshot_id !== data.portfolio_snapshot_id)) return INVALID_DATA;
    return data;
  }
  const portfolioDiagnostic = /^\/internal\/v1\/portfolios\/([^/]+)\/diagnostics\/latest$/.exec(pathname);
  if (portfolioDiagnostic && validateUuid(portfolioDiagnostic[1])) {
    return matchesRuntimeSchema(data, portfolioDiagnosticRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  const portfolioSnapshot = /^\/internal\/v1\/portfolios\/([^/]+)\/snapshots\/latest$/.exec(pathname);
  if (portfolioSnapshot && validateUuid(portfolioSnapshot[1])) {
    return matchesRuntimeSchema(data, portfolioSnapshotRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  const brokerSyncRun = /^\/internal\/v1\/broker-sync-runs(?:\/([^/]+))?$/.exec(pathname);
  if (brokerSyncRun && (!brokerSyncRun[1] || validateUuid(brokerSyncRun[1]))) {
    return matchesRuntimeSchema(data, brokerSyncRunRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  const watchlistAction = /^\/internal\/v1\/watchlist\/([^/]+)$/.exec(pathname);
  if (watchlistAction && validateUuid(watchlistAction[1])) {
    return matchesRuntimeSchema(data, watchlistMembershipRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/optimization-proposals") {
    return matchesRuntimeSchema(data, optimizationJobRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  const proposalGroup = /^\/internal\/v1\/optimization-proposals\/([^/]+)$/.exec(pathname);
  if (proposalGroup && validateUuid(proposalGroup[1])) {
    if (!matchesRuntimeSchema(data, optimizationProposalGroupRuntimeSchema) || !isPlain(data) || !Array.isArray(data.proposals)) return INVALID_DATA;
    const variants = data.proposals.map((proposal) => isPlain(proposal) ? proposal.variant : null);
    const ids = data.proposals.map((proposal) => isPlain(proposal) ? proposal.proposal_id : null);
    return sameUuid(data.proposal_group_id, proposalGroup[1])
      && variants.join(":") === "hold_current:minimum_change:strategy_target"
      && ids.every((id) => typeof id === "string") && new Set(ids).size === 3
      ? data
      : INVALID_DATA;
  }
  const proposalLifecycle = /^\/internal\/v1\/optimization-proposals\/([^/]+)\/(shadow|reject)$/.exec(pathname);
  if (proposalLifecycle && validateUuid(proposalLifecycle[1])) {
    return matchesRuntimeSchema(data, optimizationLifecycleRuntimeSchema) && isPlain(data)
      && sameUuid(data.proposal_group_id, proposalLifecycle[1])
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/what-if/evaluate") {
    return matchesRuntimeSchema(data, whatIfRuntimeSchema) && isPlain(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/research-runs") {
    return matchesRuntimeSchema(data, researchRunRuntimeSchema) && isPlain(data) && researchAuthorityValid(data)
      ? data
      : INVALID_DATA;
  }
  if (pathname === "/internal/v1/strategies") {
    if (!Array.isArray(data) || data.length > 100) return INVALID_DATA;
    const strategies = data.filter((strategy) =>
      matchesRuntimeSchema(strategy, strategySummaryRuntimeSchema) && isPlain(strategy));
    if (strategies.length !== data.length) return INVALID_DATA;
    const champions = strategies.filter((strategy) => strategy.champion === true);
    return strategies.every(strategyEvidenceValid)
      && champions.length === 1
      && champions[0].strategy_version === "baseline_v1"
      && champions[0].historical_agent_overlay === false
      && champions[0].live_execution_enabled === false
      ? data
      : INVALID_DATA;
  }
  const researchRun = /^\/internal\/v1\/research-runs\/([^/]+)$/.exec(pathname);
  if (researchRun && validateUuid(researchRun[1])) {
    return matchesRuntimeSchema(data, researchRunRuntimeSchema) && isPlain(data) && researchAuthorityValid(data)
      && sameUuid(data.run_id, researchRun[1])
      ? data
      : INVALID_DATA;
  }
  const instrumentResearch = /^\/internal\/v1\/instruments\/([^/]+)\/research$/.exec(pathname);
  if (instrumentResearch && validateUuid(instrumentResearch[1])) {
    return matchesRuntimeSchema(data, researchRunRuntimeSchema) && isPlain(data) && researchAuthorityValid(data)
      && sameUuid(data.instrument_id, instrumentResearch[1])
      ? data
      : INVALID_DATA;
  }
  const researchAdmission = /^\/internal\/v1\/instruments\/([^/]+)\/research-admission$/.exec(pathname);
  if (researchAdmission && validateUuid(researchAdmission[1])) {
    return matchesRuntimeSchema(data, researchAdmissionRuntimeSchema) && isPlain(data)
      && sameUuid(data.instrument_id, researchAdmission[1]) && researchAdmissionValid(data)
      ? data
      : INVALID_DATA;
  }
  const brief = /^\/internal\/v1\/briefs\/(\d{4}-\d{2}-\d{2})$/.exec(pathname);
  if (brief && isIsoDate(brief[1])) {
    return matchesRuntimeSchema(data, dailyBriefRuntimeSchema) && isPlain(data)
      && data.session_date === brief[1]
      ? data
      : INVALID_DATA;
  }
  return schemaPath === undefined && isPlain(data) ? data : INVALID_DATA;
}

function safeIntegrationStatus(value: unknown): Record<string, unknown> | null {
  if (!matchesRuntimeSchema(value, integrationStatusRuntimeSchema) || !isPlain(value)) return null;
  if (value.contract_hash !== null && (typeof value.contract_hash !== "string" || !/^[a-f0-9]{64}$/.test(value.contract_hash))) return null;
  return Object.fromEntries(
    Object.keys(integrationStatusRuntimeSchema.properties).map((key) => [key, value[key]]),
  );
}

function matchesRuntimeSchema(value: unknown, schema: unknown): boolean {
  if (!isPlain(schema)) return false;
  if (typeof schema.$ref === "string") {
    const prefix = "#/components/schemas/";
    if (!schema.$ref.startsWith(prefix)) return false;
    const referenced = (runtimeContractSchemas as Record<string, unknown>)[schema.$ref.slice(prefix.length)];
    return referenced !== undefined && matchesRuntimeSchema(value, referenced);
  }
  if (Array.isArray(schema.anyOf)) return schema.anyOf.some((candidate) => matchesRuntimeSchema(value, candidate));
  if (Array.isArray(schema.oneOf)) return schema.oneOf.filter((candidate) => matchesRuntimeSchema(value, candidate)).length === 1;
  if (Array.isArray(schema.allOf)) return schema.allOf.every((candidate) => matchesRuntimeSchema(value, candidate));
  if ("const" in schema && !Object.is(schema.const, value)) return false;
  if (Array.isArray(schema.enum) && !schema.enum.some((candidate) => Object.is(candidate, value))) return false;
  if (schema.type === "null") return value === null;
  if (schema.type === "boolean") return typeof value === "boolean";
  if (schema.type === "string") return matchesRuntimeString(value, schema);
  if (schema.type === "integer" || schema.type === "number") {
    return typeof value === "number"
      && Number.isFinite(value)
      && (schema.type !== "integer" || Number.isInteger(value))
      && (typeof schema.minimum !== "number" || value >= schema.minimum)
      && (typeof schema.maximum !== "number" || value <= schema.maximum);
  }
  if (schema.type === "array") {
    if (!Array.isArray(value)
      || (typeof schema.minItems === "number" && value.length < schema.minItems)
      || (typeof schema.maxItems === "number" && value.length > schema.maxItems)) return false;
    const prefixItems = schema.prefixItems;
    if (Array.isArray(prefixItems)) {
      if (schema.items === false && value.length > prefixItems.length) return false;
      return value.every((item, index) => index < prefixItems.length
        ? matchesRuntimeSchema(item, prefixItems[index])
        : isPlain(schema.items) && matchesRuntimeSchema(item, schema.items));
    }
    return isPlain(schema.items) && value.every((item) => matchesRuntimeSchema(item, schema.items));
  }
  if (schema.type !== "object" || !isPlain(value) || !isPlain(schema.properties)) return false;
  const properties = schema.properties;
  const required = Array.isArray(schema.required) && schema.required.every((key) => typeof key === "string")
    ? schema.required as string[]
    : [];
  if (!required.every((key) => key in value)) return false;
  if (schema.additionalProperties === false && Object.keys(value).some((key) => !(key in properties))) return false;
  return Object.entries(value).every(([key, item]) => key in properties && matchesRuntimeSchema(item, properties[key]));
}

function matchesRuntimeString(value: unknown, schema: Record<string, unknown>): boolean {
  if (typeof value !== "string") return false;
  if (typeof schema.minLength === "number" && value.length < schema.minLength) return false;
  if (typeof schema.maxLength === "number" && value.length > schema.maxLength) return false;
  if (typeof schema.pattern === "string") {
    try { if (!new RegExp(schema.pattern).test(value)) return false; } catch { return false; }
  }
  if (schema.format === "date-time" && !isUtcIso(value)) return false;
  if (schema.format === "date" && !isIsoDate(value)) return false;
  if (schema.format === "uuid" && !validateUuid(value)) return false;
  return true;
}

function enginePathname(requestUrl?: string, schemaPath?: string): string {
  if (schemaPath) return schemaPath;
  if (!requestUrl) return "";
  try {
    const pathname = new URL(requestUrl).pathname;
    return pathname.startsWith("/api/v1/") ? pathname.replace("/api/v1/", "/internal/v1/") : pathname;
  } catch { return ""; }
}

function responseIdentityValid(data: unknown, meta: Record<string, unknown>, requestUrl?: string): boolean {
  if (!requestUrl) return true;
  try {
    const url = new URL(requestUrl);
    if (/^\/(?:api|internal)\/v1\/instruments\/[^/]+\/research-admission$/.test(url.pathname)) {
      const values = url.searchParams.getAll("as_of");
      return values.length === 1 && isUtcIso(values[0]) && meta.as_of === values[0]
        && isPlain(data) && data.decision_time === values[0];
    }
    if (/^\/(?:api|internal)\/v1\/instruments\/[^/]+\/research$/.test(url.pathname)) {
      const values = url.searchParams.getAll("as_of");
      return values.length === 1 && isUtcIso(values[0]) && meta.as_of === values[0]
        && isPlain(data) && typeof data.decision_time === "string"
        && Date.parse(data.decision_time) <= Date.parse(values[0]);
    }
    return true;
  } catch { return false; }
}

function researchAdmissionValid(data: Record<string, unknown>): boolean {
  if (typeof data.eligible !== "boolean") return false;
  const hasSnapshot = typeof data.data_snapshot_id === "string" && validateUuid(data.data_snapshot_id);
  if (data.eligible) return data.reason === null && hasSnapshot;
  if (data.reason === null) return false;
  if (["instrument_not_found", "decision_time_in_future", "data_snapshot_unavailable"].includes(String(data.reason))) {
    return data.data_snapshot_id === null;
  }
  return hasSnapshot;
}

function researchAuthorityValid(data: Record<string, unknown>): boolean {
  if (data.intent === null) return true;
  if (!isPlain(data.intent)) return false;
  return sameUuid(data.intent.instrument_id, data.instrument_id)
    && sameUuid(data.intent.data_snapshot_id, data.data_snapshot_id)
    && typeof data.intent.decision_time === "string"
    && typeof data.decision_time === "string"
    && Date.parse(data.intent.decision_time) === Date.parse(data.decision_time);
}

function strategyEvidenceValid(data: Record<string, unknown>): boolean {
  if (data.latest_backtest === null) return true;
  if (!isPlain(data.latest_backtest)) return false;
  const evidence = data.latest_backtest;
  const hashes = [evidence.result_hash, evidence.data_hash, evidence.code_hash];
  if (!hashes.every((value) => typeof value === "string" && /^[0-9a-f]{64}$/.test(value))) return false;
  if (!isIsoDate(evidence.start_session) || !isIsoDate(evidence.end_session)
    || String(evidence.start_session) > String(evidence.end_session)) return false;
  if (!Number.isInteger(evidence.oos_window_count) || Number(evidence.oos_window_count) < 0) return false;
  if (!isUtcIso(evidence.completed_at) || !Array.isArray(evidence.comparison_keys)) return false;
  const keys = evidence.comparison_keys;
  if (keys.length !== new Set(keys).size
    || !keys.every((key) => ["spy", "equal_weight", "champion"].includes(String(key)))) return false;
  const complete = keys.length === 3
    && ["spy", "equal_weight", "champion"].every((key) => keys.includes(key));
  if (typeof evidence.benchmark_comparison_complete !== "boolean"
    || evidence.benchmark_comparison_complete !== complete
    || !Array.isArray(evidence.data_sources)
    || !evidence.data_sources.every((source) => typeof source === "string" && source.length > 0)
    || evidence.data_sources.length === 0
    || evidence.data_sources.length !== new Set(evidence.data_sources).size) return false;
  const sources = evidence.data_sources as string[];
  const domains = ["price", "universe", "sector", "corporate_action", "calendar"];
  const byDomain = Object.fromEntries(domains.map((domain) => [
    domain,
    sources.filter((source) => source.startsWith(`${domain}:`)),
  ])) as Record<string, string[]>;
  if (domains.some((domain) => byDomain[domain].length === 0)) return false;
  const marketSources = ["price", "universe", "sector", "corporate_action"]
    .flatMap((domain) => byDomain[domain]);
  const fixtureFlags = marketSources.map((source) => source.split(":").includes("fixture"));
  const priceModes = new Set(byDomain.price.map((source) => source.split(":").at(-1)));
  if (evidence.data_mode === "fixture") return fixtureFlags.every(Boolean);
  if (evidence.data_mode === "delayed" || evidence.data_mode === "real_time") {
    return !fixtureFlags.some(Boolean) && priceModes.size === 1 && priceModes.has(evidence.data_mode);
  }
  return evidence.data_mode === "mixed"
    && ((fixtureFlags.some(Boolean) && !fixtureFlags.every(Boolean)) || priceModes.size > 1);
}


function isV7(value: unknown): boolean { return typeof value === "string" && validateUuid(value) && uuidVersion(value) === 7; }
function isUtcIso(value: unknown): boolean { if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/.test(value)) return false; const date = new Date(value); return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 19) === value.slice(0, 19); }
function isIsoDate(value: unknown): boolean { if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false; const date = new Date(`${value}T00:00:00Z`); return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 10) === value; }
function isStableCode(value: unknown): value is StableEngineErrorCode { return typeof value === "string" && Object.hasOwn(ERROR_STATUSES, value); }
function allowsStatus(code: unknown, status: number): boolean { return isStableCode(code) && ERROR_STATUSES[code].includes(status as never); }
function safeHeaders(upstream: Headers, requestUrl?: string): Headers { const headers = new Headers(); for (const name of ["retry-after", "etag"]) { const value = upstream.get(name); if (value) headers.set(name, value); } const location = upstream.get("location"); if (location && requestUrl) try { const target = new URL(location, requestUrl); const origin = new URL(requestUrl).origin; if (target.origin === origin && !target.username && !target.password) headers.set("location", `${target.pathname}${target.search}${target.hash}`); } catch { return headers; } return headers; }
function isObject(value: unknown, keys: string[]): value is Record<string, unknown> { return isPlain(value) && Object.keys(value).length === keys.length && keys.every((key) => key in value); }
function isPlain(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
