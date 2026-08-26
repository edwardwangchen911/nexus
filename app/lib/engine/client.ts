import "server-only";

import { v7 as uuidv7, validate as validateUuid } from "uuid";
import type { paths } from "../generated/schema";
import { integrationProviders } from "../integrations/providers";
import type { NexusActor } from "../auth/nexus-user";
import { errorResponse, isAllowedAuditExportEvent, rebuildAllowedEngineResponse } from "./errors";
import { createEngineHmacHeaders } from "./hmac";

const MAX_ENGINE_RESPONSE_BYTES = 64 * 1024;
const MAX_MARKET_RESPONSE_BYTES = 256 * 1024;
const MAX_PORTFOLIO_RESPONSE_BYTES = 4 * 1024 * 1024;
const MAX_ANALYSIS_RESPONSE_BYTES = 1024 * 1024;
const MAX_PAPER_RESPONSE_BYTES = 1024 * 1024;
const MAX_RISK_AUDIT_RESPONSE_BYTES = 1024 * 1024;
const MAX_AUDIT_EXPORT_BYTES = 32 * 1024 * 1024;
const MAX_AUDIT_EXPORT_ROWS = 100_000;
type InternalPath = Extract<keyof paths, `/internal/v1/${string}` | "/health/ready">;
type DynamicInternalPath = Extract<InternalPath, `${string}{${string}}${string}`>;
type QueryInternalPath = Extract<InternalPath, "/internal/v1/market" | "/internal/v1/overview" | "/internal/v1/paper/orders" | "/internal/v1/portfolios" | "/internal/v1/instruments/{instrument_id}/research" | "/internal/v1/instruments/{instrument_id}/research-admission" | "/internal/v1/risk-audit" | "/internal/v1/risk-audit/export">;
type EngineBody = string | Uint8Array | ArrayBuffer;
type MethodFor<Path extends InternalPath> =
  | (Exclude<paths[Path]["get"], undefined> extends never ? never : "GET")
  | (Exclude<paths[Path]["post"], undefined> extends never ? never : "POST")
  | (Exclude<paths[Path]["put"], undefined> extends never ? never : "PUT")
  | (Exclude<paths[Path]["delete"], undefined> extends never ? never : "DELETE");
export type EngineTarget<Path extends InternalPath> = Path extends DynamicInternalPath
  ? Path extends QueryInternalPath
    ? { template: Path; path: string; query?: string }
    : { template: Path; path: string }
  : Path extends QueryInternalPath
    ? Path | { template: Path; path: Path; query?: string }
    : Path;
export type EngineRequestInit<Path extends InternalPath> = Omit<RequestInit, "body" | "method"> & { actor: NexusActor; body?: EngineBody; method?: MethodFor<Path> };
export type EngineResponse<Path extends InternalPath> = Response & { readonly nexusPath?: Path };

export async function engineFetch<Path extends InternalPath>(target: EngineTarget<Path>, init: EngineRequestInit<Path>): Promise<EngineResponse<Path>> {
  const config = engineConfig();
  const resolved = allowedTarget(target, config.baseUrl);
  if (!resolved) return internalFailure<Path>();
  const method = (init.method ?? "GET").toUpperCase();
  const body = exactBodyBytes(init.body);
  const headers = allowedCallerHeaders(init.headers);
  const protectedHeaders = createEngineHmacHeaders({ keyId: config.keyId, secret: config.secret, method, pathAndQuery: resolved.pathAndQuery, timestamp: Math.floor(Date.now() / 1000), nonce: uuidv7(), actorId: init.actor.userId, requestId: init.actor.requestId || uuidv7(), body });
  for (const name of Object.keys(protectedHeaders)) headers.delete(name);
  for (const [name, value] of Object.entries(protectedHeaders)) headers.set(name, value);
  try {
    const response = await fetch(resolved.url, { ...init, method, headers, body: body.byteLength ? body as unknown as BodyInit : undefined, redirect: "manual", signal: AbortSignal.any([init.signal, AbortSignal.timeout(timeoutForMethod(method))].filter(Boolean) as AbortSignal[]) });
    return resolved.url.pathname === "/internal/v1/risk-audit/export"
      ? await sanitizeAuditExportResponse<Path>(response, resolved.url.toString(), resolved.url.pathname, method)
      : await sanitizeEngineResponse<Path>(response, resolved.url.toString(), resolved.url.pathname, method);
  } catch { return internalFailure<Path>(); }
}

export async function sanitizeEngineResponse<Path extends InternalPath>(
  response: Response,
  requestUrl?: string,
  schemaPath?: string,
  requestMethod?: string,
): Promise<EngineResponse<Path>> {
  if (!response.headers.get("content-type")?.toLowerCase().startsWith("application/json")) return internalFailure<Path>();
  const bytes = await boundedResponseBytes(response, responseLimit(requestUrl, schemaPath));
  if (!bytes) return internalFailure<Path>();
  try { return (rebuildAllowedEngineResponse(response.status, JSON.parse(new TextDecoder().decode(bytes)), response.headers, requestUrl, schemaPath, requestMethod) ?? internalFailure<Path>()) as EngineResponse<Path>; } catch { return internalFailure<Path>(); }
}

export async function sanitizeAuditExportResponse<Path extends InternalPath>(
  response: Response,
  requestUrl?: string,
  schemaPath = "/internal/v1/risk-audit/export",
  requestMethod = "GET",
): Promise<EngineResponse<Path>> {
  if (response.status !== 200) {
    return sanitizeEngineResponse<Path>(response, requestUrl, schemaPath, requestMethod);
  }
  const mediaType = response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
  const encoding = response.headers.get("content-encoding")?.trim().toLowerCase();
  const advertisedLength = response.headers.get("content-length");
  if (mediaType !== "application/x-ndjson" || encoding !== "gzip"
    || advertisedLength !== null && (!/^\d+$/.test(advertisedLength) || Number(advertisedLength) > MAX_AUDIT_EXPORT_BYTES)) {
    return internalFailure<Path>();
  }
  const bytes = await boundedResponseBytes(response, MAX_AUDIT_EXPORT_BYTES);
  if (!bytes) return internalFailure<Path>();
  const safeBytes = await normalizedAuditExportBytes(bytes);
  if (!safeBytes) return internalFailure<Path>();
  return new Response(safeBytes as unknown as BodyInit, {
    status: 200,
    headers: {
      "cache-control": "no-store",
      "content-disposition": 'attachment; filename="nexus-risk-audit.ndjson.gz"',
      "content-encoding": "gzip",
      "content-length": String(safeBytes.byteLength),
      "content-type": "application/x-ndjson",
    },
  }) as EngineResponse<Path>;
}

async function normalizedAuditExportBytes(bytes: Uint8Array): Promise<Uint8Array | null> {
  const isGzip = bytes.byteLength >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b;
  try {
    const decoded = isGzip
      ? await transformedResponseBytes(bytes, new DecompressionStream("gzip"))
      : bytes;
    if (!decoded || !validAuditNdjson(decoded)) return null;
    if (isGzip) return bytes;
    return transformedResponseBytes(decoded, new CompressionStream("gzip"));
  } catch {
    return null;
  }
}

async function transformedResponseBytes(
  bytes: Uint8Array,
  transform: GenericTransformStream,
): Promise<Uint8Array | null> {
  const body = new Response(bytes as unknown as BodyInit).body;
  if (!body) return null;
  return boundedResponseBytes(new Response(body.pipeThrough(transform)), MAX_AUDIT_EXPORT_BYTES);
}

function validAuditNdjson(bytes: Uint8Array): boolean {
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return false;
  }
  if (text === "") return true;
  if (!text.endsWith("\n")) return false;
  const lines = text.slice(0, -1).split("\n");
  if (lines.length > MAX_AUDIT_EXPORT_ROWS || lines.some((line) => line.length === 0)) return false;
  return lines.every((line) => {
    try {
      return isAllowedAuditExportEvent(JSON.parse(line));
    } catch {
      return false;
    }
  });
}

function responseLimit(requestUrl?: string, schemaPath?: string): number {
  if (schemaPath === "/internal/v1/market") return MAX_MARKET_RESPONSE_BYTES;
  if (schemaPath === "/internal/v1/portfolios" || schemaPath?.includes("/snapshots/latest")) {
    return MAX_PORTFOLIO_RESPONSE_BYTES;
  }
  if (schemaPath && isAnalysisPath(schemaPath)) return MAX_ANALYSIS_RESPONSE_BYTES;
  if (schemaPath?.startsWith("/internal/v1/paper/")) return MAX_PAPER_RESPONSE_BYTES;
  if (schemaPath === "/internal/v1/risk-audit") return MAX_RISK_AUDIT_RESPONSE_BYTES;
  if (requestUrl) {
    try {
      const pathname = new URL(requestUrl).pathname;
      if (pathname === "/api/v1/market" || pathname === "/internal/v1/market") {
        return MAX_MARKET_RESPONSE_BYTES;
      }
      if (pathname === "/api/v1/portfolios" || pathname === "/internal/v1/portfolios" || pathname.includes("/snapshots/latest")) {
        return MAX_PORTFOLIO_RESPONSE_BYTES;
      }
      if (isAnalysisPath(pathname)) return MAX_ANALYSIS_RESPONSE_BYTES;
      if (pathname.startsWith("/api/v1/paper/") || pathname.startsWith("/internal/v1/paper/")) return MAX_PAPER_RESPONSE_BYTES;
      if (pathname === "/api/v1/risk-audit" || pathname === "/internal/v1/risk-audit") return MAX_RISK_AUDIT_RESPONSE_BYTES;
    } catch {
      return MAX_ENGINE_RESPONSE_BYTES;
    }
  }
  return MAX_ENGINE_RESPONSE_BYTES;
}

async function boundedResponseBytes(response: Response, limit: number): Promise<Uint8Array | null> {
  if (!response.body) return null;
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let total = 0;
  try { while (true) { const { done, value } = await reader.read(); if (done) break; total += value.byteLength; if (total > limit) { void reader.cancel().catch(() => undefined); return null; } chunks.push(value); } } catch { return null; }
  const all = new Uint8Array(total); let offset = 0; for (const chunk of chunks) { all.set(chunk, offset); offset += chunk.byteLength; } return all;
}

function internalFailure<Path extends InternalPath>(): EngineResponse<Path> { return errorResponse(500, "INTERNAL_ERROR", "The request could not be completed.") as EngineResponse<Path>; }
export function timeoutForMethod(method: string): number { return ["GET", "HEAD"].includes(method.toUpperCase()) ? 10_000 : 30_000; }
function allowedTarget<Path extends InternalPath>(target: EngineTarget<Path>, baseUrl: string): { url: URL; pathAndQuery: string } | null {
  const dynamic = typeof target !== "string"
    ? target as { template: InternalPath; path: string; query?: string }
    : null;
  const template = dynamic?.template ?? target as InternalPath;
  const path = dynamic?.path ?? target as string;
  if (!matchesTemplate(template, path)) return null;

  const query = dynamic?.query;
  if (query !== undefined && template !== "/internal/v1/market" && template !== "/internal/v1/overview" && template !== "/internal/v1/paper/orders" && template !== "/internal/v1/portfolios" && template !== "/internal/v1/instruments/{instrument_id}/research" && template !== "/internal/v1/instruments/{instrument_id}/research-admission" && template !== "/internal/v1/risk-audit" && template !== "/internal/v1/risk-audit/export") return null;
  const canonicalQuery = template === "/internal/v1/market"
    ? canonicalMarketQuery(query ?? "")
    : template === "/internal/v1/overview"
      ? canonicalOverviewQuery(query ?? "")
    : template === "/internal/v1/paper/orders"
      ? canonicalPaperOrderQuery(query ?? "")
    : template === "/internal/v1/portfolios"
      ? canonicalPortfolioQuery(query ?? "")
      : template === "/internal/v1/instruments/{instrument_id}/research" || template === "/internal/v1/instruments/{instrument_id}/research-admission"
        ? canonicalResearchQuery(query ?? "")
      : template === "/internal/v1/risk-audit"
        ? canonicalRiskAuditQuery(query ?? "", false)
      : template === "/internal/v1/risk-audit/export"
        ? canonicalRiskAuditQuery(query ?? "", true)
      : "";
  if (canonicalQuery === null) return null;

  try {
    const base = new URL(baseUrl);
    if (base.username || base.password || (process.env.NEXUS_ENV === "production" && base.protocol !== "https:")) return null;
    const pathAndQuery = canonicalQuery ? `${path}?${canonicalQuery}` : path;
    const url = new URL(pathAndQuery, base);
    if (url.origin !== base.origin || url.pathname !== path || url.hash || url.username || url.password) return null;
    if (url.search !== (canonicalQuery ? `?${canonicalQuery}` : "")) return null;
    return { url, pathAndQuery };
  } catch { return null; }
}

function matchesTemplate(template: InternalPath, path: string): boolean {
  if (["/health/ready", "/internal/v1/system-status", "/internal/v1/integrations", "/internal/v1/market", "/internal/v1/overview", "/internal/v1/paper/orders", "/internal/v1/paper/portfolio", "/internal/v1/portfolios", "/internal/v1/broker-sync-runs", "/internal/v1/optimization-proposals", "/internal/v1/what-if/evaluate", "/internal/v1/research-runs", "/internal/v1/strategies", "/internal/v1/risk-audit", "/internal/v1/risk-audit/export", "/internal/v1/risk/drawdown-lock/acknowledge"].includes(template)) {
    return path === template;
  }
  const integration = /^\/internal\/v1\/integrations\/([^/]+)\/(test|mode)$/.exec(path);
  if (integration && integrationProviders.includes(integration[1] as (typeof integrationProviders)[number])) {
    return template === `/internal/v1/integrations/{provider}/${integration[2]}`;
  }
  const watchlist = /^\/internal\/v1\/watchlist\/([^/]+)$/.exec(path);
  if (
    watchlist
      && validateUuid(watchlist[1])
      && template === "/internal/v1/watchlist/{instrument_id}"
  ) return true;
  const paperCancel = /^\/internal\/v1\/paper\/orders\/([^/]+)\/cancel$/.exec(path);
  if (paperCancel && validateUuid(paperCancel[1])) {
    return template === "/internal/v1/paper/orders/{order_id}/cancel";
  }
  const jobRetry = /^\/internal\/v1\/jobs\/([^/]+)\/retry$/.exec(path);
  if (jobRetry && validateUuid(jobRetry[1])) {
    return template === "/internal/v1/jobs/{job_id}/retry";
  }
  const snapshot = /^\/internal\/v1\/portfolios\/([^/]+)\/snapshots\/latest$/.exec(path);
  if (snapshot && validateUuid(snapshot[1])) {
    return template === "/internal/v1/portfolios/{portfolio_id}/snapshots/latest";
  }
  const diagnostics = /^\/internal\/v1\/portfolios\/([^/]+)\/diagnostics\/latest$/.exec(path);
  if (diagnostics && validateUuid(diagnostics[1])) {
    return template === "/internal/v1/portfolios/{portfolio_id}/diagnostics/latest";
  }
  const proposal = /^\/internal\/v1\/optimization-proposals\/([^/]+)(?:\/(shadow|reject))?$/.exec(path);
  if (proposal && validateUuid(proposal[1])) {
    const expected = proposal[2]
      ? `/internal/v1/optimization-proposals/{group_id}/${proposal[2]}`
      : "/internal/v1/optimization-proposals/{group_id}";
    return template === expected;
  }
  const researchRun = /^\/internal\/v1\/research-runs\/([^/]+)$/.exec(path);
  if (researchRun && validateUuid(researchRun[1])) {
    return template === "/internal/v1/research-runs/{run_id}";
  }
  const instrumentResearch = /^\/internal\/v1\/instruments\/([^/]+)\/research$/.exec(path);
  if (instrumentResearch && validateUuid(instrumentResearch[1])) {
    return template === "/internal/v1/instruments/{instrument_id}/research";
  }
  const researchAdmission = /^\/internal\/v1\/instruments\/([^/]+)\/research-admission$/.exec(path);
  if (researchAdmission && validateUuid(researchAdmission[1])) {
    return template === "/internal/v1/instruments/{instrument_id}/research-admission";
  }
  const brief = /^\/internal\/v1\/briefs\/(\d{4}-\d{2}-\d{2})$/.exec(path);
  if (brief && isIsoDate(brief[1])) {
    return template === "/internal/v1/briefs/{session_date}";
  }
  const syncRun = /^\/internal\/v1\/broker-sync-runs\/([^/]+)$/.exec(path);
  return Boolean(syncRun && validateUuid(syncRun[1]) && template === "/internal/v1/broker-sync-runs/{run_id}");
}

function canonicalOverviewQuery(query: string): string | null {
  if (query.includes("?") || query.includes("#") || !hasValidPercentEscapes(query)) return null;
  const allowedKeys = new Set(["portfolio_id", "session_date"]);
  const seen = new Set<string>();
  const pairs: Array<[string, string]> = [];
  for (const [key, value] of new URLSearchParams(query)) {
    if (!allowedKeys.has(key) || seen.has(key)) return null;
    if (key === "portfolio_id" && !validateUuid(value)) return null;
    if (key === "session_date" && !isIsoDate(value)) return null;
    seen.add(key);
    pairs.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  pairs.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return pairs.map(([key, value]) => `${key}=${value}`).join("&");
}

function canonicalMarketQuery(query: string): string | null {
  if (query.includes("?") || query.includes("#") || !hasValidPercentEscapes(query)) return null;
  const allowedKeys = new Set(["cursor", "limit", "selected_instrument_id", "watchlist"]);
  const seen = new Set<string>();
  const pairs: Array<[string, string]> = [];
  for (const [key, value] of new URLSearchParams(query)) {
    if (!allowedKeys.has(key) || seen.has(key) || !isValidMarketQueryValue(key, value)) return null;
    seen.add(key);
    pairs.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  pairs.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return pairs.map(([key, value]) => `${key}=${value}`).join("&");
}

function canonicalPortfolioQuery(query: string): string | null {
  if (query.includes("?") || query.includes("#") || !hasValidPercentEscapes(query)) return null;
  const allowedKeys = new Set(["cursor", "kind", "limit"]);
  const kinds = new Set(["real_read_only", "internal_paper", "shadow"]);
  const seen = new Set<string>();
  const pairs: Array<[string, string]> = [];
  for (const [key, value] of new URLSearchParams(query)) {
    if (!allowedKeys.has(key) || seen.has(key)) return null;
    if (key === "limit" && (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 50)) return null;
    if (key === "kind" && !kinds.has(value)) return null;
    if (key === "cursor" && (value.length < 1 || value.length > 2048)) return null;
    seen.add(key);
    pairs.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  pairs.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return pairs.map(([key, value]) => `${key}=${value}`).join("&");
}

function canonicalPaperOrderQuery(query: string): string | null {
  if (query.includes("?") || query.includes("#") || !hasValidPercentEscapes(query)) return null;
  const allowedKeys = new Set(["cursor", "limit", "status"]);
  const statuses = new Set(["proposed", "risk_rejected", "risk_approved", "awaiting_approval", "submitted", "partially_filled", "filled", "cancelled", "rejected", "reconciling"]);
  const seen = new Set<string>();
  const pairs: Array<[string, string]> = [];
  for (const [key, value] of new URLSearchParams(query)) {
    if (!allowedKeys.has(key) || seen.has(key)) return null;
    if (key === "limit" && (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 50)) return null;
    if (key === "status" && !statuses.has(value)) return null;
    if (key === "cursor" && (value.length < 1 || value.length > 2048)) return null;
    seen.add(key);
    pairs.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  pairs.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return pairs.map(([key, value]) => `${key}=${value}`).join("&");
}

function canonicalResearchQuery(query: string): string | null {
  if (query.includes("?") || query.includes("#") || !hasValidPercentEscapes(query)) return null;
  const entries = [...new URLSearchParams(query).entries()];
  if (entries.length !== 1 || entries[0][0] !== "as_of" || !isUtcIso(entries[0][1])) return null;
  return `as_of=${strictPercentEncode(entries[0][1])}`;
}

function canonicalRiskAuditQuery(query: string, exportRequest: boolean): string | null {
  if (query.includes("?") || query.includes("#") || !hasValidPercentEscapes(query)) return null;
  const allowedKeys = new Set(exportRequest ? ["from", "kind", "to"] : ["cursor", "from", "kind", "limit", "to"]);
  const kinds = new Set(["quality", "risk", "agent_cost", "job", "reconciliation", "audit"]);
  const seen = new Set<string>();
  const values = new Map<string, string>();
  const pairs: Array<[string, string]> = [];
  for (const [key, value] of new URLSearchParams(query)) {
    if (!allowedKeys.has(key) || seen.has(key)) return null;
    if (key === "limit" && (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 100)) return null;
    if (key === "kind" && !kinds.has(value)) return null;
    if (key === "cursor" && (value.length < 1 || value.length > 2048)) return null;
    if ((key === "from" || key === "to") && !isUtcIso(value)) return null;
    seen.add(key);
    values.set(key, value);
    pairs.push([strictPercentEncode(key), strictPercentEncode(value)]);
  }
  if (exportRequest) {
    const from = values.get("from");
    const to = values.get("to");
    if (!from || !to) return null;
    const duration = Date.parse(to) - Date.parse(from);
    if (duration <= 0 || duration > 31 * 24 * 60 * 60 * 1000) return null;
  }
  pairs.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return pairs.map(([key, value]) => `${key}=${value}`).join("&");
}

function isValidMarketQueryValue(key: string, value: string): boolean {
  if (key === "limit") return /^\d+$/.test(value) && Number(value) >= 1 && Number(value) <= 100;
  if (key === "watchlist") return value === "true" || value === "false";
  if (key === "selected_instrument_id") return validateUuid(value);
  if (key === "cursor") return value.length >= 1 && value.length <= 2048;
  return false;
}

function hasValidPercentEscapes(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    if (value[index] === "%") {
      if (!/^[0-9a-fA-F]{2}$/.test(value.slice(index + 1, index + 3))) return false;
      index += 2;
    }
  }
  return true;
}

function strictPercentEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (character) =>
    `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}
function isIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 10) === value;
}
function isUtcIso(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value)) return false;
  const date = new Date(value);
  return !Number.isNaN(date.valueOf()) && date.toISOString().slice(0, 19) === value.slice(0, 19);
}
function isAnalysisPath(pathname: string): boolean {
  return pathname === "/api/v1/overview" || pathname === "/internal/v1/overview"
    || pathname.includes("/diagnostics/latest")
    || pathname.includes("/optimization-proposals")
    || pathname.endsWith("/what-if/evaluate")
    || pathname.includes("/research-runs")
    || pathname.includes("/instruments/") && (pathname.endsWith("/research") || pathname.endsWith("/research-admission"))
    || pathname.includes("/briefs/");
}
function engineConfig(): { baseUrl: string; keyId: string; secret: string } { const baseUrl = process.env.NEXUS_ENGINE_BASE_URL; const keyId = process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID ?? "current"; const secret = process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET ?? process.env.NEXUS_ENGINE_HMAC_SECRET; if (!baseUrl || !secret) throw new Error("NEXUS engine service configuration is incomplete"); return { baseUrl, keyId, secret }; }
function exactBodyBytes(body: EngineBody | undefined): Uint8Array { if (body === undefined) return new Uint8Array(); if (typeof body === "string") return new TextEncoder().encode(body); if (body instanceof Uint8Array) return body; if (body instanceof ArrayBuffer) return new Uint8Array(body); throw new TypeError("engine request bodies must have exact byte representation"); }
function allowedCallerHeaders(input?: HeadersInit): Headers {
  const source = new Headers(input);
  const headers = new Headers();
  for (const name of ["accept", "content-type", "idempotency-key", "if-match"]) {
    const value = source.get(name);
    if (value !== null) headers.set(name, value);
  }
  return headers;
}
