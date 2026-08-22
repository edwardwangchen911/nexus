import "server-only";

import { v7 as uuidv7, validate as validateUuid } from "uuid";
import type { paths } from "../generated/schema";
import { integrationProviders } from "../integrations/providers";
import type { NexusActor } from "../auth/nexus-user";
import { errorResponse, rebuildAllowedEngineResponse } from "./errors";
import { createEngineHmacHeaders } from "./hmac";

const MAX_ENGINE_RESPONSE_BYTES = 64 * 1024;
type InternalPath = Extract<keyof paths, `/internal/v1/${string}`>;
type DynamicInternalPath = Extract<InternalPath, `${string}{${string}}${string}`>;
type EngineBody = string | Uint8Array | ArrayBuffer;
type MethodFor<Path extends InternalPath> =
  | (Exclude<paths[Path]["get"], undefined> extends never ? never : "GET")
  | (Exclude<paths[Path]["post"], undefined> extends never ? never : "POST")
  | (Exclude<paths[Path]["put"], undefined> extends never ? never : "PUT")
  | (Exclude<paths[Path]["delete"], undefined> extends never ? never : "DELETE");
export type EngineTarget<Path extends InternalPath> = Path extends "/internal/v1/market"
  ? Path | { template: Path; path: Path; query?: string }
  : Path extends DynamicInternalPath
    ? { template: Path; path: string }
    : Path;
export type EngineRequestInit<Path extends InternalPath> = Omit<RequestInit, "body" | "method"> & { actor: NexusActor; body?: EngineBody; method?: MethodFor<Path> };
export type EngineResponse<Path extends InternalPath> = Response & { readonly nexusPath?: Path };

export async function engineFetch<Path extends InternalPath>(target: EngineTarget<Path>, init: EngineRequestInit<Path>): Promise<EngineResponse<Path>> {
  const config = engineConfig();
  const resolved = allowedTarget(target, config.baseUrl);
  if (!resolved) return internalFailure<Path>();
  const method = (init.method ?? "GET").toUpperCase();
  const body = exactBodyBytes(init.body);
  const headers = new Headers(init.headers);
  const protectedHeaders = createEngineHmacHeaders({ keyId: config.keyId, secret: config.secret, method, pathAndQuery: resolved.pathAndQuery, timestamp: Math.floor(Date.now() / 1000), nonce: uuidv7(), actorId: init.actor.userId, requestId: init.actor.requestId || uuidv7(), body });
  for (const name of Object.keys(protectedHeaders)) headers.delete(name);
  for (const [name, value] of Object.entries(protectedHeaders)) headers.set(name, value);
  try {
    const response = await fetch(resolved.url, { ...init, method, headers, body: body.byteLength ? body as unknown as BodyInit : undefined, signal: AbortSignal.any([init.signal, AbortSignal.timeout(timeoutForMethod(method))].filter(Boolean) as AbortSignal[]) });
    return await sanitizeEngineResponse<Path>(response, undefined, resolved.url.pathname);
  } catch { return internalFailure<Path>(); }
}

export async function sanitizeEngineResponse<Path extends InternalPath>(response: Response, requestUrl?: string, schemaPath?: string): Promise<EngineResponse<Path>> {
  if (!response.headers.get("content-type")?.toLowerCase().startsWith("application/json")) return internalFailure<Path>();
  const bytes = await boundedResponseBytes(response, MAX_ENGINE_RESPONSE_BYTES);
  if (!bytes) return internalFailure<Path>();
  try { return (rebuildAllowedEngineResponse(response.status, JSON.parse(new TextDecoder().decode(bytes)), response.headers, requestUrl, schemaPath) ?? internalFailure<Path>()) as EngineResponse<Path>; } catch { return internalFailure<Path>(); }
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
  if (query !== undefined && template !== "/internal/v1/market") return null;
  const canonicalQuery = template === "/internal/v1/market"
    ? canonicalMarketQuery(query ?? "")
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
  if (template === "/internal/v1/system-status" || template === "/internal/v1/integrations" || template === "/internal/v1/market") {
    return path === template;
  }
  const integration = /^\/internal\/v1\/integrations\/([^/]+)\/(test|mode)$/.exec(path);
  if (integration && integrationProviders.includes(integration[1] as (typeof integrationProviders)[number])) {
    return template === `/internal/v1/integrations/{provider}/${integration[2]}`;
  }
  const watchlist = /^\/internal\/v1\/watchlist\/([^/]+)$/.exec(path);
  return Boolean(
    watchlist
      && validateUuid(watchlist[1])
      && template === "/internal/v1/watchlist/{instrument_id}",
  );
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
function engineConfig(): { baseUrl: string; keyId: string; secret: string } { const baseUrl = process.env.NEXUS_ENGINE_BASE_URL; const keyId = process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID ?? "current"; const secret = process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET ?? process.env.NEXUS_ENGINE_HMAC_SECRET; if (!baseUrl || !secret) throw new Error("NEXUS engine service configuration is incomplete"); return { baseUrl, keyId, secret }; }
function exactBodyBytes(body: EngineBody | undefined): Uint8Array { if (body === undefined) return new Uint8Array(); if (typeof body === "string") return new TextEncoder().encode(body); if (body instanceof Uint8Array) return body; if (body instanceof ArrayBuffer) return new Uint8Array(body); throw new TypeError("engine request bodies must have exact byte representation"); }
