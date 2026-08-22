import "server-only";

import { v7 as uuidv7 } from "uuid";
import type { NexusActor } from "../auth/nexus-user";
import { errorResponse, rebuildAllowedEngineResponse } from "./errors";
import { createEngineHmacHeaders } from "./hmac";

const SYSTEM_STATUS_PATH = "/internal/v1/system-status";
const MAX_ENGINE_RESPONSE_BYTES = 64 * 1024;
type TemporaryInternalPaths = { "/internal/v1/system-status": unknown };
export type EngineRequestInit<Path extends keyof TemporaryInternalPaths> = Omit<RequestInit, "body"> & { actor: NexusActor; body?: string | Uint8Array | ArrayBuffer; method?: Path extends "/internal/v1/system-status" ? "GET" : never };
export type EngineResponse<Path extends keyof TemporaryInternalPaths> = Response & { readonly nexusPath?: Path };

export async function engineFetch<Path extends keyof TemporaryInternalPaths>(path: Path, init: EngineRequestInit<Path>): Promise<EngineResponse<Path>> {
  const config = engineConfig();
  const target = allowedTarget(path, config.baseUrl);
  if (!target) return internalFailure<Path>();
  const body = exactBodyBytes(init.body);
  const headers = new Headers(init.headers);
  const protectedHeaders = createEngineHmacHeaders({ keyId: config.keyId, secret: config.secret, method: init.method ?? "GET", pathAndQuery: SYSTEM_STATUS_PATH, timestamp: Math.floor(Date.now() / 1000), nonce: uuidv7(), actorId: init.actor.userId, requestId: init.actor.requestId || uuidv7(), body });
  for (const name of Object.keys(protectedHeaders)) headers.delete(name);
  for (const [name, value] of Object.entries(protectedHeaders)) headers.set(name, value);
  try {
    const response = await fetch(target, { ...init, method: init.method ?? "GET", headers, body: body.byteLength ? body as unknown as BodyInit : undefined, signal: AbortSignal.any([init.signal, AbortSignal.timeout(10_000)].filter(Boolean) as AbortSignal[]) });
    return await sanitizeEngineResponse<Path>(response);
  } catch { return internalFailure<Path>(); }
}

export async function sanitizeEngineResponse<Path extends keyof TemporaryInternalPaths>(response: Response): Promise<EngineResponse<Path>> {
  if (!response.headers.get("content-type")?.toLowerCase().startsWith("application/json")) return internalFailure<Path>();
  const bytes = await boundedResponseBytes(response, MAX_ENGINE_RESPONSE_BYTES);
  if (!bytes) return internalFailure<Path>();
  try { return (rebuildAllowedEngineResponse(response.status, JSON.parse(new TextDecoder().decode(bytes)), response.headers) ?? internalFailure<Path>()) as EngineResponse<Path>; } catch { return internalFailure<Path>(); }
}

async function boundedResponseBytes(response: Response, limit: number): Promise<Uint8Array | null> {
  if (!response.body) return null;
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let total = 0;
  try { while (true) { const { done, value } = await reader.read(); if (done) break; total += value.byteLength; if (total > limit) { void reader.cancel().catch(() => undefined); return null; } chunks.push(value); } } catch { return null; }
  const all = new Uint8Array(total); let offset = 0; for (const chunk of chunks) { all.set(chunk, offset); offset += chunk.byteLength; } return all;
}

function internalFailure<Path extends keyof TemporaryInternalPaths>(): EngineResponse<Path> { return errorResponse(500, "INTERNAL_ERROR", "The request could not be completed.") as EngineResponse<Path>; }
function allowedTarget(path: string, baseUrl: string): URL | null { if (path !== SYSTEM_STATUS_PATH) return null; try { const base = new URL(baseUrl); if (base.username || base.password || (process.env.NEXUS_ENV === "production" && base.protocol !== "https:")) return null; const target = new URL(SYSTEM_STATUS_PATH, base); return target.origin === base.origin && target.pathname === SYSTEM_STATUS_PATH && !target.search ? target : null; } catch { return null; } }
function engineConfig(): { baseUrl: string; keyId: string; secret: string } { const baseUrl = process.env.NEXUS_ENGINE_BASE_URL; const keyId = process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID ?? "current"; const secret = process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET ?? process.env.NEXUS_ENGINE_HMAC_SECRET; if (!baseUrl || !secret) throw new Error("NEXUS engine service configuration is incomplete"); return { baseUrl, keyId, secret }; }
function exactBodyBytes(body: EngineRequestInit<keyof TemporaryInternalPaths>["body"]): Uint8Array { if (body === undefined) return new Uint8Array(); if (typeof body === "string") return new TextEncoder().encode(body); if (body instanceof Uint8Array) return body; if (body instanceof ArrayBuffer) return new Uint8Array(body); throw new TypeError("engine request bodies must have exact byte representation"); }
