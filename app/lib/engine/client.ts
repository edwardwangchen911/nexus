import "server-only";

import { v7 as uuidv7 } from "uuid";
import type { NexusActor } from "../auth/nexus-user";
import { createEngineHmacHeaders } from "./hmac";

type TemporaryInternalPaths = {
  "/internal/v1/system-status": { get: { responses: { 200: { content: { "application/json": unknown } } } } };
};

export type EngineRequestInit<Path extends keyof TemporaryInternalPaths> = Omit<RequestInit, "body"> & {
  actor: NexusActor;
  body?: string | Uint8Array | ArrayBuffer;
  method?: Path extends "/internal/v1/system-status" ? "GET" : never;
};

export type EngineResponse<Path extends keyof TemporaryInternalPaths> = Response & {
  readonly nexusPath?: Path;
};

export async function engineFetch<Path extends keyof TemporaryInternalPaths>(
  path: Path,
  init: EngineRequestInit<Path>,
): Promise<EngineResponse<Path>> {
  const config = engineConfig();
  const method = init.method ?? "GET";
  const body = exactBodyBytes(init.body);
  const requestId = init.actor.requestId || uuidv7();
  const protectedHeaders = createEngineHmacHeaders({
    keyId: config.keyId,
    secret: config.secret,
    method,
    pathAndQuery: path,
    timestamp: Math.floor(Date.now() / 1000),
    nonce: uuidv7(),
    actorId: init.actor.userId,
    requestId,
    body,
  });
  const headers = new Headers(init.headers);
  for (const name of Object.keys(protectedHeaders)) headers.delete(name);
  for (const [name, value] of Object.entries(protectedHeaders)) headers.set(name, value);
  const timeout = method === "GET" || method === "HEAD" ? 10_000 : 30_000;
  const signal = AbortSignal.any([init.signal, AbortSignal.timeout(timeout)].filter(Boolean) as AbortSignal[]);
  return fetch(new URL(path, config.baseUrl), {
    ...init,
    method,
    headers,
    body: body.byteLength === 0 ? undefined : new Blob([new Uint8Array(body).buffer]),
    signal,
  }) as Promise<EngineResponse<Path>>;
}

function engineConfig(): { baseUrl: string; keyId: string; secret: string } {
  const baseUrl = process.env.NEXUS_ENGINE_BASE_URL;
  const keyId = process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  const secret = process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  if (!baseUrl || !keyId || !secret) throw new Error("NEXUS engine service configuration is incomplete");
  return { baseUrl, keyId, secret };
}

function exactBodyBytes(body: EngineRequestInit<keyof TemporaryInternalPaths>["body"]): Uint8Array {
  if (body === undefined) return new Uint8Array();
  if (typeof body === "string") return new TextEncoder().encode(body);
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  throw new TypeError("engine request bodies must have exact byte representation");
}
