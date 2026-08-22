import "server-only";

import { NexusApiAuthError, requireNexusApiActor, type NexusActor } from "../auth/nexus-user";
import { NexusBffError, safeBffErrorResponse } from "./errors";
import { sanitizeEngineResponse } from "./client";

const BFF_ROUTE = Symbol("nexus-bff-route");
const MAX_REQUEST_BYTES = 64 * 1024;
export type NexusBffOptions = { requireIdempotencyKey?: boolean; requireVersion?: boolean };
type NexusBffHandler = (request: Request, actor: NexusActor) => Promise<Response>;
type MarkedRoute = ((request: Request) => Promise<Response>) & { [BFF_ROUTE]?: true };

export function withNexusBff(handler: NexusBffHandler, options: NexusBffOptions = {}): MarkedRoute {
  const wrapped: MarkedRoute = async (request) => {
    try {
      const actor = await requireNexusApiActor(request);
      const safeRequest = await boundedRequest(request, options);
      return await sanitizeEngineResponse(await handler(safeRequest, actor));
    } catch (error) {
      if (error instanceof NexusApiAuthError) return safeBffErrorResponse(new NexusBffError(error.status, error.status === 401 ? "UNAUTHENTICATED" : "FORBIDDEN", error.message));
      return safeBffErrorResponse(error);
    }
  };
  Object.defineProperty(wrapped, BFF_ROUTE, { value: true });
  return wrapped;
}
export function isNexusBffRoute(value: unknown): value is MarkedRoute { return typeof value === "function" && (value as MarkedRoute)[BFF_ROUTE] === true; }

async function boundedRequest(request: Request, options: NexusBffOptions): Promise<Request> {
  const method = request.method.toUpperCase(); const isWrite = !["GET", "HEAD", "OPTIONS"].includes(method);
  if (isWrite) {
    if (request.headers.get("origin") !== new URL(request.url).origin) throw new NexusBffError(403, "FORBIDDEN", "Cross-origin writes are not permitted.");
    if ((options.requireIdempotencyKey ?? method !== "PUT") && !request.headers.get("idempotency-key")) throw new NexusBffError(400, "VALIDATION_ERROR", "Idempotency-Key is required.");
    if (options.requireVersion && !request.headers.get("if-match")) throw new NexusBffError(400, "VALIDATION_ERROR", "If-Match is required.");
  }
  const length = request.headers.get("content-length");
  if (length !== null && (!/^\d+$/.test(length) || Number(length) > MAX_REQUEST_BYTES)) throw new NexusBffError(400, "VALIDATION_ERROR", "Request body is too large.");
  const bytes = await readBounded(request, MAX_REQUEST_BYTES);
  const headers = new Headers(request.headers); headers.delete("content-length");
  return new Request(request.url, { method: request.method, headers, body: bytes.byteLength && !["GET", "HEAD"].includes(method) ? bytes as unknown as BodyInit : undefined });
}
async function readBounded(request: Request, limit: number): Promise<Uint8Array> {
  if (!request.body) return new Uint8Array(); const reader = request.body.getReader(); const chunks: Uint8Array[] = []; let total = 0;
  while (true) { const { done, value } = await reader.read(); if (done) break; total += value.byteLength; if (total > limit) { await reader.cancel(); throw new NexusBffError(400, "VALIDATION_ERROR", "Request body is too large."); } chunks.push(value); }
  const all = new Uint8Array(total); let offset = 0; for (const chunk of chunks) { all.set(chunk, offset); offset += chunk.byteLength; } return all;
}
