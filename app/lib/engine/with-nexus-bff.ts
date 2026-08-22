import "server-only";

import { NexusApiAuthError, requireNexusApiActor, type NexusActor } from "../auth/nexus-user";
import { NexusBffError, safeBffErrorResponse } from "./errors";

const BFF_ROUTE = Symbol("nexus-bff-route");
const MAX_REQUEST_BYTES = 64 * 1024;

export type NexusBffOptions = {
  requireIdempotencyKey?: boolean;
  requireVersion?: boolean;
};

type NexusBffHandler = (request: Request, actor: NexusActor) => Promise<Response>;
type MarkedRoute = ((request: Request) => Promise<Response>) & { [BFF_ROUTE]?: true };

export function withNexusBff(handler: NexusBffHandler, options: NexusBffOptions = {}): MarkedRoute {
  const wrapped: MarkedRoute = async (request) => {
    try {
      const actor = await requireNexusApiActor(request);
      await enforceRequestBoundary(request, options);
      return await handler(request, actor);
    } catch (error) {
      if (error instanceof NexusApiAuthError) {
        return Response.json(
          { error: { code: error.status === 401 ? "UNAUTHENTICATED" : "FORBIDDEN", message: error.message } },
          { status: error.status },
        );
      }
      return safeBffErrorResponse(error);
    }
  };
  Object.defineProperty(wrapped, BFF_ROUTE, { value: true });
  return wrapped;
}

export function isNexusBffRoute(value: unknown): value is MarkedRoute {
  return typeof value === "function" && (value as MarkedRoute)[BFF_ROUTE] === true;
}

async function enforceRequestBoundary(request: Request, options: NexusBffOptions): Promise<void> {
  const method = request.method.toUpperCase();
  const isWrite = !["GET", "HEAD", "OPTIONS"].includes(method);
  if (isWrite) {
    const origin = request.headers.get("origin");
    if (!origin || origin !== new URL(request.url).origin) {
      throw new NexusBffError(403, "FORBIDDEN", "Cross-origin writes are not permitted.");
    }
    if ((options.requireIdempotencyKey ?? method !== "PUT") && !request.headers.get("idempotency-key")) {
      throw new NexusBffError(400, "VALIDATION_ERROR", "Idempotency-Key is required.");
    }
    if (options.requireVersion && !request.headers.get("if-match")) {
      throw new NexusBffError(400, "VALIDATION_ERROR", "If-Match is required.");
    }
  }
  const declaredLength = Number(request.headers.get("content-length") ?? "0");
  if (!Number.isFinite(declaredLength) || declaredLength > MAX_REQUEST_BYTES) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "Request body is too large.");
  }
  if ((await request.clone().arrayBuffer()).byteLength > MAX_REQUEST_BYTES) {
    throw new NexusBffError(400, "VALIDATION_ERROR", "Request body is too large.");
  }
}
