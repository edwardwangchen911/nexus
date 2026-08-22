import { beforeEach, describe, expect, it, vi } from "vitest";

const { requireActor } = vi.hoisted(() => ({ requireActor: vi.fn() }));

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {
    constructor(readonly status: 401 | 403, message: string) {
      super(message);
    }
  },
  requireNexusApiActor: requireActor,
}));

import { NexusApiAuthError } from "../../../app/lib/auth/nexus-user";
import { withNexusBff } from "../../../app/lib/engine/with-nexus-bff";

const actor = { userId: "nexus-test-user", requestId: "0198f7a0-1111-7777-8111-111111111114" };

function request(body: Uint8Array, headers: HeadersInit = {}): Request {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(body);
      controller.close();
    },
  });
  return requestFromStream(stream, headers);
}

function requestFromStream(stream: ReadableStream<Uint8Array>, headers: HeadersInit = {}): Request {
  return new Request("https://nexus.local/api/v1/example", {
    method: "POST",
    headers: { origin: "https://nexus.local", "idempotency-key": "idem-1", ...headers },
    body: stream as unknown as BodyInit,
    duplex: "half",
  } as RequestInit);
}

function successPayload() {
  return {
    data: { status: "ok" },
    meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-22T00:00:00Z" },
  };
}

async function expectSafeError(response: Response, status: 400 | 500, code: "VALIDATION_ERROR" | "INTERNAL_ERROR"): Promise<void> {
  expect(response.status).toBe(status);
  expect(await response.json()).toEqual({
    error: {
      code,
      message: status === 400 ? "Request body is too large." : "The request could not be completed.",
      request_id: expect.stringMatching(/^.{8}-.{4}-7.{3}-.{4}-.{12}$/),
      details: {},
    },
  });
}

beforeEach(() => {
  requireActor.mockReset();
  requireActor.mockResolvedValue(actor);
});

describe("withNexusBff", () => {
  it.each([
    [200, { proposal_id: "proposal-1" }, {}],
    [202, { run_id: "run-1" }, { location: "/api/v1/runs/run-1" }],
    [409, null, { error: { code: "VERSION_CONFLICT", message: "conflict", request_id: actor.requestId, details: {} } }],
    [503, null, { error: { code: "PROVIDER_UNAVAILABLE", message: "unavailable", request_id: actor.requestId, details: {} } }],
  ])("passes safe generic engine envelope status %s", async (status, data, error) => {
    const payload = data === null
      ? error
      : { data, meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-22T00:00:00Z" } };
    const response = await withNexusBff(async () => Response.json(payload, { status, headers: { location: "/api/v1/runs/run-1", "x-upstream": "discard" } }))(request(new Uint8Array()));
    expect(response.status).toBe(status);
    expect(response.headers.get("x-upstream")).toBeNull();
  });
  it("streams an exact allowed body to the real handler", async () => {
    let received: number[] = [];
    const handler = vi.fn(async (incoming: Request) => {
      received = Array.from(new Uint8Array(await incoming.arrayBuffer()));
      return Response.json({ data: { status: "ok" }, meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-22T00:00:00Z" } });
    });
    const wrapped = withNexusBff(handler);

    const response = await wrapped(request(new Uint8Array([0, 1, 2])));

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ data: { status: "ok" } });
    expect(received).toEqual([0, 1, 2]);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it.each([64 * 1024 + 1, 64 * 1024])("rejects oversized streamed bodies but accepts the exact limit", async (size) => {
    const handler = vi.fn(async () => Response.json({ data: { status: "ok" }, meta: { request_id: actor.requestId, schema_version: "1", as_of: "2026-08-22T00:00:00Z" } }));
    const response = await withNexusBff(handler)(request(new Uint8Array(size), { "content-length": "1" }));

    expect(response.status).toBe(size > 64 * 1024 ? 400 : 200);
  });

  it("maps auth failures to the full safe error envelope", async () => {
    requireActor.mockRejectedValue(new NexusApiAuthError(401, "Authentication is required."));

    const response = await withNexusBff(async () => Response.json({}))(request(new Uint8Array()));
    const payload = await response.json();

    expect(response.status).toBe(401);
    expect(payload.error).toMatchObject({ code: "UNAUTHENTICATED", message: "Authentication is required.", details: {} });
    expect(payload.error.request_id).toMatch(/^.{8}-.{4}-7.{3}-.{4}-.{12}$/);
  });

  it.each([
    ["/internal/v1/next?cursor=1#page", "/internal/v1/next?cursor=1#page"],
    ["https://nexus.local/api/v1/runs/run-1?cursor=1#page", "/api/v1/runs/run-1?cursor=1#page"],
    ["https://attacker.invalid/steal", null],
    ["//attacker.invalid/steal", null],
  ])("sanitizes BFF-side Location %s", async (location, expected) => {
    const wrapped = withNexusBff(async () => Response.json(successPayload(), {
      headers: { location, "x-upstream": "discard" },
    }));

    const response = await wrapped(request(new Uint8Array()));

    expect(response.status).toBe(200);
    expect(response.headers.get("location")).toBe(expected);
    expect(response.headers.get("x-upstream")).toBeNull();
  });

  it("preserves Retry-After and ETag but removes arbitrary upstream headers", async () => {
    const wrapped = withNexusBff(async () => Response.json(successPayload(), {
      headers: { etag: '"revision-1"', "retry-after": "60", "x-upstream-secret": "discard" },
    }));

    const response = await wrapped(request(new Uint8Array()));

    expect(response.headers.get("etag")).toBe('"revision-1"');
    expect(response.headers.get("retry-after")).toBe("60");
    expect(response.headers.get("x-upstream-secret")).toBeNull();
  });

  it("rejects an oversized request even when stream cancellation rejects", async () => {
    let cancelAttempted = false;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(64 * 1024 + 1));
      },
      cancel() {
        cancelAttempted = true;
        return Promise.reject(new Error("cancel failed"));
      },
    });
    const handler = vi.fn(async () => Response.json(successPayload()));

    const response = await withNexusBff(handler)(requestFromStream(stream));

    await expectSafeError(response, 400, "VALIDATION_ERROR");
    expect(cancelAttempted).toBe(true);
    expect(handler).not.toHaveBeenCalled();
  });
});
