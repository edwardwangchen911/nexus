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
  return new Request("https://nexus.local/api/v1/example", {
    method: "POST",
    headers: { origin: "https://nexus.local", "idempotency-key": "idem-1", ...headers },
    body: stream as unknown as BodyInit,
    duplex: "half",
  } as RequestInit);
}

beforeEach(() => {
  requireActor.mockReset();
  requireActor.mockResolvedValue(actor);
});

describe("withNexusBff", () => {
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
});
