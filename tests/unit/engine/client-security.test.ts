import { afterEach, describe, expect, it, vi } from "vitest";
import { engineFetch, sanitizeEngineResponse, timeoutForMethod } from "../../../app/lib/engine/client";
import { ERROR_STATUSES } from "../../../app/lib/engine/errors";
import { createEngineHmacHeaders } from "../../../app/lib/engine/hmac";

const actor = { userId: "nexus-test-user", requestId: "0198f7a0-1111-7777-8111-111111111114" };
const engineRequestId = "0198f7a0-1111-7777-8111-111111111115";

function successPayload(asOf = "2026-08-22T00:00:00Z") {
  return {
    data: { status: "ok" },
    meta: { request_id: engineRequestId, schema_version: "1", as_of: asOf },
  };
}

function jsonResponse(value: unknown, status = 200, headers: HeadersInit = {}): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

async function expectSafeInternalError(response: Response): Promise<void> {
  expect(response.status).toBe(500);
  expect(Array.from(response.headers.keys())).toEqual(["content-type"]);
  expect(await response.json()).toEqual({
    error: {
      code: "INTERNAL_ERROR",
      message: "The request could not be completed.",
      request_id: expect.stringMatching(/^.{8}-.{4}-7.{3}-.{4}-.{12}$/),
      details: {},
    },
  });
}

function setEngineEnvironment(): void {
  process.env.NEXUS_ENGINE_BASE_URL = "http://127.0.0.1:8000";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID = "test-current";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET = "nexus-test-hmac-key-32-bytes!!!!";
  process.env.NEXUS_ENV = "test";
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete process.env.NEXUS_ENGINE_BASE_URL;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  delete process.env.NEXUS_ENV;
});

describe("engineFetch runtime boundary", () => {
  it.each(["//attacker.invalid", "https://attacker.invalid/x", "/internal/v1/system-status?x=1", "/internal/v1/unknown"])(
    "rejects unsafe path %s before any network call",
    async (path) => {
      setEngineEnvironment();
      let calls = 0;
      vi.stubGlobal("fetch", async () => {
        calls += 1;
        return Response.json({});
      });

      await expect(engineFetch(path as "/internal/v1/system-status", { method: "GET", actor })).resolves.toMatchObject({ status: 500 });
      expect(calls).toBe(0);
    },
  );

  it("rebuilds only the strict system-status success envelope", async () => {
    setEngineEnvironment();
    vi.stubGlobal(
      "fetch",
      async () =>
        new Response(
          JSON.stringify({
            data: { status: "ok" },
            meta: {
              request_id: "0198f7a0-1111-7777-8111-111111111114",
              schema_version: "1",
              as_of: "2026-08-22T00:00:00Z",
            },
          }),
          { status: 200, headers: { "content-type": "application/json", "x-secret": "discard" } },
        ),
    );

    const response = await engineFetch("/internal/v1/system-status", { method: "GET", actor });

    expect(response.status).toBe(200);
    expect(response.headers.get("x-secret")).toBeNull();
    expect(await response.json()).toEqual({
      data: { status: "ok" },
      meta: {
        request_id: "0198f7a0-1111-7777-8111-111111111114",
        schema_version: "1",
        as_of: "2026-08-22T00:00:00Z",
      },
    });
  });

  it.each([["GET", 10_000], ["HEAD", 10_000], ["POST", 30_000], ["PUT", 30_000]])("uses %i ms timeout for %s", (method, expected) => {
    expect(timeoutForMethod(method as string)).toBe(expected);
  });

  it.each([
    "2026-08-22",
    "2026-08-22T08:00:00+08:00",
    "tomorrow morning",
    "2026-02-30T00:00:00Z",
  ])("rejects invalid success meta.as_of %s", async (asOf) => {
    await expectSafeInternalError(await sanitizeEngineResponse(jsonResponse(successPayload(asOf))));
  });

  it.each(["2026-08-22T00:00:00Z", "2026-08-22T00:00:00.123456Z"])(
    "accepts UTC success meta.as_of %s",
    async (asOf) => {
      const payload = successPayload(asOf);
      const response = await sanitizeEngineResponse(jsonResponse(payload));

      expect(response.status).toBe(200);
      expect(await response.json()).toEqual(payload);
    },
  );

  it.each(Object.entries(ERROR_STATUSES).flatMap(([code, statuses]) =>
    statuses.map((status) => [code, status] as const),
  ))("rebuilds the exact public %s envelope at allowed status %i", async (code, status) => {
    const payload = {
      error: {
        code,
        message: "public engine message",
        request_id: engineRequestId,
        details: { field: "value" },
      },
    };

    const response = await sanitizeEngineResponse(jsonResponse(payload, status));

    expect(response.status).toBe(status);
    expect(await response.json()).toEqual(payload);
  });

  it.each([
    [409, "UNKNOWN_CODE"],
    [409, "UNAUTHENTICATED"],
    [422, "DATA_STALE"],
  ])("rejects code/status pair %s %s at the client boundary", async (status, code) => {
    await expectSafeInternalError(await sanitizeEngineResponse(jsonResponse({
      error: {
        code,
        message: "must not escape",
        request_id: engineRequestId,
        details: { upstream: "must not escape" },
      },
    }, status, { "retry-after": "60", "x-upstream": "secret" })));
  });

  it.each([
    "/internal/v1/next",
    "http://127.0.0.1:8000/internal/v1/next?cursor=1",
    "//attacker.invalid/steal",
    "https://attacker.invalid/steal",
  ])("drops engine-side Location %s", async (location) => {
    setEngineEnvironment();
    vi.stubGlobal("fetch", async () => jsonResponse(successPayload(), 200, { location }));

    const response = await engineFetch("/internal/v1/system-status", { method: "GET", actor });

    expect(response.status).toBe(200);
    expect(response.headers.get("location")).toBeNull();
  });

  it("retains only Retry-After and ETag from a valid public response", async () => {
    const response = await sanitizeEngineResponse(jsonResponse(successPayload(), 200, {
      etag: '"revision-1"',
      "retry-after": "60",
      "x-upstream-secret": "discard",
    }));

    expect(response.headers.get("etag")).toBe('"revision-1"');
    expect(response.headers.get("retry-after")).toBe("60");
    expect(response.headers.get("x-upstream-secret")).toBeNull();
  });

  it("maps non-JSON, malformed, and oversized upstream responses to safe errors", async () => {
    const cases = [
      new Response("upstream secret", { status: 502, headers: { "content-type": "text/plain", "x-upstream": "secret" } }),
      jsonResponse({ data: { upstream: "secret" } }, 200, { "x-upstream": "secret" }),
      jsonResponse({
        data: { padding: "x".repeat(64 * 1024) },
        meta: successPayload().meta,
      }, 200, { "x-upstream": "secret" }),
    ];

    for (const upstream of cases) {
      await expectSafeInternalError(await sanitizeEngineResponse(upstream));
    }
  });

  it("completes a safe error when oversized response stream cancellation rejects", async () => {
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

    const response = await sanitizeEngineResponse(new Response(stream, {
      status: 200,
      headers: { "content-type": "application/json", "x-upstream": "secret" },
    }));

    await expectSafeInternalError(response);
    expect(cancelAttempted).toBe(true);
  });

  it.each([
    { inputMethod: "get", expectedMethod: "GET", expectedTimeout: 10_000, body: undefined },
    { inputMethod: "post", expectedMethod: "POST", expectedTimeout: 30_000, body: new Uint8Array([0, 1, 2, 255]) },
    { inputMethod: "put", expectedMethod: "PUT", expectedTimeout: 30_000, body: '{"version":7}' },
  ])("uses one normalized $expectedMethod method for fetch, HMAC, body, and timeout", async ({ inputMethod, expectedMethod, expectedTimeout, body }) => {
    setEngineEnvironment();
    vi.spyOn(Date, "now").mockReturnValue(1_787_371_200_000);
    const timeoutSpy = vi.spyOn(AbortSignal, "timeout");
    let sentInit: RequestInit | undefined;
    vi.stubGlobal("fetch", async (_input: RequestInfo | URL, init?: RequestInit) => {
      sentInit = init;
      return jsonResponse(successPayload());
    });
    const unsafeInit = { method: inputMethod, actor, body } as unknown as Parameters<typeof engineFetch>[1];

    const response = await engineFetch("/internal/v1/system-status", unsafeInit);

    expect(response.status).toBe(200);
    expect(sentInit?.method).toBe(expectedMethod);
    expect(timeoutSpy).toHaveBeenCalledWith(expectedTimeout);
    const sentBody = sentInit?.body === undefined ? new Uint8Array() : sentInit.body as Uint8Array;
    const expectedBody = typeof body === "string" ? new TextEncoder().encode(body) : body ?? new Uint8Array();
    expect(Array.from(sentBody)).toEqual(Array.from(expectedBody));
    const sentHeaders = new Headers(sentInit?.headers);
    const expectedHeaders = createEngineHmacHeaders({
      keyId: "test-current",
      secret: "nexus-test-hmac-key-32-bytes!!!!",
      method: expectedMethod,
      pathAndQuery: "/internal/v1/system-status",
      timestamp: 1_787_371_200,
      nonce: sentHeaders.get("x-nexus-nonce")!,
      actorId: actor.userId,
      requestId: actor.requestId,
      body: sentBody,
    });
    expect(sentHeaders.get("x-nexus-signature")).toBe(expectedHeaders["X-Nexus-Signature"]);
    expect(sentHeaders.get("x-nexus-body-sha256")).toBe(expectedHeaders["X-Nexus-Body-SHA256"]);
  });
});
