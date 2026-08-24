import { afterEach, describe, expect, it, vi } from "vitest";
import { engineFetch, sanitizeEngineResponse, timeoutForMethod } from "../../../app/lib/engine/client";
import { ERROR_STATUSES, rebuildAllowedEngineResponse, SAFE_ENGINE_ERROR_MESSAGES } from "../../../app/lib/engine/errors";
import { createEngineHmacHeaders } from "../../../app/lib/engine/hmac";

const actor = { userId: "nexus-test-user", requestId: "0198f7a0-1111-7777-8111-111111111114" };
const engineRequestId = "0198f7a0-1111-7777-8111-111111111115";

function successPayload(asOf = "2026-08-22T00:00:00Z") {
  return {
    data: { status: "ok" },
    meta: { request_id: engineRequestId, schema_version: "1", as_of: asOf },
  };
}

function integrationStatus() {
  return {
    as_of: "2026-08-22T00:00:00Z",
    capability_expires_at: null,
    contract_hash: null,
    mode: "fixture",
    next_action: "Read-only admission is required.",
    provider: "fosun",
    quality: "PASS",
    version: 1,
  };
}

function marketEnvelope(barCount = 1) {
  const bar = {
    session: "2026-08-21",
    open: "228.200000000000",
    high: "229.400000000000",
    low: "225.900000000000",
    close: "227.160000000000",
    volume: "46300000.000000000000",
    available_at: "2026-08-22T00:01:00Z",
    observed_at: "2026-08-21T20:00:00Z",
    quality_flags: [],
  };
  return {
    data: {
      items: [{
        instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781",
        symbol: "AAPL",
        price: "227.160000000000",
        source: "alpaca",
        feed: "iex",
        delivery_mode: "real_time",
        as_of: "2026-08-21T20:00:00Z",
        quality: "PASS",
        quality_reasons: [],
        watchlisted: true,
      }],
      next_cursor: null,
      selected_series: {
        instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781",
        source: "alpaca",
        feed: "sip",
        delivery_mode: "delayed",
        adjustment: "raw",
        interval: "1Day",
        quality: "PASS",
        quality_reasons: [],
        bars: Array.from({ length: barCount }, (_, index) => ({
          ...bar,
          session: new Date(Date.UTC(2025, 0, index + 1)).toISOString().slice(0, 10),
        })),
      },
    },
    meta: successPayload().meta,
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

  it("accepts the strict integration list envelope without leaking extra fields", async () => {
    setEngineEnvironment();
    vi.stubGlobal("fetch", async () => jsonResponse({
      data: [integrationStatus()],
      meta: successPayload().meta,
    }));

    const response = await engineFetch("/internal/v1/integrations", { method: "GET", actor });

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      data: [integrationStatus()],
      meta: successPayload().meta,
    });

    vi.stubGlobal("fetch", async () => jsonResponse({
      data: [{ ...integrationStatus(), credential: "must-not-escape" }],
      meta: successPayload().meta,
    }));
    await expectSafeInternalError(
      await engineFetch("/internal/v1/integrations", { method: "GET", actor }),
    );
  });

  it("accepts only the generated closed market contract at the runtime boundary", async () => {
    const payload = marketEnvelope();
    const accepted = await sanitizeEngineResponse(
      jsonResponse(payload),
      undefined,
      "/internal/v1/market",
    );
    expect(accepted.status).toBe(200);
    expect(await accepted.json()).toEqual(payload);

    const withNestedSecret = marketEnvelope();
    Object.assign(withNestedSecret.data.selected_series.bars[0], { credential: "must-not-escape" });
    await expectSafeInternalError(await sanitizeEngineResponse(
      jsonResponse(withNestedSecret),
      undefined,
      "/internal/v1/market",
    ));

    const invalidDecimal = marketEnvelope();
    invalidDecimal.data.items[0].price = "NaN";
    await expectSafeInternalError(await sanitizeEngineResponse(
      jsonResponse(invalidDecimal),
      undefined,
      "/internal/v1/market",
    ));
  });

  it("streams the legal 500-bar market envelope through the real engine client", async () => {
    setEngineEnvironment();
    const payload = marketEnvelope(500);
    const encodedBytes = new TextEncoder().encode(JSON.stringify(payload)).byteLength;
    expect(encodedBytes).toBeGreaterThan(64 * 1024);
    expect(encodedBytes).toBeLessThanOrEqual(256 * 1024);
    vi.stubGlobal("fetch", async () => jsonResponse(payload));

    const response = await engineFetch({
      template: "/internal/v1/market",
      path: "/internal/v1/market",
      query: "limit=50",
    }, { method: "GET", actor });

    expect(response.status).toBe(200);
    expect((await response.json()).data.selected_series.bars).toHaveLength(500);
  });

  it("enforces explicit market and selected-series collection bounds", () => {
    const tooManyItems = marketEnvelope();
    tooManyItems.data.items = Array.from({ length: 101 }, () => tooManyItems.data.items[0]);
    expect(rebuildAllowedEngineResponse(200, tooManyItems, new Headers(), undefined, "/internal/v1/market")).toBeNull();

    const tooManyBars = marketEnvelope();
    tooManyBars.data.selected_series.bars = Array.from({ length: 501 }, () => tooManyBars.data.selected_series.bars[0]);
    expect(rebuildAllowedEngineResponse(200, tooManyBars, new Headers(), undefined, "/internal/v1/market")).toBeNull();
  });

  it("accepts only the generated closed watchlist membership contract", async () => {
    const payload = {
      data: { instrument_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781", watchlisted: true },
      meta: successPayload().meta,
    };
    const accepted = await sanitizeEngineResponse(
      jsonResponse(payload),
      undefined,
      "/internal/v1/watchlist/018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781",
    );
    expect(accepted.status).toBe(200);
    expect(await accepted.json()).toEqual(payload);

    await expectSafeInternalError(await sanitizeEngineResponse(
      jsonResponse({ ...payload, data: { ...payload.data, account_id: "must-not-escape" } }),
      undefined,
      "/internal/v1/watchlist/018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781",
    ));
  });

  it("signs the resolved dynamic integration path and rejects template mismatches", async () => {
    setEngineEnvironment();
    vi.spyOn(Date, "now").mockReturnValue(1_787_371_200_000);
    let sentInit: RequestInit | undefined;
    let calls = 0;
    vi.stubGlobal("fetch", async (_input: RequestInfo | URL, init?: RequestInit) => {
      calls += 1;
      sentInit = init;
      return jsonResponse({ data: integrationStatus(), meta: successPayload().meta });
    });
    const body = new Uint8Array([123, 125]);
    const response = await engineFetch({
      template: "/internal/v1/integrations/{provider}/test",
      path: "/internal/v1/integrations/fosun/test",
    }, { method: "POST", actor, body });

    expect(response.status).toBe(200);
    const sentHeaders = new Headers(sentInit?.headers);
    const expectedHeaders = createEngineHmacHeaders({
      keyId: "test-current",
      secret: "nexus-test-hmac-key-32-bytes!!!!",
      method: "POST",
      pathAndQuery: "/internal/v1/integrations/fosun/test",
      timestamp: 1_787_371_200,
      nonce: sentHeaders.get("x-nexus-nonce")!,
      actorId: actor.userId,
      requestId: actor.requestId,
      body,
    });
    expect(sentHeaders.get("x-nexus-signature")).toBe(expectedHeaders["X-Nexus-Signature"]);

    const rejected = await engineFetch({
      template: "/internal/v1/integrations/{provider}/mode",
      path: "/internal/v1/integrations/fosun/test",
    }, { method: "POST", actor, body });
    await expectSafeInternalError(rejected);
    expect(calls).toBe(1);
  });

  it("sends and signs one canonical allowlisted market query", async () => {
    setEngineEnvironment();
    vi.spyOn(Date, "now").mockReturnValue(1_787_371_200_000);
    let sentInput: RequestInfo | URL | undefined;
    let sentInit: RequestInit | undefined;
    vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
      sentInput = input;
      sentInit = init;
      return jsonResponse({
        error: {
          code: "PROVIDER_UNAVAILABLE",
          message: "must be redacted",
          request_id: engineRequestId,
          details: {},
        },
      }, 503);
    });
    const query = `watchlist=true&selected_instrument_id=018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781&limit=50&cursor=opaque%2Bcursor`;

    const response = await engineFetch({
      template: "/internal/v1/market",
      path: "/internal/v1/market",
      query,
    }, { method: "GET", actor });

    expect(response.status).toBe(503);
    const canonicalPath = "/internal/v1/market?cursor=opaque%2Bcursor&limit=50&selected_instrument_id=018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781&watchlist=true";
    expect(String(sentInput)).toBe(`http://127.0.0.1:8000${canonicalPath}`);
    const sentHeaders = new Headers(sentInit?.headers);
    const expected = createEngineHmacHeaders({
      keyId: "test-current",
      secret: "nexus-test-hmac-key-32-bytes!!!!",
      method: "GET",
      pathAndQuery: canonicalPath,
      timestamp: 1_787_371_200,
      nonce: sentHeaders.get("x-nexus-nonce")!,
      actorId: actor.userId,
      requestId: actor.requestId,
      body: new Uint8Array(),
    });
    expect(sentHeaders.get("x-nexus-signature")).toBe(expected["X-Nexus-Signature"]);
  });

  it("canonicalizes overview filters and accepts only allowlisted optimization paths", async () => {
    setEngineEnvironment();
    const sent: string[] = [];
    vi.stubGlobal("fetch", async (input: RequestInfo | URL) => {
      sent.push(String(input));
      return jsonResponse({
        error: {
          code: "NOT_FOUND",
          message: "must be redacted",
          request_id: engineRequestId,
          details: {},
        },
      }, 404);
    });

    await engineFetch({
      template: "/internal/v1/overview",
      path: "/internal/v1/overview",
      query: "session_date=2026-08-24&portfolio_id=0198d500-1000-7000-8000-000000001301",
    } as never, { method: "GET", actor } as never);
    await engineFetch({
      template: "/internal/v1/optimization-proposals/{group_id}/shadow",
      path: "/internal/v1/optimization-proposals/0198d500-1000-7000-8000-000000001303/shadow",
    } as never, { method: "POST", actor, body: new Uint8Array() } as never);

    expect(sent).toEqual([
      "http://127.0.0.1:8000/internal/v1/overview?portfolio_id=0198d500-1000-7000-8000-000000001301&session_date=2026-08-24",
      "http://127.0.0.1:8000/internal/v1/optimization-proposals/0198d500-1000-7000-8000-000000001303/shadow",
    ]);

    await engineFetch({
      template: "/internal/v1/optimization-proposals/{group_id}/reject",
      path: "/internal/v1/optimization-proposals/not-a-uuid/reject",
    } as never, { method: "POST", actor } as never);
    await engineFetch({
      template: "/internal/v1/overview",
      path: "/internal/v1/overview",
      query: "session_date=2026-02-30",
    } as never, { method: "GET", actor } as never);
    expect(sent).toHaveLength(2);
  });

  it.each([
    { template: "/internal/v1/market", path: "/internal/v1/market", query: "limit=10&limit=20" },
    { template: "/internal/v1/market", path: "/internal/v1/market", query: "limit=101" },
    { template: "/internal/v1/market", path: "/internal/v1/market", query: "selected_instrument_id=not-a-uuid" },
    { template: "/internal/v1/market", path: "/internal/v1/market", query: "redirect=https%3A%2F%2Fattacker.invalid" },
    { template: "/internal/v1/integrations/{provider}/test", path: "/internal/v1/integrations/fosun/test", query: "limit=1" },
  ])("rejects a non-canonical or unauthorized engine query %# before fetch", async (target) => {
    setEngineEnvironment();
    const fetchSpy = vi.fn(async () => jsonResponse(successPayload()));
    vi.stubGlobal("fetch", fetchSpy);

    await expect(engineFetch(target as never, { method: "GET", actor } as never)).resolves.toMatchObject({ status: 500 });
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it.each([
    "//attacker.invalid/internal/v1/market",
    "/internal/v1/market/../system-status",
    "/internal/v1/market%2F..%2Fsystem-status",
  ])("rejects unsafe market target path %s before fetch", async (path) => {
    setEngineEnvironment();
    const fetchSpy = vi.fn(async () => jsonResponse(successPayload()));
    vi.stubGlobal("fetch", fetchSpy);

    await expect(engineFetch({
      template: "/internal/v1/market",
      path,
      query: "limit=10",
    } as never, { method: "GET", actor } as never)).resolves.toMatchObject({ status: 500 });
    expect(fetchSpy).not.toHaveBeenCalled();
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
  ))("rebuilds a redacted public %s envelope at allowed status %i", async (code, status) => {
    const payload = {
      error: {
        code,
        message: "upstream message must not escape",
        request_id: engineRequestId,
        details: { api_key: "must-not-escape", nested: { credential: "must-not-escape" } },
      },
    };

    const response = await sanitizeEngineResponse(jsonResponse(payload, status));

    expect(response.status).toBe(status);
    expect(await response.json()).toEqual({
      error: {
        code,
        message: SAFE_ENGINE_ERROR_MESSAGES[code as keyof typeof SAFE_ENGINE_ERROR_MESSAGES],
        request_id: engineRequestId,
        details: {},
      },
    });
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
