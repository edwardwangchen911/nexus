import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const actor = vi.hoisted(() => ({
  userId: "allowed-user",
  requestId: "0198f7a0-1111-7777-8111-111111111111",
}));

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

import MarketPage from "../../../app/(protected)/market/page";
import { GET as getMarket } from "../../../app/api/v1/market/route";
import { createEngineHmacHeaders } from "../../../app/lib/engine/hmac";

const instrumentId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781";
const hmacSecret = "nexus-test-hmac-key-32-bytes!!!!";

function engineEnvelope(includeSeries: boolean, explicitTimes = false) {
  const bars = Array.from({ length: 500 }, (_, index) => ({
    session: new Date(Date.UTC(2025, 0, index + 1)).toISOString().slice(0, 10),
    open: "228.200000000000",
    high: "229.400000000000",
    low: "225.900000000000",
    close: "227.160000000000",
    volume: "46300000.000000000000",
    available_at: "2026-08-22T00:01:00Z",
    observed_at: "2026-08-21T20:00:00Z",
    quality_flags: [],
  }));
  return {
    data: {
      items: [{
        instrument_id: instrumentId,
        symbol: "AAPL",
        price: "227.160000000000",
        source: "alpaca",
        feed: "iex",
        delivery_mode: "real_time",
        as_of: "2026-08-21T20:00:00Z",
        ...(explicitTimes ? { source_observed_at: null, received_at: "2026-08-21T21:02:03Z" } : {}),
        quality: "PASS",
        quality_reasons: [],
        watchlisted: true,
      }],
      next_cursor: null,
      selected_series: includeSeries ? {
        instrument_id: instrumentId,
        source: "polygon",
        feed: "delayed",
        delivery_mode: "delayed",
        adjustment: "raw",
        interval: "1Day",
        quality: "PASS",
        quality_reasons: [],
        bars,
      } : null,
      intraday_monitor: includeSeries ? {
        instrument_id: instrumentId,
        status: "ready",
        source: "alpaca",
        feed: "iex",
        delivery_mode: "real_time",
        last: "227.160000000000",
        bid: "227.110000000000",
        ask: "227.210000000000",
        trade_as_of: "2026-08-21T20:00:02Z",
        quote_as_of: "2026-08-21T20:00:01Z",
        bar_as_of: "2026-08-21T20:00:00Z",
        return_5m: "0.010000000000",
        return_30m: "-0.025000000000",
        range_30m: "0.031250000000",
        spread_bps: "4.402300000000",
        quote_age_seconds: 14,
        bar_age_seconds: 15,
        sample_count: 4,
        quality_reasons: [],
        bars: ["19:57", "19:58", "19:59", "20:00"].map((time, index) => ({
          observed_at: `2026-08-21T${time}:00Z`,
          available_at: `2026-08-21T${time}:00Z`,
          open: `${224 + index}.000000000000`,
          high: `${225 + index}.000000000000`,
          low: `${223 + index}.000000000000`,
          close: `${224 + index}.500000000000`,
          volume: `${1_000_000 + index}.000000000000`,
          quality_flags: [],
        })),
      } : null,
    },
    meta: {
      request_id: "0198f7a0-1111-7777-8111-111111111115",
      schema_version: "1",
      as_of: "2026-08-22T00:00:00Z",
    },
  };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  delete process.env.NEXUS_ENGINE_BASE_URL;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  delete process.env.NEXUS_ENV;
  window.history.replaceState(null, "", "/market");
});

describe("market UI to Engine boundary", () => {
  it.each([false, true])("passes a signed 500-bar envelope through the real BFF into the UI (explicit times: %s)", async (explicitTimes) => {
    process.env.NEXUS_ENGINE_BASE_URL = "http://127.0.0.1:8000";
    process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID = "test-current";
    process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET = hmacSecret;
    process.env.NEXUS_ENV = "test";
    window.history.replaceState(null, "", "/market");
    const engineRequests: Array<{ url: URL; init: RequestInit }> = [];

    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const rawUrl = String(input);
      if (rawUrl.startsWith("/api/v1/market")) {
        const browserUrl = new URL(rawUrl, "https://nexus.test");
        return getMarket(new Request(browserUrl, init));
      }
      const engineUrl = new URL(rawUrl);
      if (engineUrl.origin !== "http://127.0.0.1:8000") throw new Error("unexpected fetch target");
      engineRequests.push({ url: engineUrl, init });
      const includeSeries = engineUrl.searchParams.get("selected_instrument_id") === instrumentId;
      return Response.json(engineEnvelope(includeSeries, explicitTimes));
    }));

    const view = render(<MarketPage />);

    expect(await screen.findByRole("region", { name: "AAPL 价格图表" }, { timeout: 5_000 })).toBeVisible();
    expect(screen.getByText(/共 500 个交易日/)).toBeVisible();
    expect(screen.getByRole("region", { name: "AAPL 报价证据" })).toHaveTextContent("ALPACA");
    const quote = screen.getByRole("region", { name: "AAPL 报价证据" });
    if (explicitTimes) {
      expect(quote).toHaveTextContent("报价源时间未知");
      expect(quote).toHaveTextContent("本机接收时间2026-08-22 05:02:03 北京时间");
      expect(quote).not.toHaveTextContent("行情记录时间");
    } else {
      expect(quote).toHaveTextContent("行情记录时间2026-08-22 04:00:00 北京时间");
    }
    expect(screen.getByRole("region", { name: "AAPL 盘中证据" })).toHaveTextContent("实时 IEX（仅 IEX）");
    expect(screen.getByRole("region", { name: "AAPL OHLCV 证据" })).toHaveTextContent("POLYGON");

    const selectedRequest = engineRequests.find(({ url }) => url.searchParams.has("selected_instrument_id"));
    expect(selectedRequest).toBeDefined();
    expect(selectedRequest?.url.pathname).toBe("/internal/v1/market");
    expect(selectedRequest?.url.search).toBe(`?limit=50&selected_instrument_id=${instrumentId}`);
    const headers = new Headers(selectedRequest?.init.headers);
    const timestamp = Number(headers.get("x-nexus-timestamp"));
    const nonce = headers.get("x-nexus-nonce")!;
    const expectedHeaders = createEngineHmacHeaders({
      keyId: "test-current",
      secret: hmacSecret,
      method: "GET",
      pathAndQuery: `/internal/v1/market?limit=50&selected_instrument_id=${instrumentId}`,
      timestamp,
      nonce,
      actorId: actor.userId,
      requestId: actor.requestId,
      body: new Uint8Array(),
    });
    expect(headers.get("x-nexus-signature")).toBe(expectedHeaders["X-Nexus-Signature"]);
    view.unmount();
  });
});
