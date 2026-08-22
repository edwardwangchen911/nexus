import { describe, expect, it } from "vitest";
import { createEngineHmacHeaders } from "../../../app/lib/engine/hmac";

describe("createEngineHmacHeaders", () => {
  it("signs the shared UTF-8 golden vector with exact canonical bytes", () => {
    const headers = createEngineHmacHeaders({
      keyId: "test-current",
      secret: "nexus-test-hmac-key-32-bytes!!!!",
      method: "POST",
      pathAndQuery: "/internal/v1/research-runs?instrument_id=abc&tag=a&tag=z",
      timestamp: 1787371200,
      nonce: "0198f7a0-1111-7777-8111-111111111111",
      actorId: "nexus-test-user",
      requestId: "0198f7a0-1111-7777-8111-111111111114",
      body: new TextEncoder().encode('{"instrument_id":"abc"}'),
    });

    expect(headers).toMatchObject({
      "X-Nexus-Key-Id": "test-current",
      "X-Nexus-Actor-Id": "nexus-test-user",
      "X-Request-Id": "0198f7a0-1111-7777-8111-111111111114",
      "X-Nexus-Body-SHA256": "5d2235c37b076326bed7b94bb8315b5141c2edc8b2a74a5e737146e2c20fc117",
      "X-Nexus-Signature": "AcCgtVoeHFg46x1y6ggnVthTrQwuBH1GoyulelJ-lYo",
    });
  });

  it("normalizes repeated keys and hashes an empty GET body", () => {
    const headers = createEngineHmacHeaders({
      keyId: "test-current",
      secret: "nexus-test-hmac-key-32-bytes!!!!",
      method: "GET",
      pathAndQuery: "/internal/v1/system-status?tag=z&tag=a&instrument_id=abc",
      timestamp: 1787371200,
      nonce: "0198f7a0-1111-7777-8111-111111111112",
      actorId: "nexus-test-user",
      requestId: "0198f7a0-1111-7777-8111-111111111114",
      body: new Uint8Array(),
    });

    expect(headers["X-Nexus-Body-SHA256"]).toBe(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    );
    expect(headers["X-Nexus-Signature"]).toBe(
      "wtOOjfEZFK97l-HA0wHlkyZ9p0-rM7fL3S0NLk7t7js",
    );
  });

  it("preserves UTF-8 body bytes and encoded query ordering", () => {
    const headers = createEngineHmacHeaders({
      keyId: "test-current",
      secret: "nexus-test-hmac-key-32-bytes!!!!",
      method: "POST",
      pathAndQuery: "/internal/v1/research-runs?z=%E4%B8%AD&a=%2F&a=%20",
      timestamp: 1787371200,
      nonce: "0198f7a0-1111-7777-8111-111111111113",
      actorId: "nexus-test-user",
      requestId: "0198f7a0-1111-7777-8111-111111111114",
      body: new TextEncoder().encode('{"note":"中文"}'),
    });

    expect(headers["X-Nexus-Body-SHA256"]).toBe(
      "c7935850b16d3e9a0d211b542fcd1e181e13209ab365bb7d7e022d64f8738a8b",
    );
    expect(headers["X-Nexus-Signature"]).toBe(
      "FTX4MTbJapN28aKxVkVhDAfEVYuVRQKQl2F9wNKeQLE",
    );
  });
});
