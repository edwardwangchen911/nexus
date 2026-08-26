import { afterEach, describe, expect, it, vi } from "vitest";
import { createServer } from "node:http";
import { gzipSync, gunzipSync } from "node:zlib";

import {
  engineFetch,
  sanitizeAuditExportResponse,
  sanitizeEngineResponse,
} from "../../../app/lib/engine/client";

const requestId = "0198f7a0-1111-7777-8111-111111111111";
const jobId = "0198f7a0-1111-7777-8111-111111111112";
const meta = { request_id: requestId, schema_version: "1", as_of: "2026-08-27T00:00:00Z" };
const actor = { userId: "allowed-user", requestId };

afterEach(() => {
  vi.unstubAllGlobals();
  delete process.env.NEXUS_ENGINE_BASE_URL;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  delete process.env.NEXUS_ENV;
});

describe("risk-audit Engine response boundary", () => {
  it("accepts the closed readiness projection at 200 and 503 only", async () => {
    const data = {
      status: "ready",
      components: { database: "ready", migration: "ready", object_store: "ready", worker: "ready" },
    };
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, "/health/ready", "GET")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { ...data, status: "unready" }, meta }, 503), undefined, "/health/ready", "GET")).status).toBe(503);
    expect((await sanitizeEngineResponse(json({ data: { ...data, broker: "ready" }, meta }, 503), undefined, "/health/ready", "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { ...data, status: "unready" }, meta }, 503), undefined, "/internal/v1/system-status", "GET")).status).toBe(500);
  });

  it("accepts a bounded redacted page and rejects nested extras", async () => {
    const data = auditPage();
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, "/internal/v1/risk-audit", "GET")).status).toBe(200);

    const leaked = structuredClone(data);
    Object.assign(leaked.items[0], { payload: { account_id: "must-not-escape" } });
    expect((await sanitizeEngineResponse(json({ data: leaked, meta }), undefined, "/internal/v1/risk-audit", "GET")).status).toBe(500);

    const leakedOutcome = structuredClone(data);
    Object.assign(leakedOutcome.items[0].outcome, { account_id: "must-not-escape" });
    expect((await sanitizeEngineResponse(json({ data: leakedOutcome, meta }), undefined, "/internal/v1/risk-audit", "GET")).status).toBe(500);

    const tooMany = { ...data, items: Array.from({ length: 101 }, () => data.items[0]) };
    expect((await sanitizeEngineResponse(json({ data: tooMany, meta }), undefined, "/internal/v1/risk-audit", "GET")).status).toBe(500);

    const tooManyJobs = structuredClone(data);
    tooManyJobs.summary.failed_jobs.retryable = Array.from({ length: 21 }, () => data.summary.failed_jobs.retryable[0]);
    expect((await sanitizeEngineResponse(json({ data: tooManyJobs, meta }), undefined, "/internal/v1/risk-audit", "GET")).status).toBe(500);
  });

  it("binds recovery results to the requested job, method, and strict schemas", async () => {
    const retry = { job_id: jobId, status: "queued", version: 8, manual_retry_count: 1, replayed: false };
    const retryPath = `/internal/v1/jobs/${jobId}/retry`;
    expect((await sanitizeEngineResponse(json({ data: retry, meta }), undefined, retryPath, "POST")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { ...retry, job_id: requestId }, meta }), undefined, retryPath, "POST")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: retry, meta }), undefined, retryPath, "GET")).status).toBe(500);

    const acknowledge = { state: "unlocked", event_seq: 13, replayed: false };
    const acknowledgePath = "/internal/v1/risk/drawdown-lock/acknowledge";
    expect((await sanitizeEngineResponse(json({ data: acknowledge, meta }), undefined, acknowledgePath, "POST")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { ...acknowledge, order_created: true }, meta }), undefined, acknowledgePath, "POST")).status).toBe(500);
  });

  it("allows only canonical risk-audit targets and signed recovery paths", async () => {
    setEngineEnvironment();
    const calls: Array<{ url: string; init: RequestInit }> = [];
    vi.stubGlobal("fetch", async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: input.toString(), init: init ?? {} });
      return json({ data: auditPage(), meta });
    });

    const response = await engineFetch({
      template: "/internal/v1/risk-audit",
      path: "/internal/v1/risk-audit",
      query: "to=2026-08-27T00%3A00%3A00Z&limit=25&kind=risk&from=2026-08-20T00%3A00%3A00Z",
    } as never, { method: "GET", actor } as never);
    expect(response.status).toBe(200);
    expect(calls[0].url).toBe("http://127.0.0.1:8000/internal/v1/risk-audit?from=2026-08-20T00%3A00%3A00Z&kind=risk&limit=25&to=2026-08-27T00%3A00%3A00Z");

    calls.length = 0;
    const unsafeFetch = engineFetch as unknown as (target: unknown, init: { method: string; actor: typeof actor }) => Promise<Response>;
    for (const target of [
      { template: "/internal/v1/risk-audit", path: "/internal/v1/risk-audit", query: "account_id=secret" },
      { template: "/internal/v1/risk-audit", path: "/internal/v1/risk-audit", query: "kind=risk&kind=job" },
      { template: "/internal/v1/jobs/{job_id}/retry", path: "/internal/v1/jobs/not-a-uuid/retry" },
    ]) expect((await unsafeFetch(target, { method: "GET", actor })).status).toBe(500);
    expect(calls).toHaveLength(0);
  });

  it("passes only bounded gzip NDJSON with a fixed safe filename", async () => {
    const gzip = gzipSync(`${JSON.stringify(auditPage().items[0])}\n`);
    const safe = await sanitizeAuditExportResponse(new Response(gzip, {
      status: 200,
      headers: {
        "content-type": "application/x-ndjson",
        "content-encoding": "gzip",
        "content-disposition": "attachment; filename=../../secret.txt",
      },
    }));
    expect(safe.status).toBe(200);
    expect(safe.headers.get("content-disposition")).toBe('attachment; filename="nexus-risk-audit.ndjson.gz"');
    expect(new Uint8Array(await safe.arrayBuffer()).slice(0, 2)).toEqual(new Uint8Array([0x1f, 0x8b]));

    for (const unsafe of [
      new Response(gzip, { headers: { "content-type": "text/plain", "content-encoding": "gzip" } }),
      new Response(gzip, { headers: { "content-type": "application/x-ndjson" } }),
      new Response(new Uint8Array([0x7b, 0x7d]), { headers: { "content-type": "application/x-ndjson", "content-encoding": "gzip" } }),
    ]) expect((await sanitizeAuditExportResponse(unsafe)).status).toBe(500);
  });

  it("normalizes Node fetch auto-decompression without weakening export redaction", async () => {
    const ndjson = `${JSON.stringify(auditPage().items[0])}\n`;
    const compressed = gzipSync(ndjson);
    const server = createServer((_request, response) => {
      response.writeHead(200, {
        "content-encoding": "gzip",
        "content-length": String(compressed.byteLength),
        "content-type": "application/x-ndjson",
      });
      response.end(compressed);
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    try {
      const address = server.address();
      if (address === null || typeof address === "string") throw new Error("test server did not bind");
      const upstream = await fetch(`http://127.0.0.1:${address.port}/audit`);
      expect(new Uint8Array(await upstream.clone().arrayBuffer()).slice(0, 2)).not.toEqual(new Uint8Array([0x1f, 0x8b]));

      const safe = await sanitizeAuditExportResponse(upstream);
      expect(safe.status).toBe(200);
      expect(gunzipSync(Buffer.from(await safe.arrayBuffer())).toString()).toBe(ndjson);
    } finally {
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }

    const leaked = `${JSON.stringify({ ...auditPage().items[0], outcome: { account_id: "must-not-escape" } })}\n`;
    const rejected = await sanitizeAuditExportResponse(new Response(leaked, {
      headers: { "content-type": "application/x-ndjson", "content-encoding": "gzip" },
    }));
    expect(rejected.status).toBe(500);
  });
});

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}

function auditPage() {
  return {
    summary: {
      data_quality: "PASS" as const,
      drawdown_lock: { state: "unlocked" as const, event_seq: 12, acknowledge_allowed: false },
      failed_jobs: {
        count: 1,
        retryable: [{
          job_id: jobId,
          job_type: "daily_pipeline",
          status: "failed" as const,
          version: 7,
          manual_retry_count: 0,
          last_error_code: "PROVIDER_UNAVAILABLE",
          finished_at: "2026-08-26T23:30:00Z",
        }],
      },
    },
    items: [{
      event_seq: 9,
      occurred_at: "2026-08-26T23:30:00Z",
      kind: "job" as const,
      actor_kind: "system" as const,
      action: "job.failed",
      schema_version: "1",
      outcome: { status: "failed", retryable: true, error_code: "PROVIDER_UNAVAILABLE" },
      previous_hash: "a".repeat(64),
      event_hash: "b".repeat(64),
    }],
    next_cursor: "signed-next",
  };
}

function setEngineEnvironment(): void {
  process.env.NEXUS_ENGINE_BASE_URL = "http://127.0.0.1:8000";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID = "test-current";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET = "nexus-test-hmac-key-32-bytes!!!!";
  process.env.NEXUS_ENV = "test";
}
