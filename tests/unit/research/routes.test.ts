import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  dataSnapshotId,
  decisionTime,
  instrumentId,
  requestId,
  researchData,
  researchAdmissionEnvelope,
  researchEnvelope,
  runId,
  sessionDate,
} from "./fixtures";

const actor = { userId: "allowed-user", requestId };
const engineFetch = vi.hoisted(() => vi.fn());

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => actor),
}));

vi.mock("../../../app/lib/engine/client", () => ({
  engineFetch,
  sanitizeEngineResponse: vi.fn(async (response: Response) => response),
}));

describe("research BFF routes", () => {
  beforeEach(() => {
    engineFetch.mockReset();
    engineFetch.mockResolvedValue(Response.json(researchEnvelope()));
  });

  it("forwards exact research bytes and rebinds the accepted Location", async () => {
    engineFetch.mockResolvedValueOnce(Response.json(researchEnvelope(), { status: 202 }));
    const { POST } = await import("../../../app/api/v1/research-runs/route");
    const body = JSON.stringify({ instrument_id: instrumentId, data_snapshot_id: dataSnapshotId, decision_time: decisionTime });
    const response = await POST(new Request("https://nexus.test/api/v1/research-runs", {
      method: "POST",
      headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "research-15" },
      body,
    }));

    const [target, init] = engineFetch.mock.calls[0];
    expect(target).toBe("/internal/v1/research-runs");
    expect(init).toMatchObject({ method: "POST", actor, headers: { "content-type": "application/json", "idempotency-key": "research-15" } });
    expect(new TextDecoder().decode(init.body)).toBe(body);
    expect(response.status).toBe(202);
    expect(response.headers.get("location")).toBe(`/api/v1/research-runs/${runId}`);
  });

  it("rejects a research admission bound to different frozen authority", async () => {
    engineFetch.mockResolvedValueOnce(Response.json(researchEnvelope(researchData({
      data_snapshot_id: "0198d600-1000-7000-8000-000000001598",
    })), { status: 202 }));
    const { POST } = await import("../../../app/api/v1/research-runs/route");
    const response = await POST(new Request("https://nexus.test/api/v1/research-runs", {
      method: "POST",
      headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "mismatch-15" },
      body: JSON.stringify({ instrument_id: instrumentId, data_snapshot_id: dataSnapshotId, decision_time: decisionTime }),
    }));
    expect(response.status).toBe(500);
  });

  it.each([
    { instrument_id: instrumentId, data_snapshot_id: dataSnapshotId, decision_time: "2026-08-24T20:00:00+00:00" },
    { instrument_id: instrumentId, data_snapshot_id: dataSnapshotId, decision_time: decisionTime, url: "https://attacker.invalid" },
    { instrument_id: "not-a-uuid", data_snapshot_id: dataSnapshotId, decision_time: decisionTime },
  ])("rejects an unsafe research body before Engine", async (value) => {
    const { POST } = await import("../../../app/api/v1/research-runs/route");
    const response = await POST(new Request("https://nexus.test/api/v1/research-runs", {
      method: "POST",
      headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "research-15" },
      body: JSON.stringify(value),
    }));
    expect(response.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("binds run, instrument/as-of and brief identities to canonical paths", async () => {
    const runRoute = await import("../../../app/api/v1/research-runs/[runId]/route");
    const instrumentRoute = await import("../../../app/api/v1/instruments/[instrumentId]/research/route");
    const briefRoute = await import("../../../app/api/v1/briefs/[sessionDate]/route");

    await runRoute.GET(new Request(`https://nexus.test/api/v1/research-runs/${runId.toUpperCase()}`));
    expect(engineFetch).toHaveBeenLastCalledWith({
      template: "/internal/v1/research-runs/{run_id}",
      path: `/internal/v1/research-runs/${runId.toUpperCase()}`,
    }, { method: "GET", actor });

    await instrumentRoute.GET(new Request(`https://nexus.test/api/v1/instruments/${instrumentId}/research?as_of=${encodeURIComponent(decisionTime)}`));
    expect(engineFetch).toHaveBeenLastCalledWith({
      template: "/internal/v1/instruments/{instrument_id}/research",
      path: `/internal/v1/instruments/${instrumentId}/research`,
      query: `as_of=${encodeURIComponent(decisionTime)}`,
    }, { method: "GET", actor });

    await briefRoute.GET(new Request(`https://nexus.test/api/v1/briefs/${sessionDate}`));
    expect(engineFetch).toHaveBeenLastCalledWith({
      template: "/internal/v1/briefs/{session_date}",
      path: `/internal/v1/briefs/${sessionDate}`,
    }, { method: "GET", actor });
  });

  it("binds research admission to the canonical instrument and as-of", async () => {
    engineFetch.mockResolvedValueOnce(Response.json(researchAdmissionEnvelope()));
    const admissionRoute = await import("../../../app/api/v1/instruments/[instrumentId]/research-admission/route");

    const response = await admissionRoute.GET(new Request(
      `https://nexus.test/api/v1/instruments/${instrumentId}/research-admission?as_of=${encodeURIComponent(decisionTime)}`,
    ));

    expect(response.status).toBe(200);
    expect(engineFetch).toHaveBeenLastCalledWith({
      template: "/internal/v1/instruments/{instrument_id}/research-admission",
      path: `/internal/v1/instruments/${instrumentId}/research-admission`,
      query: `as_of=${encodeURIComponent(decisionTime)}`,
    }, { method: "GET", actor });
  });

  it.each([
    `/api/v1/instruments/${instrumentId}/research-admission`,
    `/api/v1/instruments/${instrumentId}/research-admission?as_of=${encodeURIComponent(decisionTime)}&as_of=${encodeURIComponent(decisionTime)}`,
    `/api/v1/instruments/not-a-uuid/research-admission?as_of=${encodeURIComponent(decisionTime)}`,
  ])("rejects invalid research admission route identity %s before Engine", async (path) => {
    const admissionRoute = await import("../../../app/api/v1/instruments/[instrumentId]/research-admission/route");
    const response = await admissionRoute.GET(new Request(`https://nexus.test${path}`));
    expect(response.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it.each([
    `/api/v1/research-runs/not-a-uuid`,
    `/api/v1/instruments/${instrumentId}/research`,
    `/api/v1/instruments/${instrumentId}/research?as_of=${encodeURIComponent(decisionTime)}&as_of=${encodeURIComponent(decisionTime)}`,
    `/api/v1/instruments/${instrumentId}/research?as_of=${encodeURIComponent(decisionTime)}&redirect=bad`,
    `/api/v1/instruments/${instrumentId}/research?as_of=%ZZ`,
    "/api/v1/briefs/2026-02-30",
  ])("rejects invalid route identity %s before Engine", async (path) => {
    const route = path.includes("research-runs")
      ? await import("../../../app/api/v1/research-runs/[runId]/route")
      : path.includes("instruments")
        ? await import("../../../app/api/v1/instruments/[instrumentId]/research/route")
        : await import("../../../app/api/v1/briefs/[sessionDate]/route");
    const response = await route.GET(new Request(`https://nexus.test${path}`));
    expect(response.status).toBe(400);
    expect(engineFetch).not.toHaveBeenCalled();
  });

  it("rejects non-JSON input and an unbounded idempotency key", async () => {
    const { POST } = await import("../../../app/api/v1/research-runs/route");
    for (const request of [
      new Request("https://nexus.test/api/v1/research-runs", { method: "POST", headers: { origin: "https://nexus.test", "content-type": "text/plain", "idempotency-key": "research-15" }, body: "{}" }),
      new Request("https://nexus.test/api/v1/research-runs", { method: "POST", headers: { origin: "https://nexus.test", "content-type": "application/json", "idempotency-key": "x".repeat(256) }, body: JSON.stringify({ instrument_id: instrumentId, data_snapshot_id: dataSnapshotId, decision_time: decisionTime }) }),
    ]) {
      const response = await POST(request);
      expect(response.status).toBe(400);
      expect(engineFetch).not.toHaveBeenCalled();
    }
  });
});
