import { beforeEach, describe, expect, it, vi } from "vitest";
import { sanitizeEngineResponse, timeoutForMethod } from "../../../app/lib/engine/client";
import { disclosureMonitoring, disclosureTrigger } from "./disclosure-monitoring-fixture";
import { researchData, researchEnvelope, runId, instrumentId, requestId } from "./fixtures";

const engineFetch = vi.hoisted(() => vi.fn());
vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => ({ userId: "allowed-user", requestId })),
}));
vi.mock("../../../app/lib/engine/client", async (importOriginal) => ({
  ...await importOriginal<typeof import("../../../app/lib/engine/client")>(), engineFetch,
}));
beforeEach(() => engineFetch.mockReset());

function envelope(disclosure: unknown) { return researchEnvelope(researchData({ disclosure_monitoring: disclosure })); }
async function sanitize(disclosure: unknown) {
  return sanitizeEngineResponse(Response.json(envelope(disclosure)), undefined, `/internal/v1/research-runs/${runId}`);
}

describe("SEC monitoring closed BFF contract", () => {
  it.each(["events_unresolved", "review_unverified", "replacement_rejected"])("preserves %s through the real sanitizer", async (coverage_reason) => {
    const monitoring = disclosureMonitoring({ coverage_reason, state: "withdrawn", condition: null });
    const response = await sanitize(monitoring);
    expect(response.status).toBe(200);
    expect((await response.json()).data.disclosure_monitoring).toEqual(monitoring);
  });
  it.each(["addressed", "unresolved", "unverified", "not_performed"])("preserves typed %s without rewriting original run identity or unknown usage", async (event_review) => {
    const monitoring = disclosureMonitoring({ triggers: [disclosureTrigger({ event_review, status: "unknown", usage_status: "unknown" })] });
    const response = await sanitize(monitoring);
    expect(response.status).toBe(200);
    const data = (await response.json()).data;
    expect(data.run_id).toBe(runId); expect(data.instrument_id).toBe(instrumentId);
    expect(data.disclosure_monitoring).toEqual(monitoring);
  });
  it.each(["extra-view", "extra-trigger", "too-many", "unlocked", "fake-delivery", "fake-review", "other-forms"])("rejects %s without leaking untrusted material", async (mutation) => {
    const monitoring = disclosureMonitoring();
    if (mutation === "extra-view") Object.assign(monitoring, { account_id: "must-not-escape" });
    if (mutation === "extra-trigger") Object.assign(monitoring.triggers[0], { credential: "must-not-escape" });
    if (mutation === "too-many") monitoring.triggers = Array.from({ length: 6 }, () => disclosureTrigger());
    if (mutation === "unlocked") monitoring.execution_locked = false;
    if (mutation === "fake-delivery") monitoring.triggers[0].notification_status = "device_delivered";
    if (mutation === "fake-review") monitoring.triggers[0].event_review = "resolved_by_assumption";
    if (mutation === "other-forms") monitoring.condition.forms = ["10-K"];
    const response = await sanitize(monitoring);
    expect(response.status).toBe(500); expect(await response.text()).not.toContain("must-not-escape");
  });
  it("keeps old payloads and explicit null compatible", async () => {
    expect((await sanitizeEngineResponse(Response.json(researchEnvelope()), undefined, `/internal/v1/research-runs/${runId}`)).status).toBe(200);
    expect((await sanitize(null)).status).toBe(200);
  });
  it("passes the exact selected run through one GET with the unchanged ten-second deadline", async () => {
    const monitoring = disclosureMonitoring();
    engineFetch.mockResolvedValueOnce(Response.json(envelope(monitoring)));
    const { GET } = await import("../../../app/api/v1/research-runs/[runId]/route");
    const response = await GET(new Request(`https://nexus.test/api/v1/research-runs/${runId}`));
    expect(response.status).toBe(200);
    expect((await response.json()).data.disclosure_monitoring).toEqual(monitoring);
    expect(engineFetch).toHaveBeenCalledTimes(1);
    expect(engineFetch).toHaveBeenCalledWith({ template: "/internal/v1/research-runs/{run_id}", path: `/internal/v1/research-runs/${runId}` }, { method: "GET", actor: { userId: "allowed-user", requestId } });
    expect(timeoutForMethod("GET")).toBe(10_000);
  });
});
