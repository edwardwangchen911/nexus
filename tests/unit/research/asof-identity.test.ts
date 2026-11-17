import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { GET as getResearch } from "../../../app/api/v1/instruments/[instrumentId]/research/route";
import { GET as getAdmission } from "../../../app/api/v1/instruments/[instrumentId]/research-admission/route";
import { instrumentId, requestId, researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope } from "./fixtures";

vi.mock("../../../app/lib/auth/nexus-user", () => ({
  NexusApiAuthError: class NexusApiAuthError extends Error {},
  requireNexusApiActor: vi.fn(async () => ({ userId: "allowed-user", requestId })),
}));

const second = "2026-08-24T20:00:00";
const routes = [
  { kind: "research", get: getResearch },
  { kind: "research-admission", get: getAdmission },
] as const;

function envelope(kind: typeof routes[number]["kind"], asOf: string, decision: string) {
  const research = researchData({ decision_time: decision });
  research.intent = { ...research.intent!, decision_time: decision };
  const payload = kind === "research"
    ? researchEnvelope(research)
    : researchAdmissionEnvelope(researchAdmissionData({ decision_time: decision }));
  payload.meta.as_of = asOf;
  return payload;
}

beforeEach(() => {
  vi.stubEnv("NEXUS_ENV", "test");
  vi.stubEnv("NEXUS_ENGINE_BASE_URL", "http://127.0.0.1:8000");
  vi.stubEnv("NEXUS_ENGINE_HMAC_CURRENT_KEY_ID", "test-current");
  vi.stubEnv("NEXUS_ENGINE_HMAC_CURRENT_SECRET", "nexus-test-hmac-key-32-bytes!!!!");
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe.each(routes)("$kind exact as-of identity through both response boundaries", ({ kind, get }) => {
  it.each([
    ["Z", ".000000Z"],
    [".1Z", ".100000Z"],
    [".123Z", ".123000Z"],
    [".123000Z", ".123Z"],
    [".1230000Z", ".123000Z"],
    [".123456Z", ".123456Z"],
  ])("accepts equivalent request %s and response %s without rewriting either", async (requestFraction, responseFraction) => {
    const requested = second + requestFraction;
    const payload = envelope(kind, second + responseFraction, second + responseFraction);
    const upstream = vi.fn<typeof fetch>(async () => Response.json(payload));
    vi.stubGlobal("fetch", upstream);

    const response = await get(new Request(`https://nexus.test/api/v1/instruments/${instrumentId}/${kind}?as_of=${encodeURIComponent(requested)}`));

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(payload);
    expect(upstream).toHaveBeenCalledOnce();
    expect(String(upstream.mock.calls[0][0])).toBe(`http://127.0.0.1:8000/internal/v1/instruments/${instrumentId}/${kind}?as_of=${encodeURIComponent(requested)}`);
  });

  it.each([".123455Z", ".123457Z"])("rejects response meta displaced by one microsecond: %s", async (fraction) => {
    vi.stubGlobal("fetch", async () => Response.json(envelope(kind, second + fraction, second + ".123456Z")));

    const response = await get(new Request(`https://nexus.test/api/v1/instruments/${instrumentId}/${kind}?as_of=${encodeURIComponent(second + ".123456Z")}`));

    expect(response.status).toBe(500);
    expect((await response.json()).error.code).toBe("INTERNAL_ERROR");
  });

  it.each(["?as_of=2026-02-30T20%3A00%3A00Z", "?as_of=2026-08-24T20%3A00%3A00%2B00%3A00", `?as_of=${second}Z&as_of=${second}Z`])("rejects invalid or repeated query before transport: %s", async (query) => {
    const upstream = vi.fn();
    vi.stubGlobal("fetch", upstream);

    const response = await get(new Request(`https://nexus.test/api/v1/instruments/${instrumentId}/${kind}${query}`));

    expect(response.status).toBe(400);
    expect(upstream).not.toHaveBeenCalled();
  });
});

describe("research decision cutoff retains sub-millisecond precision", () => {
  it.each([
    ["2026-08-24T19:59:59.999999Z", 200],
    ["2026-08-24T20:00:01Z", 500],
  ])("orders adjacent whole seconds exactly: %s", async (decision, status) => {
    const requested = second + "Z";
    vi.stubGlobal("fetch", async () => Response.json(envelope("research", requested, decision)));

    const response = await getResearch(new Request(`https://nexus.test/api/v1/instruments/${instrumentId}/research?as_of=${encodeURIComponent(requested)}`));

    expect(response.status).toBe(status);
  });

  it.each([
    [".123455Z", 200],
    [".123456Z", 200],
    [".123457Z", 500],
    [".1234561Z", 500],
  ])("handles research decision %s against .123456Z with status %s", async (fraction, status) => {
    const requested = second + ".123456Z";
    vi.stubGlobal("fetch", async () => Response.json(envelope("research", requested, second + fraction)));

    const response = await getResearch(new Request(`https://nexus.test/api/v1/instruments/${instrumentId}/research?as_of=${encodeURIComponent(requested)}`));

    expect(response.status).toBe(status);
  });

  it.each([".123455Z", ".123457Z"])("rejects admission decision displaced by one microsecond: %s", async (fraction) => {
    const requested = second + ".123456Z";
    vi.stubGlobal("fetch", async () => Response.json(envelope("research-admission", requested, second + fraction)));

    const response = await getAdmission(new Request(`https://nexus.test/api/v1/instruments/${instrumentId}/research-admission?as_of=${encodeURIComponent(requested)}`));

    expect(response.status).toBe(500);
  });
});
