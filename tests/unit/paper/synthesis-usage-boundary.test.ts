import { describe, expect, it } from "vitest";
import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { meta } from "./fixtures";
import { synthesisUsageDetail } from "./synthesis-usage-fixture";

const path = `/internal/v1/paper/management/cycles/${synthesisUsageDetail.cycle.cycle_id}`;
const status = async (data: unknown) => (await sanitizeEngineResponse(new Response(JSON.stringify({ data, meta: meta() }), { headers: { "Content-Type": "application/json" } }), undefined, path, "GET")).status;

describe("synthesis usage read boundary", () => {
  it.each(["not_accepted", "accepted", "invalid"])("accepts bounded %s historical disposition", async (value) => {
    const detail = structuredClone(synthesisUsageDetail);
    Object.assign(detail.synthesis_usage_disposition!, { status: value });
    expect(await status(detail)).toBe(200);
  });
  it.each([
    { execution_allowed: true }, { budget_preserved: false }, { usage: "known" },
    { status: "success" }, { account_id: "private-canary" },
  ])("rejects unsafe or unexpected disposition fields %j", async (mutation) => {
    const detail = structuredClone(synthesisUsageDetail);
    Object.assign(detail.synthesis_usage_disposition!, mutation);
    expect(await status(detail)).toBe(500);
  });
  it("rejects a completed cycle presented with failed usage disposition", async () => {
    const detail = structuredClone(synthesisUsageDetail);
    detail.cycle.status = "completed";
    expect(await status(detail)).toBe(500);
  });
  it("also accepts a failed cycle with unknown failed synthesis", async () => {
    const detail = structuredClone(synthesisUsageDetail);
    detail.cycle.status = "failed";
    expect(await status(detail)).toBe(200);
  });
  it("preserves older responses without disposition", async () => {
    const detail = structuredClone(synthesisUsageDetail);
    delete detail.synthesis_usage_disposition;
    expect(await status(detail)).toBe(200);
  });
});
