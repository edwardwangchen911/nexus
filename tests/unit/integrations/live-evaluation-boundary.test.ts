import { describe, expect, it } from "vitest";

import { sanitizeEngineResponse } from "../../../app/lib/engine/client";

const path = "/internal/v1/live-evaluation";
const meta = {
  request_id: "0198f7a0-1111-7777-8111-111111111114",
  schema_version: "1",
  as_of: "2026-09-17T01:00:00Z",
};

describe("live-evaluation Engine boundary", () => {
  it("accepts only a locked response with all five unique gates", async () => {
    expect((await sanitizeEngineResponse(json(envelope()), undefined, path, "GET")).status).toBe(200);

    for (const mutate of [
      (data: Record<string, unknown>) => { data.execution_locked = false; },
      (data: Record<string, unknown>) => { data.live_write = true; },
      (data: Record<string, unknown>) => {
        const gates = data.gates as Array<Record<string, unknown>>;
        gates[4].key = gates[0].key;
      },
      (data: Record<string, unknown>) => { (data.gates as unknown[]).pop(); },
    ]) {
      const unsafe = structuredClone(envelope());
      mutate(unsafe.data as unknown as Record<string, unknown>);
      await expectSafeFailure(await sanitizeEngineResponse(json(unsafe), undefined, path, "GET"));
    }
  });

  it("rejects the response on any other method", async () => {
    await expectSafeFailure(await sanitizeEngineResponse(json(envelope()), undefined, path, "POST"));
  });
});

function envelope() {
  return {
    data: {
      status: "not_ready" as const,
      execution_locked: true as const,
      live_write: false as const,
      user_decision_required: true as const,
      gates: [
        gate("internal_paper_execution"),
        gate("oos_agent_version"),
        gate("broker_simulation_lifecycle"),
        { ...gate("broker_fee_variance"), status: "unknown" as const },
        gate("broker_recovery_drill"),
      ],
    },
    meta,
  };
}

function gate(key: "internal_paper_execution" | "oos_agent_version" | "broker_simulation_lifecycle" | "broker_fee_variance" | "broker_recovery_drill") {
  return {
    key,
    status: "not_ready" as const,
    reason_code: "EVIDENCE_MISSING",
    evidence: [{ key: "count", count: 0 }],
  };
}

function json(value: unknown): Response {
  return Response.json(value);
}

async function expectSafeFailure(response: Response): Promise<void> {
  expect(response.status).toBe(500);
  expect(await response.json()).toMatchObject({ error: { code: "INTERNAL_ERROR", details: {} } });
}
