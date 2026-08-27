import { describe, expect, it } from "vitest";

import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { detailEnvelope, experimentId } from "../strategy-evolution/fixtures";

const meta = {
  request_id: "0198f7a0-1111-7777-8111-111111111114",
  schema_version: "1",
  as_of: "2026-08-26T00:00:00Z",
};
const baseline = {
  strategy_version: "baseline_v1",
  champion: true,
  evolution_status: "locked",
  rebalance_frequency: "weekly",
  historical_agent_overlay: false,
  live_execution_enabled: false,
  latest_backtest: null,
};
const fixtureSources = [
  "price:fixture:test:fixture",
  "universe:fixture",
  "sector:fixture",
  "corporate_action:fixture",
  "calendar:exchange_calendars",
];

function json(data: unknown): Response {
  return Response.json({ data, meta });
}

describe("strategy runtime boundary", () => {
  it("requires separate bounded Shadow evaluation and grace progress", async () => {
    const accepted = await sanitizeEngineResponse(
      Response.json(detailEnvelope()),
      undefined,
      `/internal/v1/strategy-experiments/${experimentId}`,
      "GET",
    );
    expect(accepted.status).toBe(200);

    for (const field of ["evaluation_sessions", "grace_sessions", "grace_elapsed_sessions"] as const) {
      const envelope = detailEnvelope();
      delete (envelope.data.shadow as Partial<typeof envelope.data.shadow>)[field];
      const rejected = await sanitizeEngineResponse(
        Response.json(envelope),
        undefined,
        `/internal/v1/strategy-experiments/${experimentId}`,
        "GET",
      );
      expect(rejected.status).toBe(500);
    }

    const overflow = detailEnvelope();
    overflow.data.shadow.completed_sessions = 64;
    const rejected = await sanitizeEngineResponse(
      Response.json(overflow),
      undefined,
      `/internal/v1/strategy-experiments/${experimentId}`,
      "GET",
    );
    expect(rejected.status).toBe(500);
  });

  it("accepts only one locked, Agent-free baseline champion", async () => {
    const response = await sanitizeEngineResponse(
      json([baseline]),
      undefined,
      "/internal/v1/strategies",
    );

    expect(response.status).toBe(200);
  });

  it("fails closed on extra fields, live execution, or duplicate champions", async () => {
    const poisoned = { ...baseline, api_key: "must-not-cross" };
    const live = { ...baseline, live_execution_enabled: true };
    const duplicate = [baseline, { ...baseline }];

    for (const data of [[poisoned], [live], duplicate]) {
      const response = await sanitizeEngineResponse(
        json(data),
        undefined,
        "/internal/v1/strategies",
      );
      expect(response.status).toBe(500);
    }
  });

  it("fails closed on malformed or internally inconsistent backtest evidence", async () => {
    const evidence = {
      result_hash: "a".repeat(64),
      data_hash: "b".repeat(64),
      code_hash: "c".repeat(64),
      start_session: "2018-01-02",
      end_session: "2026-08-25",
      oos_window_count: 8,
      comparison_keys: ["spy", "equal_weight", "champion"],
      benchmark_comparison_complete: true,
      data_mode: "fixture",
      data_sources: fixtureSources,
      completed_at: "2026-08-26T00:00:00Z",
    };
    const poisoned = [
      { ...evidence, result_hash: "not-a-hash" },
      { ...evidence, oos_window_count: -1 },
      { ...evidence, start_session: "2027-01-01" },
      { ...evidence, comparison_keys: ["spy", "spy", "champion"] },
      { ...evidence, benchmark_comparison_complete: false },
    ];

    for (const latest_backtest of poisoned) {
      const response = await sanitizeEngineResponse(
        json([{ ...baseline, latest_backtest }]),
        undefined,
        "/internal/v1/strategies",
      );
      expect(response.status).toBe(500);
    }
  });

  it("fails closed when evidence omits provenance mode or source domains", async () => {
    const evidence = {
      result_hash: "a".repeat(64),
      data_hash: "b".repeat(64),
      code_hash: "c".repeat(64),
      start_session: "2018-01-02",
      end_session: "2026-08-25",
      oos_window_count: 8,
      comparison_keys: ["spy", "equal_weight", "champion"],
      benchmark_comparison_complete: true,
      data_mode: "fixture",
      data_sources: fixtureSources,
      completed_at: "2026-08-26T00:00:00Z",
    };
    const missingMode = { ...evidence, data_mode: undefined };
    const missingSources = { ...evidence, data_sources: undefined };

    for (const latest_backtest of [missingMode, missingSources]) {
      const response = await sanitizeEngineResponse(
        json([{ ...baseline, latest_backtest }]),
        undefined,
        "/internal/v1/strategies",
      );
      expect(response.status).toBe(500);
    }
  });

  it("fails closed on incomplete, duplicate, or mode-inconsistent provenance", async () => {
    const evidence = {
      result_hash: "a".repeat(64),
      data_hash: "b".repeat(64),
      code_hash: "c".repeat(64),
      start_session: "2018-01-02",
      end_session: "2026-08-25",
      oos_window_count: 8,
      comparison_keys: ["spy", "equal_weight", "champion"],
      benchmark_comparison_complete: true,
      data_mode: "fixture",
      data_sources: fixtureSources,
      completed_at: "2026-08-26T00:00:00Z",
    };
    const invalid = [
      { ...evidence, data_sources: [] },
      { ...evidence, data_sources: [...fixtureSources, fixtureSources[0]] },
      {
        ...evidence,
        data_sources: [
          "price:alpaca:sip:real_time",
          "universe:alpaca",
          "sector:controlled_csv_v1",
          "corporate_action:alpaca",
          "calendar:exchange_calendars",
        ],
      },
    ];

    for (const latest_backtest of invalid) {
      const response = await sanitizeEngineResponse(
        json([{ ...baseline, latest_backtest }]),
        undefined,
        "/internal/v1/strategies",
      );
      expect(response.status).toBe(500);
    }
  });
});
