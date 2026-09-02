import { describe, expect, it } from "vitest";

import { sanitizeEngineResponse } from "../../../app/lib/engine/client";
import { groupId, overviewData, proposalGroup } from "./fixtures";
import { portfolio, snapshot } from "../portfolio/fixtures";

const meta = { request_id: "0198d500-1000-7000-8000-000000001399", schema_version: "1", as_of: "2026-08-24T00:00:00Z" };
const shadowGroupId = "0198d500-1000-7000-8000-000000001391";

describe("optimization Engine response boundary", () => {
  it("explains missing FX valuation without forwarding upstream details", async () => {
    const response = await sanitizeEngineResponse(Response.json({ error: {
      code: "DATA_PARTIAL", message: "untrusted", request_id: meta.request_id,
      details: { reason: "DRAFT_FX_VALUATION_UNAVAILABLE", account: "must-not-escape" },
    } }, { status: 409 }), undefined, "/internal/v1/optimization-proposal-drafts", "POST");
    expect(response.status).toBe(409);
    const body = await response.json();
    expect(body.error.message).toContain("缺少汇率估值");
    expect(body.error.details).toEqual({});
    const finalResponse = await sanitizeEngineResponse(Response.json(body, { status: 409 }),
      "http://localhost/api/v1/optimization-proposal-drafts", undefined, "POST");
    expect((await finalResponse.json()).error).toEqual(body.error);
  });
  it("rejects an executable response from initial draft creation", async () => {
    const path = "/internal/v1/optimization-proposal-drafts";
    const data = proposalGroup({ status: "draft" });
    for (const proposal of data.proposals) {
      proposal.analysis_only = true;
      proposal.comparison.non_executable = true;
      proposal.comparison.metrics.ranking_eligible = false;
    }
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "POST")).status).toBe(200);
    data.proposals[1].comparison.non_executable = false;
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "POST")).status).toBe(500);
  });
  it("accepts only the closed generated three-proposal response", async () => {
    const payload = { data: proposalGroup(), meta };
    const accepted = await sanitizeEngineResponse(json(payload), undefined, `/internal/v1/optimization-proposals/${groupId}`);
    expect(accepted.status).toBe(200);
    expect(await accepted.json()).toEqual(payload);

    const nestedSecret = proposalGroup();
    Object.assign(nestedSecret.proposals[0], { credential: "must-not-escape" });
    expect((await sanitizeEngineResponse(json({ data: nestedSecret, meta }), undefined, `/internal/v1/optimization-proposals/${groupId}`)).status).toBe(500);
  });

  it("rejects duplicate or reordered variants even when every field is schema-valid", async () => {
    const reordered = proposalGroup();
    reordered.proposals = [reordered.proposals[1], reordered.proposals[0], reordered.proposals[2]];
    expect((await sanitizeEngineResponse(json({ data: reordered, meta }), undefined, `/internal/v1/optimization-proposals/${groupId}`)).status).toBe(500);

    const duplicate = proposalGroup();
    duplicate.proposals = [duplicate.proposals[0], { ...duplicate.proposals[1], proposal_id: duplicate.proposals[0].proposal_id }, duplicate.proposals[2]];
    expect((await sanitizeEngineResponse(json({ data: duplicate, meta }), undefined, `/internal/v1/optimization-proposals/${groupId}`)).status).toBe(500);
  });

  it("binds a proposal response to the requested group identity", async () => {
    const mismatched = proposalGroup({ proposal_group_id: "0198d500-1000-7000-8000-000000001397" });
    expect((await sanitizeEngineResponse(json({ data: mismatched, meta }), undefined, `/internal/v1/optimization-proposals/${groupId}`)).status).toBe(500);

    const uppercasePath = `/internal/v1/optimization-proposals/${groupId.toUpperCase()}`;
    expect((await sanitizeEngineResponse(json({ data: proposalGroup(), meta }), undefined, uppercasePath)).status).toBe(200);
  });

  it("binds distinct Shadow start and reject responses by method and UUID semantics", async () => {
    const data = shadowStartData();
    const uppercasePath = `/internal/v1/optimization-proposals/${groupId.toUpperCase()}/shadow`;
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, uppercasePath, "POST")).status).toBe(200);

    const mismatched = { ...data, proposal_group_id: "0198d500-1000-7000-8000-000000001397" };
    expect((await sanitizeEngineResponse(json({ data: mismatched, meta }), undefined, uppercasePath, "POST")).status).toBe(500);

    const lifecycle = { proposal_group_id: groupId, status: "rejected", version: 5 };
    expect((await sanitizeEngineResponse(
      json({ data: lifecycle, meta }), undefined,
      `/internal/v1/optimization-proposals/${groupId}/reject`, "POST",
    )).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: lifecycle, meta }), undefined, uppercasePath, "POST")).status).toBe(500);
  });

  it("accepts only the closed fixed-order Shadow projection on GET", async () => {
    const path = `/internal/v1/optimization-proposals/${groupId.toUpperCase()}/shadow`;
    const data = shadowGroupData();
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "GET")).status).toBe(200);

    const reordered = shadowGroupData();
    reordered.portfolios = [
      reordered.portfolios[1],
      reordered.portfolios[0],
      reordered.portfolios[2],
    ] as unknown as typeof reordered.portfolios;
    expect((await sanitizeEngineResponse(json({ data: reordered, meta }), undefined, path, "GET")).status).toBe(500);

    const malformedDecimal = shadowGroupData();
    malformedDecimal.portfolios[0].nav[0].nav = "1e2";
    expect((await sanitizeEngineResponse(json({ data: malformedDecimal, meta }), undefined, path, "GET")).status).toBe(500);

    const tooManyPoints = shadowGroupData();
    tooManyPoints.portfolios = tooManyPoints.portfolios.map((portfolio) => ({
      ...portfolio,
      nav: Array.from({ length: 64 }, (_, index) => ({
        session: `2026-10-${String(index + 1).padStart(2, "0")}`,
        nav: "100.000000000000",
      })),
    })) as typeof tooManyPoints.portfolios;
    expect((await sanitizeEngineResponse(json({ data: tooManyPoints, meta }), undefined, path, "GET")).status).toBe(500);

    const unsafe = shadowGroupData();
    unsafe.safety.creates_orders = true as false;
    expect((await sanitizeEngineResponse(json({ data: unsafe, meta }), undefined, path, "GET")).status).toBe(500);

    const nestedExtra = shadowGroupData();
    Object.assign(nestedExtra.coverage, { credential: "must-not-escape" });
    expect((await sanitizeEngineResponse(json({ data: nestedExtra, meta }), undefined, path, "GET")).status).toBe(500);
  });

  it("rejects Shadow method confusion and inconsistent seal state", async () => {
    const path = `/internal/v1/optimization-proposals/${groupId}/shadow`;
    expect((await sanitizeEngineResponse(json({ data: shadowStartData(), meta }), undefined, path, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: shadowGroupData(), meta }), undefined, path, "POST")).status).toBe(500);

    const badSeal = shadowGroupData();
    badSeal.sealed = true;
    badSeal.seal_reason = "validated";
    expect((await sanitizeEngineResponse(json({ data: badSeal, meta }), undefined, path, "GET")).status).toBe(500);
  });

  it("accepts persisted checkpoint metrics and rejects known/unknown confusion", async () => {
    const path = `/internal/v1/optimization-proposals/${groupId}/shadow`;
    const data = shadowGroupData();
    const sessions = ["2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31"];
    data.coverage.completed = 5;
    data.portfolios.forEach((portfolio) => {
      portfolio.nav = sessions.map((session, index) => ({
        session,
        nav: `${100 + index}.000000000000`,
      }));
    });
    data.checkpoints.push(shadowCheckpoint());
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "GET")).status).toBe(200);

    const confused = structuredClone(data);
    confused.checkpoints[0].status = "incomplete";
    confused.checkpoints[0].reason = "SHADOW_DATA_INCOMPLETE";
    expect((await sanitizeEngineResponse(json({ data: confused, meta }), undefined, path, "GET")).status).toBe(500);

    const malformedUuid = structuredClone(data);
    malformedUuid.portfolios[0].shadow_portfolio_id = "not-a-uuid";
    expect((await sanitizeEngineResponse(json({ data: malformedUuid, meta }), undefined, path, "GET")).status).toBe(500);

    const malformedDate = structuredClone(data);
    malformedDate.portfolios[0].nav[0].session = "2026-02-30";
    expect((await sanitizeEngineResponse(json({ data: malformedDate, meta }), undefined, path, "GET")).status).toBe(500);
  });

  it("keeps Shadow not-started errors stable while removing upstream details", async () => {
    const path = `/internal/v1/optimization-proposals/${groupId}/shadow`;
    const upstream = new Response(JSON.stringify({
      error: {
        code: "NOT_FOUND",
        message: "SHADOW_NOT_STARTED at shadow_groups from provider credential",
        request_id: meta.request_id,
        details: { reason: "SHADOW_NOT_STARTED", table: "shadow_groups" },
      },
    }), { status: 404, headers: { "content-type": "application/json" } });
    const response = await sanitizeEngineResponse(upstream, undefined, path, "GET");
    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({
      error: {
        code: "NOT_FOUND",
        message: "The requested resource was not found.",
        request_id: meta.request_id,
        details: {},
      },
    });
  });

  it("binds overview admission and diagnosis to the returned snapshot", async () => {
    const payload = { data: overviewData(), meta };
    expect((await sanitizeEngineResponse(json(payload), undefined, "/internal/v1/overview")).status).toBe(200);

    const mismatch = overviewData({ portfolio_snapshot_id: "0198d500-1000-7000-8000-000000001397" });
    expect((await sanitizeEngineResponse(json({ data: mismatch, meta }), undefined, "/internal/v1/overview")).status).toBe(500);
  });

  it("accepts one closed facts-only reason and rejects snapshot-state confusion", async () => {
    const facts = {
      overview_mode: "PORTFOLIO_FACTS_ONLY",
      portfolio_id: portfolio.portfolio_id,
      portfolio_name: portfolio.name,
      provider: "fosun",
      portfolio_snapshot_id: snapshot.portfolio_snapshot_id,
      portfolio_snapshot: { ...snapshot, status: "PARTIAL" },
      snapshot_as_of: snapshot.as_of,
      trading_day: null,
      diagnostic_report: null,
      diagnostic_unavailable_reason: "SNAPSHOT_NOT_READY",
      proposal_group: null,
      proposal_admission: null,
      proposal_admission_reason: null,
    };

    expect((await sanitizeEngineResponse(json({ data: facts, meta }), undefined, "/internal/v1/overview")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { ...facts, diagnostic_unavailable_reason: "DIAGNOSTIC_NOT_AVAILABLE" }, meta }), undefined, "/internal/v1/overview")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { ...facts, portfolio_snapshot: { ...snapshot, status: "READY" } }, meta }), undefined, "/internal/v1/overview")).status).toBe(500);
  });
});

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { status: 200, headers: { "content-type": "application/json" } });
}

function shadowStartData() {
  return {
    proposal_group_id: groupId,
    shadow_group_id: shadowGroupId,
    status: "shadowing" as const,
    version: 5,
    start_session: "2026-08-25",
    evaluation_end_session: "2026-11-20",
    grace_end_session: "2026-11-30",
  };
}

function shadowGroupData() {
  const variants = ["hold_current", "minimum_change", "strategy_target"] as const;
  return {
    proposal_group_id: groupId,
    shadow_group_id: shadowGroupId,
    proposal_status: "shadowing" as const,
    version: 5,
    start_session: "2026-08-25",
    evaluation_end_session: "2026-11-20",
    grace_end_session: "2026-11-30",
    coverage: {
      completed: 1,
      due: 1,
      required: 63 as const,
      next_expected_session: "2026-08-26",
      missing_sessions: [],
    },
    freshness: {
      status: "current" as const,
      latest_session: "2026-08-25",
      available_at: "2026-08-25T21:30:00Z",
      last_attempt_at: "2026-08-25T21:31:00Z",
      database_time: "2026-08-25T21:31:01Z",
      reason: null,
    },
    sources: [{
      source: "alpaca",
      kind: "price_bars" as const,
      delivery_mode: "delayed" as const,
      latest_session: "2026-08-25",
      available_at: "2026-08-25T21:30:00Z",
    }],
    portfolios: variants.map((variant, index) => ({
      shadow_portfolio_id: `0198d500-1000-7000-8000-00000000139${index + 2}`,
      variant,
      nav: [{ session: "2026-08-25", nav: "100.250000000000" }],
      initial_outcomes: [],
    })) as [{
      shadow_portfolio_id: string;
      variant: "hold_current";
      nav: Array<{ session: string; nav: string }>;
      initial_outcomes: never[];
    }, {
      shadow_portfolio_id: string;
      variant: "minimum_change";
      nav: Array<{ session: string; nav: string }>;
      initial_outcomes: never[];
    }, {
      shadow_portfolio_id: string;
      variant: "strategy_target";
      nav: Array<{ session: string; nav: string }>;
      initial_outcomes: never[];
    }],
    checkpoints: [] as Array<ReturnType<typeof shadowCheckpoint>>,
    last_failed_attempt: null,
    sealed: false,
    seal_reason: null as "validated" | "rejected" | "expired" | null,
    safety: { creates_orders: false as const, trading_approval: false as const },
  };
}

function shadowCheckpoint() {
  const known = (value: string) => ({ status: "known" as const, value, reason: null });
  const metrics = {
    net_cumulative_return: known("0.040000000000"),
    return_delta_vs_hold: known("0.000000000000"),
    max_drawdown: known("-0.010000000000"),
    drawdown_improvement: known("0.000000000000"),
    annualized_volatility: known("0.120000000000"),
    volatility_improvement: known("0.000000000000"),
    worst_sessions: [{ session: "2026-08-26", return_value: "-0.010000000000" }],
    cumulative_turnover: known("0.100000000000"),
    fee_drag_index_points: known("0.010000000000"),
  };
  return {
    ordinal: 5,
    session: "2026-08-31",
    status: "complete" as "complete" | "incomplete",
    coverage_count: 5,
    reason: null as string | null,
    metrics: (["hold_current", "minimum_change", "strategy_target"] as const).map(
      (variant) => ({ variant, metrics }),
    ),
  };
}
