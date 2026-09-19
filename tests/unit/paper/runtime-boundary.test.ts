import { afterEach, describe, expect, it, vi } from "vitest";

import { engineFetch, sanitizeEngineResponse } from "../../../app/lib/engine/client";

const requestId = "0198f7a0-1111-7777-8111-111111111111";
const portfolioId = "0198f7a0-1111-7777-8111-111111111112";
const orderId = "0198f7a0-1111-7777-8111-111111111113";
const instrumentId = "0198f7a0-1111-7777-8111-111111111114";
const planId = "0198f7a0-1111-7777-8111-111111111115";
const targetId = "0198f7a0-1111-7777-8111-111111111116";
const synthesisId = "0198f7a0-1111-7777-8111-111111111117";
const candidateBatchId = "0198f7a0-1111-7777-8111-111111111118";
const secondInstrumentId = "0198f7a0-1111-7777-8111-111111111140";
const thirdInstrumentId = "0198f7a0-1111-7777-8111-111111111141";
const meta = { request_id: requestId, schema_version: "1", as_of: "2026-08-26T20:00:00Z" };
const actor = { userId: "allowed-user", requestId };
const cycleSummary = {
  decision_session: "2026-08-26", status: "in_progress", cycle_count: 1, completed_count: 0,
  latest_result_code: "PAPER_ORDERS_AWAITING_MARKET", last_completed_decision_at: null,
  next_eligible_at: null, next_eligible_reason: "CURRENT_CYCLE_IN_PROGRESS", execution_locked: true,
};

function evaluationSlots(paired = 0): Array<Record<string, unknown>> {
  return Array.from({ length: 24 }, (_, index) => ({
    phase: index < 4 ? "train" : "oos",
    decision_slot_at: new Date(Date.UTC(2026, 8, 18 + index, 14, 5)).toISOString(),
    status: index < paired ? "paired" : "scheduled",
    incumbent: { status: index < paired ? "completed" : "pending", result_code: index < paired ? "NO_ORDER_REQUIRED" : null },
    challenger: { status: index < paired ? "completed" : "pending", result_code: index < paired ? "NO_ORDER_REQUIRED" : null },
    reason_code: index < paired ? "PAIR_RECORDED" : "REGISTERED_SLOT_SCHEDULED",
  }));
}

afterEach(() => {
  vi.unstubAllGlobals();
  delete process.env.NEXUS_ENGINE_BASE_URL;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID;
  delete process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET;
  delete process.env.NEXUS_ENV;
});

describe("internal-paper Engine response boundary", () => {
  it("accepts only a bounded locked Agent cycle", async () => {
    const data = {
      cycle_id: portfolioId, status: "awaiting_market", result_code: "PAPER_ORDERS_AWAITING_MARKET",
      decision_session: "2026-08-26", decision_time: "2026-08-26T21:00:00Z",
      decision_slot_at: "2026-08-26T21:00:00Z",
      execution_session: "2026-08-27", execution_locked: true, candidate_count: 1,
      candidates: [{ instrument_id: instrumentId, symbol: "AAPL", selection_reasons: ["quality_rank"], research_status: "completed", stance: "hold", sizing_action: "maintain", intent_hash: "a".repeat(64), output_id: orderId, rejection_code: null }],
      decision: { decision_id: requestId, sizing_policy_version: "agent_paper_sizing_v1", target_cash_weight: "1.000000000000", risk_outcomes: [] },
      decision_market: null,
      execution_market: { source: "alpaca", feed: "iex", delivery_mode: "real_time", window_minutes: 5, status: "pending" },
      order_ids: [],
    };
    const path = "/internal/v1/paper/cycles/latest";
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "GET")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { ...data, execution_locked: false }, meta }), undefined, path, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { ...data, candidate_count: 2 }, meta }), undefined, path, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { ...data, execution_market: { ...data.execution_market, source: "schwab" } }, meta }), undefined, path, "GET")).status).toBe(500);

    const detailPath = `/internal/v1/paper/cycles/${portfolioId}`;
    const detail = { cycle: data, orders: [] };
    expect((await sanitizeEngineResponse(json({ data: detail, meta }), undefined, detailPath, "GET")).status).toBe(200);
    const conflated = structuredClone(detail) as Record<string, unknown>;
    conflated.proposal = { target_version_id: portfolioId };
    expect((await sanitizeEngineResponse(json({ data: conflated, meta }), undefined, detailPath, "GET")).status).toBe(500);

    const managementPath = `/internal/v1/paper/management/cycles/${portfolioId}`;
    const management = managementCycle(data);
    expect((await sanitizeEngineResponse(json({ data: management, meta }), undefined, managementPath, "GET")).status).toBe(200);
    const wrongManagementId = structuredClone(management);
    wrongManagementId.cycle.cycle_id = requestId;
    expect((await sanitizeEngineResponse(json({ data: wrongManagementId, meta }), undefined, managementPath, "GET")).status).toBe(500);

    const capacityBlocked = structuredClone(management) as Record<string, unknown>;
    capacityBlocked.candidate_batch = null;
    capacityBlocked.security_research = [];
    capacityBlocked.synthesis = null;
    capacityBlocked.proposal = null;
    capacityBlocked.activation_result = null;
    capacityBlocked.blocking_plan = null;
    capacityBlocked.plan_summary = null;
    capacityBlocked.blocker = {
      code: "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
      source_stage: "provider",
      category: "provider_capacity",
      proposal_rejection_code: null,
      as_of: "2026-08-26T21:00:03Z",
    };
    expect((await sanitizeEngineResponse(json({ data: capacityBlocked, meta }), undefined, managementPath, "GET")).status).toBe(200);
    const rawBlocked = structuredClone(capacityBlocked) as Record<string, unknown>;
    (rawBlocked.blocker as Record<string, unknown>).code = "RAW_PROVIDER_ERROR";
    expect((await sanitizeEngineResponse(json({ data: rawBlocked, meta }), undefined, managementPath, "GET")).status).toBe(500);

    const proposalBlocked = structuredClone(management) as Record<string, unknown>;
    proposalBlocked.proposal = null;
    proposalBlocked.active_target = null;
    proposalBlocked.activation_result = null;
    proposalBlocked.blocking_plan = null;
    proposalBlocked.plan_summary = null;
    proposalBlocked.blocker = {
      code: "PORTFOLIO_PROPOSAL_UNAVAILABLE",
      source_stage: "proposal",
      category: "proposal_rejected",
      proposal_rejection_code: "PORTFOLIO_DEPENDENCY_CONFLICT",
      as_of: "2026-08-26T21:00:03Z",
    };
    expect((await sanitizeEngineResponse(json({ data: proposalBlocked, meta }), undefined, managementPath, "GET")).status).toBe(200);
    proposalBlocked.active_target = management.active_target;
    expect((await sanitizeEngineResponse(json({ data: proposalBlocked, meta }), undefined, managementPath, "GET")).status).toBe(200);
    const unknownRejection = structuredClone(proposalBlocked) as Record<string, unknown>;
    (unknownRejection.blocker as Record<string, unknown>).proposal_rejection_code = "UNREVIEWED_REJECTION";
    expect((await sanitizeEngineResponse(json({ data: unknownRejection, meta }), undefined, managementPath, "GET")).status).toBe(500);

    const listPath = "/internal/v1/paper/cycles";
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: null, version_history: [], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(200);
    const versionEvaluation = {
      evaluation_id: "71000000-0000-7000-8000-000000000099", status: "observing",
      incumbent_prompt_hash: "a".repeat(64), incumbent_model_hash: "b".repeat(64),
      challenger_prompt_hash: "c".repeat(64), challenger_model_hash: "d".repeat(64),
      train_completed: 2, oos_completed: 0, oos_missing: 20, oos_incomparable: 0,
      slots: evaluationSlots(2),
      incumbent_metrics: null, challenger_metrics: null,
      comparisons: [
        { key: "hold_current", data_state: "READY", reason_code: "CASH_HOLD_CURRENT" },
        { key: "baseline_v1", data_state: "UNKNOWN", reason_code: "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE" },
        { key: "market_benchmark", data_state: "UNKNOWN", reason_code: "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE" },
      ], gates: [], reason_code: "EVALUATION_OBSERVING",
      execution_locked: true, broker_write: false, live_write: false,
    };
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: null, version_history: [], version_evaluation: versionEvaluation }, meta }), undefined, listPath, "GET")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: null, version_history: [], version_evaluation: { ...versionEvaluation, oos_missing: 19 } }, meta }), undefined, listPath, "GET")).status).toBe(500);
    const versionObservation = {
      prompt_version: "local-openai-v1", prompt_hash: "a".repeat(64), model_id: "gpt-5.6-terra",
      model_version: "codex-sdk-chatgpt-v4-actionable",
      model_hash: "b".repeat(64),
      first_decision_at: "2026-08-26T19:30:00Z", last_decision_at: "2026-08-28T19:30:00Z",
      observed_sessions: 3, cycle_count: 48, completed_count: 39, unavailable_count: 9,
      order_count: 0, fill_count: 0, challenger_available: false,
      lifecycle_state: "incumbent_frozen",
      verdict: "continue_observing",
      reason_codes: ["VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE", "VERSION_ORDER_PATH_UNOBSERVED", "VERSION_CHALLENGER_UNAVAILABLE"],
    };
    const olderVersion = {
      ...versionObservation,
      prompt_hash: "c".repeat(64),
      first_decision_at: "2026-08-25T19:30:00Z",
      last_decision_at: "2026-08-25T20:00:00Z",
      observed_sessions: 1,
      cycle_count: 4,
      completed_count: 4,
      unavailable_count: 0,
      lifecycle_state: "historical_frozen",
      verdict: "retained_as_frozen_history",
      reason_codes: ["VERSION_FROZEN_AUDIT_HISTORY", "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE", "VERSION_ORDER_PATH_UNOBSERVED", "VERSION_SEQUENTIAL_HISTORY_NOT_COMPARABLE"],
    };
    const versionHistory = [versionObservation, olderVersion];
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: versionObservation, version_history: versionHistory, version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: versionObservation }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: versionObservation, version_history: [olderVersion, versionObservation], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: versionObservation, version_history: [versionObservation, versionObservation], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: olderVersion, version_history: versionHistory, version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: { ...versionObservation, reason_codes: ["VERSION_PROMOTED"] }, version_history: versionHistory, version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: { ...versionObservation, completed_count: 49 }, version_history: versionHistory, version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: { ...versionObservation, first_decision_at: "2026-08-29T00:00:00Z" }, version_history: versionHistory, version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: { ...versionObservation, prompt_hash: "not-a-hash" }, version_history: versionHistory, version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: { ...versionObservation, reason_codes: ["VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE"] }, version_history: versionHistory, version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: { ...versionObservation, lifecycle_state: "historical_frozen" }, version_history: versionHistory, version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: olderVersion, version_history: [olderVersion], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    const secondIncumbent = { ...versionObservation, prompt_hash: "d".repeat(64), first_decision_at: "2026-08-25T19:30:00Z", last_decision_at: "2026-08-25T20:00:00Z" };
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: cycleSummary, version_observation: versionObservation, version_history: [versionObservation, secondIncumbent], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: { ...cycleSummary, cycle_count: 21 }, version_observation: null, version_history: [], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { items: [data], summary: { ...cycleSummary, cycle_count: 0 }, version_observation: null, version_history: [], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: Array.from({ length: 21 }, () => data), summary: cycleSummary, version_observation: null, version_history: [], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { items: [{ ...data, execution_locked: false }], summary: cycleSummary, version_observation: null, version_history: [], version_evaluation: null }, meta }), undefined, listPath, "GET")).status).toBe(500);
  });
  it("accepts the strict portfolio projection and rejects nested extras", async () => {
    const data = portfolio();
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, "/internal/v1/paper/portfolio")).status).toBe(200);

    const leaked = structuredClone(data);
    Object.assign(leaked.positions[0], { account_id: "must-not-escape" });
    expect((await sanitizeEngineResponse(json({ data: leaked, meta }), undefined, "/internal/v1/paper/portfolio")).status).toBe(500);

    const inconsistentCash = structuredClone(data);
    inconsistentCash.performance.reserved_cash = "501.000000000000";
    expect((await sanitizeEngineResponse(json({ data: inconsistentCash, meta }), undefined, "/internal/v1/paper/portfolio")).status).toBe(500);

    const inventedBaseline = structuredClone(data);
    inventedBaseline.comparison.rows[2].nav = "100000.000000000000";
    expect((await sanitizeEngineResponse(json({ data: inventedBaseline, meta }), undefined, "/internal/v1/paper/portfolio")).status).toBe(500);

    const persistedBenchmark = structuredClone(data);
    Object.assign(persistedBenchmark.comparison.rows[3], {
      data_state: "READY",
      nav: "99881.821590000000",
      total_return_ratio: "-0.001181784100",
      max_drawdown_ratio: "0.019841269800",
      turnover_ratio: "0",
      total_cost: "0",
      reason_code: "PERSISTED_MARKET_BENCHMARK",
    });
    expect((await sanitizeEngineResponse(json({ data: persistedBenchmark, meta }), undefined, "/internal/v1/paper/portfolio")).status).toBe(200);
  });

  it("rejects cross-object drift in a management cycle without recomputing server facts", async () => {
    const cycle = {
      cycle_id: portfolioId, status: "completed", result_code: "PAPER_BATCH_EVALUATED",
      decision_session: "2026-08-26", decision_time: "2026-08-26T21:00:00Z",
      decision_slot_at: "2026-08-26T21:00:00Z", execution_session: "2026-08-27",
      execution_locked: true, candidate_count: 0, candidates: [], decision: null,
      decision_market: null,
      execution_market: { source: "alpaca", feed: "iex", delivery_mode: "real_time", window_minutes: 5, status: "qualified" },
      order_ids: [],
    };
    const path = `/internal/v1/paper/management/cycles/${portfolioId}`;
    const valid = managementCycle(cycle);
    const mutations: Array<(value: Record<string, unknown>) => void> = [
      (value) => { (value.synthesis as Record<string, unknown>).candidate_batch_id = requestId; },
      (value) => { value.security_research = []; },
      (value) => { (value.proposal as Record<string, unknown>).synthesis_output_id = requestId; },
      (value) => {
        const targets = (value.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
        targets[0].depends_on_instrument_ids = [requestId];
      },
      (value) => { (value.activation_result as Record<string, unknown>).strategy_decision_id = orderId; },
      (value) => { (value.plan_summary as Record<string, unknown>).decision_hash = "f".repeat(64); },
      (value) => { (value.plan_summary as Record<string, unknown>).plan_created_at = "2026-08-26T21:00:07Z"; },
      (value) => {
        value.blocker = { code: "POLICY_UNCONFIRMED", source_stage: "policy", category: "policy_unconfirmed", proposal_rejection_code: null, as_of: "2026-08-26T21:00:06Z" };
      },
    ];
    for (const mutate of mutations) {
      const drift = structuredClone(valid) as Record<string, unknown>;
      mutate(drift);
      expect((await sanitizeEngineResponse(json({ data: drift, meta }), undefined, path, "GET")).status).toBe(500);
    }

    const policyUnconfirmed = structuredClone(valid) as Record<string, unknown>;
    policyUnconfirmed.proposal = null;
    policyUnconfirmed.active_target = null;
    policyUnconfirmed.activation_result = null;
    policyUnconfirmed.blocking_plan = null;
    policyUnconfirmed.plan_summary = null;
    policyUnconfirmed.blocker = { code: "POLICY_UNCONFIRMED", source_stage: "policy", category: "policy_unconfirmed", proposal_rejection_code: null, as_of: "2026-08-26T21:00:03Z" };
    expect((await sanitizeEngineResponse(json({ data: policyUnconfirmed, meta }), undefined, path, "GET")).status).toBe(200);
    policyUnconfirmed.active_target = valid.active_target;
    expect((await sanitizeEngineResponse(json({ data: policyUnconfirmed, meta }), undefined, path, "GET")).status).toBe(200);

    const legacy = structuredClone(valid) as Record<string, unknown>;
    legacy.candidate_batch = null; legacy.security_research = []; legacy.synthesis = null;
    legacy.proposal = null; legacy.active_target = null; legacy.activation_result = null;
    legacy.blocker = null; legacy.blocking_plan = null; legacy.plan_summary = null;
    expect((await sanitizeEngineResponse(json({ data: legacy, meta }), undefined, path, "GET")).status).toBe(200);

    const historicalTarget = structuredClone(valid) as Record<string, unknown>;
    (historicalTarget.active_target as Record<string, unknown>).target_version_id = requestId;
    (historicalTarget.active_target as Record<string, unknown>).strategy_decision_id = orderId;
    expect((await sanitizeEngineResponse(json({ data: historicalTarget, meta }), undefined, path, "GET")).status).toBe(200);

    const noOrder = structuredClone(valid) as Record<string, unknown>;
    noOrder.activation_result = { ...(noOrder.activation_result as Record<string, unknown>), result_code: "NO_ORDER_REQUIRED", batch_id: null };
    noOrder.plan_summary = null;
    expect((await sanitizeEngineResponse(json({ data: noOrder, meta }), undefined, path, "GET")).status).toBe(200);

    const occupied = structuredClone(valid) as Record<string, unknown>;
    occupied.activation_result = {
      result_code: "TARGET_ACTIVATION_BLOCKED",
      strategy_decision_id: requestId,
      target_version_id: null,
      batch_id: null,
      as_of: "2026-08-26T21:00:05Z",
    };
    occupied.blocker = { code: "TARGET_ACTIVATION_BLOCKED", source_stage: "activation", category: "resource_occupied", proposal_rejection_code: null, as_of: "2026-08-26T21:00:05Z" };
    occupied.blocking_plan = occupied.plan_summary;
    occupied.plan_summary = null;
    expect((await sanitizeEngineResponse(json({ data: occupied, meta }), undefined, path, "GET")).status).toBe(200);

    const bounded = structuredClone(valid) as Record<string, unknown>;
    const boundedBatch = bounded.candidate_batch as Record<string, unknown>;
    boundedBatch.candidate_instrument_ids = [instrumentId, secondInstrumentId, thirdInstrumentId];
    const firstResearch = (bounded.security_research as Array<Record<string, unknown>>)[0];
    bounded.security_research = [
      firstResearch,
      { ...firstResearch, instrument_id: secondInstrumentId, research_run_id: requestId, research_output_id: orderId },
      { ...firstResearch, instrument_id: thirdInstrumentId, research_run_id: targetId, research_output_id: planId },
    ];
    const firstChoice = ((bounded.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>)[0];
    (bounded.synthesis as Record<string, unknown>).choices = [
      firstChoice,
      { ...firstChoice, instrument_id: secondInstrumentId, rank: 2 },
      { ...firstChoice, instrument_id: thirdInstrumentId, rank: 3 },
    ];
    const firstProposalTarget = ((bounded.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[0];
    (bounded.proposal as Record<string, unknown>).targets = [
      firstProposalTarget,
      { ...firstProposalTarget, instrument_id: secondInstrumentId, rank: 2 },
      { ...firstProposalTarget, instrument_id: thirdInstrumentId, rank: 3 },
    ];
    expect((await sanitizeEngineResponse(json({ data: bounded, meta }), undefined, path, "GET")).status).toBe(200);
    const missingChoice = structuredClone(bounded) as Record<string, unknown>;
    (missingChoice.synthesis as Record<string, unknown>).choices =
      ((missingChoice.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>).slice(0, 2);
    expect((await sanitizeEngineResponse(json({ data: missingChoice, meta }), undefined, path, "GET")).status).toBe(500);
    for (let index = 0; index < 3; index += 1) {
      const missingTarget = structuredClone(bounded) as Record<string, unknown>;
      const targets = (missingTarget.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
      targets.splice(index, 1);
      expect((await sanitizeEngineResponse(json({ data: missingTarget, meta }), undefined, path, "GET")).status).toBe(500);
    }

    for (const mutate of [
      (choice: Record<string, unknown>, target: Record<string, unknown>) => {
        choice.rank = null;
        target.rank = null;
      },
      (choice: Record<string, unknown>, target: Record<string, unknown>) => {
        Object.assign(choice, { action: "maintain", role: "none", rank: 1 });
        Object.assign(target, { reason_code: "MAINTAIN", role: "none", rank: 1 });
      },
      (choice: Record<string, unknown>, target: Record<string, unknown>) => {
        Object.assign(choice, { action: "increase", role: "none", rank: 1 });
        Object.assign(target, { reason_code: "INCREASE", role: "none", rank: 1 });
      },
    ]) {
      const invalidRankOrRole = structuredClone(valid) as Record<string, unknown>;
      mutate(
        ((invalidRankOrRole.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>)[0],
        ((invalidRankOrRole.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[0],
      );
      expect((await sanitizeEngineResponse(json({ data: invalidRankOrRole, meta }), undefined, path, "GET")).status).toBe(500);
    }
    for (const rank of [1, 4]) {
      const invalidRankSequence = structuredClone(bounded) as Record<string, unknown>;
      const choices = (invalidRankSequence.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>;
      const targets = (invalidRankSequence.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
      choices[1].rank = rank;
      targets[1].rank = rank;
      expect((await sanitizeEngineResponse(json({ data: invalidRankSequence, meta }), undefined, path, "GET")).status).toBe(500);
    }
    const forgedTarget = structuredClone(bounded) as Record<string, unknown>;
    (((forgedTarget.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[1]).instrument_id = requestId;
    expect((await sanitizeEngineResponse(json({ data: forgedTarget, meta }), undefined, path, "GET")).status).toBe(500);

    const inherited = structuredClone(valid) as Record<string, unknown>;
    const inheritedProposal = inherited.proposal as Record<string, unknown>;
    inheritedProposal.cash_weight = "0.7";
    (inheritedProposal.targets as Array<Record<string, unknown>>).push({
      instrument_id: secondInstrumentId,
      target_weight: "0.1",
      exit: false,
      role: "core",
      reason_code: "INHERITED_TARGET",
      rank: null,
      replaces_instrument_id: null,
      depends_on_instrument_ids: [],
    });
    const inheritedTarget = inherited.active_target as Record<string, unknown>;
    inheritedTarget.cash_weight = "0.7";
    (inheritedTarget.targets as Array<Record<string, unknown>>).push({
      instrument_id: secondInstrumentId,
      target_weight: "0.1",
      exit: false,
      role: "core",
      reason_code: "INHERITED_TARGET",
    });
    expect((await sanitizeEngineResponse(json({ data: inherited, meta }), undefined, path, "GET")).status).toBe(200);
    for (const forged of [
      { reason_code: "SYNTHESIS_INCLUDE" },
      { rank: 2 },
      { replaces_instrument_id: thirdInstrumentId },
      { depends_on_instrument_ids: [thirdInstrumentId] },
    ]) {
      const invalidInherited = structuredClone(inherited) as Record<string, unknown>;
      Object.assign(
        ((invalidInherited.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[1],
        forged,
      );
      expect((await sanitizeEngineResponse(json({ data: invalidInherited, meta }), undefined, path, "GET")).status).toBe(500);
    }

    const deferred = structuredClone(valid) as Record<string, unknown>;
    const deferredChoice = ((deferred.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>)[0];
    Object.assign(deferredChoice, { action: "defer", role: "none", rank: null });
    const deferredTarget = ((deferred.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[0];
    Object.assign(deferredTarget, { target_weight: "0", role: "none", rank: null, reason_code: "DEFER" });
    expect((await sanitizeEngineResponse(json({ data: deferred, meta }), undefined, path, "GET")).status).toBe(200);
    const dependencyDeferred = structuredClone(valid) as Record<string, unknown>;
    const dependencyBatch = dependencyDeferred.candidate_batch as Record<string, unknown>;
    dependencyBatch.candidate_instrument_ids = [instrumentId, secondInstrumentId];
    const dependencyResearch = (dependencyDeferred.security_research as Array<Record<string, unknown>>)[0];
    dependencyDeferred.security_research = [
      { ...dependencyResearch, stance: "reduce_candidate" },
      { ...dependencyResearch, instrument_id: secondInstrumentId, research_run_id: requestId, research_output_id: orderId },
    ];
    const dependencyChoice = ((dependencyDeferred.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>)[0];
    (dependencyDeferred.synthesis as Record<string, unknown>).choices = [
      { ...dependencyChoice, action: "reduce", role: "none", rank: null },
      {
        ...dependencyChoice,
        instrument_id: secondInstrumentId,
        action: "defer",
        role: "none",
        rank: null,
        depends_on_instrument_ids: [instrumentId],
      },
    ];
    const dependencyTarget = ((dependencyDeferred.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[0];
    (dependencyDeferred.proposal as Record<string, unknown>).cash_weight = "0.98";
    (dependencyDeferred.proposal as Record<string, unknown>).targets = [
      { ...dependencyTarget, target_weight: "0.02", role: "none", rank: null, reason_code: "ATOMIC_STEP_REQUIRED" },
      {
        ...dependencyTarget,
        instrument_id: secondInstrumentId,
        target_weight: "0",
        role: "none",
        rank: null,
        depends_on_instrument_ids: [instrumentId],
        reason_code: "DEPENDENCY_NOT_APPLIED",
      },
    ];
    const dependencyActiveTarget = dependencyDeferred.active_target as Record<string, unknown>;
    dependencyActiveTarget.cash_weight = "0.98";
    (dependencyActiveTarget.targets as Array<Record<string, unknown>>)[0].target_weight = "0.02";
    expect((await sanitizeEngineResponse(json({ data: dependencyDeferred, meta }), undefined, path, "GET")).status).toBe(200);
    const dependencyWithoutSource = structuredClone(dependencyDeferred) as Record<string, unknown>;
    const emptyDependencyChoice = ((dependencyWithoutSource.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>)[1];
    const emptyDependencyTarget = ((dependencyWithoutSource.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[1];
    emptyDependencyChoice.depends_on_instrument_ids = [];
    emptyDependencyTarget.depends_on_instrument_ids = [];
    expect((await sanitizeEngineResponse(json({ data: dependencyWithoutSource, meta }), undefined, path, "GET")).status).toBe(500);
    const unknownDependency = structuredClone(dependencyDeferred) as Record<string, unknown>;
    (((unknownDependency.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>)[1]).depends_on_instrument_ids = [thirdInstrumentId];
    (((unknownDependency.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[1]).depends_on_instrument_ids = [thirdInstrumentId];
    expect((await sanitizeEngineResponse(json({ data: unknownDependency, meta }), undefined, path, "GET")).status).toBe(500);

    const invalidReplacement = structuredClone(dependencyDeferred) as Record<string, unknown>;
    const invalidReplacementChoices = (invalidReplacement.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>;
    Object.assign(invalidReplacementChoices[0], { action: "maintain", role: "none", rank: null });
    Object.assign(invalidReplacementChoices[1], {
      action: "include", role: "core", rank: 1,
      replaces_instrument_id: instrumentId, depends_on_instrument_ids: [],
    });
    const invalidReplacementTargets = (invalidReplacement.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
    Object.assign(invalidReplacementTargets[0], { role: "none", rank: null, reason_code: "MAINTAIN" });
    Object.assign(invalidReplacementTargets[1], {
      role: "core", rank: 1, replaces_instrument_id: instrumentId,
      depends_on_instrument_ids: [], reason_code: "INCLUDE",
    });
    expect((await sanitizeEngineResponse(json({ data: invalidReplacement, meta }), undefined, path, "GET")).status).toBe(500);

    const invertedRank = structuredClone(invalidReplacement) as Record<string, unknown>;
    const invertedChoices = (invertedRank.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>;
    Object.assign(invertedChoices[0], { action: "include", role: "core", rank: 2 });
    Object.assign(invertedChoices[1], {
      action: "include", role: "core", rank: 1,
      replaces_instrument_id: null, depends_on_instrument_ids: [instrumentId],
    });
    const invertedTargets = (invertedRank.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
    Object.assign(invertedTargets[0], { role: "core", rank: 2, reason_code: "INCLUDE" });
    Object.assign(invertedTargets[1], {
      role: "core", rank: 1, replaces_instrument_id: null,
      depends_on_instrument_ids: [instrumentId], reason_code: "DEPENDENCY_NOT_APPLIED",
    });
    expect((await sanitizeEngineResponse(json({ data: invertedRank, meta }), undefined, path, "GET")).status).toBe(500);

    for (const mutate of [
      (value: Record<string, unknown>) => {
        const choices = (value.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>;
        const targets = (value.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
        choices[1].depends_on_instrument_ids = [secondInstrumentId];
        targets[1].depends_on_instrument_ids = [secondInstrumentId];
      },
      (value: Record<string, unknown>) => {
        const choices = (value.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>;
        const targets = (value.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
        choices[1].depends_on_instrument_ids = [instrumentId, instrumentId];
        targets[1].depends_on_instrument_ids = [instrumentId, instrumentId];
      },
      (value: Record<string, unknown>) => {
        const choices = (value.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>;
        const targets = (value.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
        choices[0].depends_on_instrument_ids = [secondInstrumentId];
        targets[0].depends_on_instrument_ids = [secondInstrumentId];
        targets[0].reason_code = "DEPENDENCY_NOT_APPLIED";
      },
      (value: Record<string, unknown>) => {
        const choices = (value.synthesis as Record<string, unknown>).choices as Array<Record<string, unknown>>;
        const targets = (value.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>;
        choices[1].replaces_instrument_id = instrumentId;
        targets[1].replaces_instrument_id = instrumentId;
      },
    ]) {
      const invalidStructure = structuredClone(dependencyDeferred) as Record<string, unknown>;
      mutate(invalidStructure);
      expect((await sanitizeEngineResponse(json({ data: invalidStructure, meta }), undefined, path, "GET")).status).toBe(500);
    }

    for (const reason_code of ["INHERITED_TARGET", "UNKNOWN_REASON"]) {
      const invalidReason = structuredClone(valid) as Record<string, unknown>;
      (((invalidReason.proposal as Record<string, unknown>).targets as Array<Record<string, unknown>>)[0]).reason_code = reason_code;
      expect((await sanitizeEngineResponse(json({ data: invalidReason, meta }), undefined, path, "GET")).status).toBe(500);
    }
  });

  it("keeps management, plan, and cancellation facts as distinct strict reads", async () => {
    const management = managementCycle({
      cycle_id: portfolioId, status: "completed", result_code: "PAPER_BATCH_EVALUATED",
      decision_session: "2026-08-26", decision_time: "2026-08-26T21:00:00Z",
      decision_slot_at: "2026-08-26T21:00:00Z", execution_session: "2026-08-27",
      execution_locked: true, candidate_count: 0, candidates: [], decision: null,
      decision_market: null, execution_market: null, order_ids: [],
    });
    const summary = {
      current_target: management.active_target,
      occupying_plan: management.plan_summary,
      latest_proposal: management.proposal,
      read_as_of: "2026-08-26T22:00:00Z",
    };
    const summaryPath = "/internal/v1/paper/management/portfolio";
    expect((await sanitizeEngineResponse(json({ data: summary, meta }), undefined, summaryPath, "GET")).status).toBe(200);
    const leakedSummary = structuredClone(summary) as Record<string, unknown>;
    leakedSummary.available_cash = "3500";
    expect((await sanitizeEngineResponse(json({ data: leakedSummary, meta }), undefined, summaryPath, "GET")).status).toBe(500);
    const laterProposal = structuredClone(summary);
    laterProposal.latest_proposal.strategy_decision_id = orderId;
    laterProposal.latest_proposal.decision_hash = "f".repeat(64);
    expect((await sanitizeEngineResponse(json({ data: laterProposal, meta }), undefined, summaryPath, "GET")).status).toBe(200);
    for (const drift of [
      { ...structuredClone(summary), current_target: { ...summary.current_target, as_of: "2026-08-26T22:00:01Z" } },
      { ...structuredClone(summary), occupying_plan: { ...summary.occupying_plan, occupation_state: "released" } },
      { ...structuredClone(summary), occupying_plan: { ...summary.occupying_plan, reconciliation_as_of: "2026-08-26T22:00:01Z" } },
      { ...structuredClone(summary), latest_proposal: { ...summary.latest_proposal, as_of: "2026-08-26T22:00:01Z" } },
      { ...structuredClone(summary), occupying_plan: { ...summary.occupying_plan, target_version_id: orderId } },
      { ...structuredClone(summary), current_target: null },
      { ...structuredClone(summary), occupying_plan: { ...summary.occupying_plan, strategy_decision_id: orderId } },
    ]) {
      expect((await sanitizeEngineResponse(json({ data: drift, meta }), undefined, summaryPath, "GET")).status).toBe(500);
    }

    const plan = paperPlan();
    const planPath = `/internal/v1/paper/plans/${planId}`;
    expect((await sanitizeEngineResponse(json({ data: plan, meta }), undefined, planPath, "GET")).status).toBe(200);
    const legacyPlan = { ...structuredClone(plan), association_state: "legacy_unassociated", target: null };
    expect((await sanitizeEngineResponse(json({ data: legacyPlan, meta }), undefined, planPath, "GET")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data: { ...plan, batch_id: requestId }, meta }), undefined, planPath, "GET")).status).toBe(500);
    for (const drift of [
      { ...structuredClone(plan), association_state: "legacy_unassociated" },
      { ...structuredClone(plan), target: null },
      { ...structuredClone(plan), plan: { ...plan.plan, reconciliation_as_of: "2026-08-26T21:00:04Z" } },
      { ...structuredClone(plan), actual: { data_state: "UNKNOWN", as_of: null, available_cash: "8400", reserved_cash: null, nav: null, positions: null } },
      { ...structuredClone(plan), actual: { ...plan.actual, as_of: "2026-08-26T21:00:07Z" } },
      { ...structuredClone(plan), plan: { ...plan.plan, items: [{ ...plan.plan.items[0], order_id: requestId }] } },
    ]) {
      expect((await sanitizeEngineResponse(json({ data: drift, meta }), undefined, planPath, "GET")).status).toBe(500);
    }
    for (const field of ["available_cash", "reserved_cash", "nav", "positions"] as const) {
      const drift = structuredClone(plan);
      drift.actual[field] = null as never;
      expect((await sanitizeEngineResponse(json({ data: drift, meta }), undefined, planPath, "GET")).status).toBe(500);
    }

    const cancelResult = {
      status: "UNKNOWN", result_event_seq: null, committed_at: null,
      order: { order_id: orderId, status: "partially_filled", current_event_seq: 4, filled_quantity: "2", remaining_quantity: "3", as_of: "2026-08-26T21:00:00Z" },
      reservation: { reservation_id: requestId, reservation_type: "cash", amount: "500", outstanding_amount: "500", released: false, as_of: "2026-08-26T21:00:00Z" },
      reservation_released: false,
    };
    const cancelPath = `/internal/v1/paper/orders/${orderId}/cancel-result`;
    expect((await sanitizeEngineResponse(json({ data: cancelResult, meta }), undefined, cancelPath, "GET")).status).toBe(200);
    const wrongOrder = structuredClone(cancelResult);
    wrongOrder.order.order_id = portfolioId;
    expect((await sanitizeEngineResponse(json({ data: wrongOrder, meta }), undefined, cancelPath, "GET")).status).toBe(500);

    const committed = {
      status: "COMMITTED", result_event_seq: 5, committed_at: "2026-08-26T21:01:00Z",
      order: { order_id: orderId, status: "cancelled", current_event_seq: 5, filled_quantity: "2", remaining_quantity: "3", as_of: "2026-08-26T21:01:00Z" },
      reservation: { reservation_id: requestId, reservation_type: "cash", amount: "500", outstanding_amount: "0", released: true, as_of: "2026-08-26T21:01:00Z" },
      reservation_released: true,
    };
    expect((await sanitizeEngineResponse(json({ data: committed, meta }), undefined, cancelPath, "GET")).status).toBe(200);
    for (const drift of [
      { ...structuredClone(committed), result_event_seq: 4 },
      { ...structuredClone(committed), committed_at: "2026-08-26T21:02:00Z" },
      { ...structuredClone(committed), reservation_released: false },
      { ...structuredClone(committed), order: { ...committed.order, status: "partially_filled" } },
    ]) {
      expect((await sanitizeEngineResponse(json({ data: drift, meta }), undefined, cancelPath, "GET")).status).toBe(500);
    }
  });

  it("uses the bounded paper response budget for a legal projection above the default cap", async () => {
    const data = portfolio();
    data.attributions = Array.from({ length: 500 }, (_, index) => ({
      ...data.attributions[0],
      dimension_key: `${String(index).padStart(3, "0")}-${"a".repeat(96)}`,
    }));
    const envelope = { data, meta };
    expect(JSON.stringify(envelope).length).toBeGreaterThan(64 * 1024);
    expect((await sanitizeEngineResponse(json(envelope), undefined, "/internal/v1/paper/portfolio")).status).toBe(200);
  });

  it("accepts bounded orders and rejects forged cancellation authority", async () => {
    const data = { items: [order()], next_cursor: "signed-cursor" };
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(200);

    const leaked = structuredClone(data);
    Object.assign(leaked.items[0], { account_id: "must-not-escape" });
    expect((await sanitizeEngineResponse(json({ data: leaked, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);

    const forged = structuredClone(data);
    Object.assign(forged.items[0], { cancellable: false, status: "submitted" });
    expect((await sanitizeEngineResponse(json({ data: forged, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);

    const inconsistent = structuredClone(data);
    inconsistent.items[0].remaining_quantity = "4.000000000000";
    expect((await sanitizeEngineResponse(json({ data: inconsistent, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);

    const brokenTimeline = structuredClone(data);
    brokenTimeline.items[0].events[2].from_status = "proposed";
    expect((await sanitizeEngineResponse(json({ data: brokenTimeline, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);
  });

  it("binds cancellation to method and requested order identity", async () => {
    const data = {
      order_id: orderId,
      status: "cancelled",
      filled_quantity: "2.000000000000",
      remaining_quantity: "3.000000000000",
      result_event_seq: 8,
      replayed: false,
    };
    const path = `/internal/v1/paper/orders/${orderId}/cancel`;
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "POST")).status).toBe(200);
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, path, "GET")).status).toBe(500);
    expect((await sanitizeEngineResponse(json({ data: { ...data, order_id: portfolioId }, meta }), undefined, path, "POST")).status).toBe(500);
  });

  it("caps order collections independently of schema-shaped content", async () => {
    const data = { items: Array.from({ length: 51 }, order), next_cursor: null };
    expect((await sanitizeEngineResponse(json({ data, meta }), undefined, "/internal/v1/paper/orders", "GET")).status).toBe(500);
  });

  it("allows only canonical paper targets and rebuilds protected headers", async () => {
    setEngineEnvironment();
    const calls: Array<{ url: string; init: RequestInit }> = [];
    vi.stubGlobal("fetch", async (input: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: input.toString(), init: init ?? {} });
      if (input.toString().includes("/paper/cycles")) {
        return json({ data: { items: [], summary: { ...cycleSummary, status: "ready", cycle_count: 0, completed_count: 0, latest_result_code: null, next_eligible_at: "2026-08-26T21:00:00Z", next_eligible_reason: "CURRENT_SLOT" }, version_observation: null, version_history: [], version_evaluation: null }, meta });
      }
      return json({ data: { items: [order()], next_cursor: null }, meta });
    });

    const response = await engineFetch({
      template: "/internal/v1/paper/orders",
      path: "/internal/v1/paper/orders",
      query: "status=submitted&limit=25",
    }, {
      method: "GET",
      actor,
      headers: { authorization: "must-not-survive", "x-nexus-signature": "forged" },
    });
    expect(response.status).toBe(200);
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe("http://127.0.0.1:8000/internal/v1/paper/orders?limit=25&status=submitted");
    const headers = new Headers(calls[0].init.headers);
    expect(headers.get("x-nexus-signature")).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(headers.get("x-nexus-actor-id")).toBe(actor.userId);
    expect(headers.get("authorization")).toBeNull();

    calls.length = 0;
    const cycles = await engineFetch({
      template: "/internal/v1/paper/cycles",
      path: "/internal/v1/paper/cycles",
      query: "decision_session=2026-09-14&limit=20",
    }, { method: "GET", actor });
    expect(cycles.status).toBe(200);
    expect(calls[0].url).toBe("http://127.0.0.1:8000/internal/v1/paper/cycles?decision_session=2026-09-14&limit=20");

    calls.length = 0;
    const unsafeTargets = [
      {
        template: "/internal/v1/paper/orders",
        path: "/internal/v1/paper/orders",
        query: "account_id=secret",
      },
      {
        template: "/internal/v1/paper/orders/{order_id}/cancel",
        path: "/internal/v1/paper/orders/../../system-status/cancel",
      },
    ];
    const unsafeFetch = engineFetch as unknown as (
      target: unknown,
      init: { method: string; actor: typeof actor },
    ) => Promise<Response>;
    for (const target of unsafeTargets) {
      expect((await unsafeFetch(target, { method: "GET", actor })).status).toBe(500);
    }
    expect(calls).toHaveLength(0);
  });
});

function managementCycle(cycle: Record<string, unknown>) {
  const targetAllocation = { instrument_id: instrumentId, target_weight: "0.2", exit: false, role: "core", reason_code: "INCLUDE" };
  const proposalTarget = { ...targetAllocation, rank: 1, replaces_instrument_id: null, depends_on_instrument_ids: [] };
  const proposal = {
    strategy_decision_id: requestId,
    synthesis_output_id: synthesisId,
    decision_hash: "d".repeat(64),
    policy_hash: "e".repeat(64),
    baseline_target_version_id: null,
    initial_approval_id: "0198f7a0-1111-7777-8111-111111111119",
    cash_weight: "0.8",
    targets: [proposalTarget],
    expires_at: "2026-08-27T21:00:00Z",
    as_of: "2026-08-26T21:00:03Z",
  };
  const planSummary = {
    batch_id: planId,
    target_version_id: targetId,
    strategy_decision_id: requestId,
    decision_hash: "d".repeat(64),
    plan_created_at: "2026-08-26T21:00:05Z",
    reconciliation_as_of: "2026-08-26T21:00:06Z",
    occupation_state: "occupied",
  };
  return {
    cycle,
    orders: [],
    candidate_batch: {
      candidate_batch_id: candidateBatchId,
      universe_snapshot_id: "0198f7a0-1111-7777-8111-111111111130",
      candidate_instrument_ids: [instrumentId],
      source: "explicit_research_scope",
      batch_hash: "a".repeat(64),
      as_of: "2026-08-26T21:00:00Z",
    },
    security_research: [{
      instrument_id: instrumentId,
      research_run_id: "0198f7a0-1111-7777-8111-111111111131",
      research_output_id: "0198f7a0-1111-7777-8111-111111111132",
      status: "completed",
      content_hash: "b".repeat(64),
      intent_hash: "c".repeat(64),
      stance: "buy_candidate",
      quality_status: "complete",
      as_of: "2026-08-26T21:00:01Z",
    }],
    synthesis: {
      synthesis_run_id: "0198f7a0-1111-7777-8111-111111111133",
      synthesis_output_id: synthesisId,
      candidate_batch_id: candidateBatchId,
      input_hash: "4".repeat(64),
      content_hash: "5".repeat(64),
      binding_hash: "6".repeat(64),
      status: "completed",
      choices: [{
        instrument_id: instrumentId,
        action: "include",
        role: "core",
        rank: 1,
        replaces_instrument_id: null,
        depends_on_instrument_ids: [],
        thesis: "Bounded inclusion thesis.",
        counter_thesis: "Bounded counter thesis.",
        invalidation: "Invalidate on evidence expiry.",
      }],
      cash_rationale: "Cash remains bounded by policy.",
      as_of: "2026-08-26T21:00:02Z",
    },
    proposal,
    active_target: {
      target_version_id: targetId,
      version_seq: 1,
      strategy_decision_id: requestId,
      previous_target_version_id: null,
      target_content_hash: "7".repeat(64),
      activation_hash: "8".repeat(64),
      cash_weight: "0.8",
      targets: [targetAllocation],
      as_of: "2026-08-26T21:00:04Z",
    },
    activation_result: {
      result_code: "PLAN_CREATED",
      strategy_decision_id: requestId,
      target_version_id: targetId,
      batch_id: planId,
      as_of: "2026-08-26T21:00:05Z",
    },
    blocker: null,
    blocking_plan: null,
    plan_summary: planSummary,
  };
}

function paperPlan() {
  return {
    batch_id: planId,
    currency: "USD",
    association_state: "target_associated",
    target: { target_version_id: targetId, version_seq: 1, target_content_hash: "7".repeat(64), as_of: "2026-08-26T21:00:04Z" },
    proposal: { strategy_decision_id: requestId, decision_hash: "d".repeat(64), cash_weight: "0.8", targets: [{ instrument_id: instrumentId, target_weight: "0.2", exit: false }], as_of: "2026-08-26T21:00:03Z" },
    plan: {
      plan_created_at: "2026-08-26T21:00:05Z",
      reconciliation_as_of: "2026-08-26T21:00:06Z",
      occupation_state: "occupied",
      risk: [{ order_id: orderId, instrument_id: instrumentId, outcome: "reduced", desired_quantity: "10", approved_quantity: "5", reduced_quantity: "5", rejected_quantity: "0", desired_amount_usd: "2000", approved_amount_usd: "1000", reduced_amount_usd: "1000", rejected_amount_usd: "0" }],
      items: [{ order_id: orderId, instrument_id: instrumentId, side: "buy", status: "partially_filled", confirmed_fill_quantity: "2", confirmed_fills: [], open_order_coverage_quantity: "3", open_order_coverage_amount_usd: "600", cash_reservation_outstanding: "600", position_reservation_outstanding: null, remaining_target_gap_quantity: "8", remaining_target_gap_amount_usd: "1600", order_as_of: "2026-08-26T21:00:06Z", reservation_as_of: "2026-08-26T21:00:06Z" }],
    },
    actual: { data_state: "READY", as_of: "2026-08-26T21:00:06Z", available_cash: "8400", reserved_cash: "600", nav: "10000", positions: [{ instrument_id: instrumentId, quantity: "2", reserved_quantity: "0" }] },
  };
}

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { status: 200, headers: { "content-type": "application/json" } });
}

function portfolio() {
  return {
    portfolio_id: portfolioId,
    name: "NEXUS Internal Paper",
    mode: "internal_paper" as const,
    currency: "USD" as const,
    initial_capital: "100000.000000000000",
    data_state: "READY" as const,
    performance: {
      as_of: "2026-08-26T20:00:00Z",
      cash: "90000.000000000000",
      available_cash: "89500.000000000000",
      reserved_cash: "500.000000000000",
      market_value: "10500.000000000000",
      nav: "100500.000000000000",
      peak_nav: "101000.000000000000",
      realized_pnl: "100.000000000000",
      unrealized_pnl: "400.000000000000",
      fees: "2.000000000000",
      drawdown_ratio: "0.004950495050",
    },
    comparison: {
      window_start: "2026-08-26T13:00:00Z",
      as_of: "2026-08-26T20:00:00Z",
      rows: [
        { key: "agent" as const, data_state: "READY" as const, nav: "100500.000000000000", total_return_ratio: "0.005000000000", max_drawdown_ratio: "0.004950495050", turnover_ratio: "0.105000000000", total_cost: "7.000000000000", reason_code: "PERSISTED_AGENT_PERFORMANCE" as const },
        { key: "hold_current" as const, data_state: "READY" as const, nav: "100000.000000000000", total_return_ratio: "0", max_drawdown_ratio: "0", turnover_ratio: "0", total_cost: "0", reason_code: "CASH_HOLD_CURRENT" as const },
        { key: "baseline_v1" as const, data_state: "UNAVAILABLE" as const, nav: null, total_return_ratio: null, max_drawdown_ratio: null, turnover_ratio: null, total_cost: null, reason_code: "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE" as const },
        { key: "market_benchmark" as const, data_state: "UNAVAILABLE" as const, nav: null, total_return_ratio: null, max_drawdown_ratio: null, turnover_ratio: null, total_cost: null, reason_code: "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE" as const },
      ],
    },
    positions: [{
      instrument_id: instrumentId,
      symbol: "AAPL",
      quantity: "50.000000000000",
      reserved_quantity: "5.000000000000",
      available_quantity: "45.000000000000",
      cost_basis: "10100.000000000000",
      average_cost: "202.000000000000",
      reference_close: "210.000000000000",
      market_value: "10500.000000000000",
      unrealized_pnl: "400.000000000000",
      price_source: "alpaca",
      price_feed: "iex",
      delivery_mode: "delayed" as const,
      price_as_of: "2026-08-26T20:00:00Z",
    }],
    attributions: [{
      dimension: "instrument" as const,
      dimension_key: instrumentId,
      market_value: "10500.000000000000",
      realized_pnl: "100.000000000000",
      unrealized_pnl: "400.000000000000",
      fees: "2.000000000000",
      slippage: "5.000000000000",
      completeness: "FULL" as const,
    }],
  };
}

function order() {
  return {
    order_id: orderId,
    instrument_id: instrumentId,
    symbol: "AAPL",
    side: "buy" as const,
    quantity: "5.000000000000",
    filled_quantity: "2.000000000000",
    remaining_quantity: "3.000000000000",
    worst_price: "220.000000000000",
    status: "partially_filled" as const,
    cancellable: true,
    current_event_seq: 4,
    submitted_at: "2026-08-26T19:30:00Z",
    strategy_version: "baseline_v1",
    risk_policy_version: "paper_risk_v1",
    events: [
      {
        event_seq: 1,
        from_status: null,
        to_status: "proposed" as const,
        actor_kind: "task18" as const,
        reason_code: "paper_order_proposed",
        occurred_at: "2026-08-26T19:27:00Z",
      },
      {
        event_seq: 2,
        from_status: "proposed" as const,
        to_status: "risk_approved" as const,
        actor_kind: "task18" as const,
        reason_code: "paper_risk_approved",
        occurred_at: "2026-08-26T19:28:00Z",
      },
      {
        event_seq: 3,
        from_status: "risk_approved" as const,
        to_status: "submitted" as const,
        actor_kind: "task19" as const,
        reason_code: "paper_order_submitted",
        occurred_at: "2026-08-26T19:30:00Z",
      },
      {
        event_seq: 4,
        from_status: "submitted" as const,
        to_status: "partially_filled" as const,
        actor_kind: "task19" as const,
        reason_code: "paper_partial_fill",
        occurred_at: "2026-08-26T20:00:00Z",
      },
    ],
    fills: [{
      fill_id: "0198f7a0-1111-7777-8111-111111111115",
      fill_seq: 1,
      quantity: "2.000000000000",
      price: "210.000000000000",
      notional: "420.000000000000",
      fee: "1.000000000000",
      occurred_at: "2026-08-26T20:00:00Z",
      source: "alpaca",
      feed: "iex",
      delivery_mode: "delayed" as const,
      source_as_of: "2026-08-26T19:59:00Z",
      execution_model_hash: "a".repeat(64),
      slippage_bps: 10 as const,
      slippage: "0.420000000000",
    }],
  };
}

function setEngineEnvironment(): void {
  process.env.NEXUS_ENGINE_BASE_URL = "http://127.0.0.1:8000";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_KEY_ID = "test-current";
  process.env.NEXUS_ENGINE_HMAC_CURRENT_SECRET = "nexus-test-hmac-key-32-bytes!!!!";
  process.env.NEXUS_ENV = "test";
}
