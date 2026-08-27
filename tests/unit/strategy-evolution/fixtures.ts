import type { components } from "../../../app/lib/generated/schema";

export const requestId = "0198f7a0-1111-7777-8111-111111111111";
export const familyId = "0198d500-1000-7000-8000-000000002201";
export const experimentId = "0198d500-1000-7000-8000-000000002202";
export const registryEventId = "0198d500-1000-7000-8000-000000002203";

export const meta: components["schemas"]["EvolutionSuccessMeta"] = {
  request_id: requestId,
  schema_version: "1",
  as_of: "2026-08-27T16:00:00Z",
};

export const overview: components["schemas"]["StrategyEvolutionData"] = {
  status: "ready",
  write_enabled: true,
  write_block_reason: null,
  experiment_start_enabled: false,
  experiment_start_reason: "ACTIVE_FAMILY_EXISTS",
  champion: {
    strategy_version: "baseline_v1",
    generation: 1,
    registry_version: 1,
    event_sequence: 1,
    effective_session: null,
    auto_promotion_paused: false,
    pending_transition: null,
    pending_effective_session: null,
    live_execution_enabled: false,
  },
  active_family: {
    family_id: familyId,
    created_at: "2026-08-27T15:00:00Z",
    hypothesis: "提高流动性门槛可减少成本侵蚀，同时保留中期动量溢价。",
    expected_mechanism: "减少低流动性标的并提高下一交易日开盘成交稳定性。",
    search_axes: [
      { axis: "min_adv20_usd", candidate_value_count: 3 },
      { axis: "ranking_feature", candidate_value_count: 2 },
    ],
    candidate_count: 3,
    state: "SHADOW_RUNNING",
    projection_version: 4,
    cumulative_oos_trial_count: 3,
    authority_hash: "a".repeat(64),
    protocol_hash: "b".repeat(64),
    code_hash: "c".repeat(64),
    next_step: "继续完成 Strategy Shadow 检查点。",
  },
  cooldown_until: "2026-10-01T00:00:00Z",
  experiments: [{
    experiment_id: experimentId,
    family_id: familyId,
    candidate_ordinal: 1,
    strategy_version: "challenger_liquidity_v1",
    state: "SHADOW_RUNNING",
    created_at: "2026-08-27T15:00:00Z",
    candidate_hash: "d".repeat(64),
    expression_hash: "e".repeat(64),
  }],
  registry_events: [{
    sequence: 1,
    event_type: "GENESIS",
    target_strategy_version: "baseline_v1",
    champion_generation: 1,
    occurred_at: "2026-08-27T00:00:00Z",
    effective_session: null,
    status: "effective",
    event_hash: "f".repeat(64),
  }],
  next_cursor: null,
  live_execution_enabled: false,
};

export const detail: components["schemas"]["ExperimentDetailResponse"] = {
  experiment: overview.experiments[0],
  family: overview.active_family!,
  comparison: {
    status: "available",
    valid_oos_window_count: 8,
    cumulative_oos_trial_count: 3,
    annualized_return: { challenger: "0.142", champion: "0.118", delta: "0.024" },
    sharpe: { challenger: "1.31", champion: "1.12", delta: "0.19" },
    max_drawdown: { challenger: "-0.18", champion: "-0.16", delta: "-0.02" },
    double_cost_return: { challenger: "0.126", champion: "0.104", delta: "0.022" },
    spy_comparison_complete: true,
    equal_weight_comparison_complete: true,
    data_mode: "point_in_time",
    authority_hash: "1".repeat(64),
    protocol_hash: "2".repeat(64),
    cost_policy_hash: "3".repeat(64),
    comparison_hash: "4".repeat(64),
  },
  shadow: {
    status: "running",
    completed_sessions: 21,
    evaluation_sessions: 63,
    grace_sessions: 5,
    grace_elapsed_sessions: 0,
    checkpoints_completed: [5, 21],
    full_fill_count: 24,
    after_cost_excess_return: "0.012",
    drawdown_difference: "-0.006",
    hard_risk_violation_count: 0,
    data_complete: true,
    authority_hash: "5".repeat(64),
  },
  gates: [
    { stage: "historical", gate: "minimum_oos_windows", status: "PASS", observed: ["8"], thresholds: ["8"], reason_code: "PASS" },
    { stage: "historical", gate: "deflated_sharpe_confidence", status: "PASS", observed: ["0.97"], thresholds: ["0.95"], reason_code: "PASS" },
  ],
  events: [
    { sequence: 1, event_type: "ADMITTED", occurred_at: "2026-08-27T15:00:00Z", event_hash: "6".repeat(64) },
    { sequence: 2, event_type: "BACKTEST_PASSED", occurred_at: "2026-08-27T15:20:00Z", event_hash: "7".repeat(64) },
    { sequence: 3, event_type: "SHADOW_RUNNING", occurred_at: "2026-08-27T15:30:00Z", event_hash: "8".repeat(64) },
  ],
  integrity_status: "verified",
  write_enabled: true,
  write_block_reason: null,
  live_execution_enabled: false,
};

export function overviewEnvelope() {
  return { data: structuredClone(overview), meta: { ...meta } };
}

export function detailEnvelope() {
  return { data: structuredClone(detail), meta: { ...meta } };
}

export function actionEnvelope(
  action: components["schemas"]["EvolutionActionResponse"]["action"] = "generate_family",
) {
  return {
    data: {
      action,
      status: action === "generate_family" ? "QUEUED" : action === "rollback_registry" ? "PENDING_BOUNDARY" : "EFFECTIVE",
      reason_code: "ACCEPTED",
      generation: 1,
      registry_version: 2,
      resource_id: registryEventId,
      pending_effective_session: action === "rollback_registry" ? "2026-08-28" : null,
      live_execution_enabled: false,
    },
    meta: { ...meta },
  };
}
