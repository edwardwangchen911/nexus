import type { components } from "../../../app/lib/generated/schema";

export const portfolioId = "0198d500-1000-7000-8000-000000001301";
export const snapshotId = "0198d500-1000-7000-8000-000000001302";
export const groupId = "0198d500-1000-7000-8000-000000001303";
export const proposalIds = {
  hold_current: "0198d500-1000-7000-8000-000000001304",
  minimum_change: "0198d500-1000-7000-8000-000000001305",
  strategy_target: "0198d500-1000-7000-8000-000000001306",
} as const;
export const instrumentIds = {
  aapl: "0198d500-1000-7000-8000-000000001307",
  spy: "0198d500-1000-7000-8000-000000001308",
} as const;

export function draftResearchContext(): components["schemas"]["FrozenDailyResearchContext"] {
  return {
    schema_version: "daily_research_context_v1", mode: "context_only", execution_locked: true,
    root_id: "0198d500-1000-7000-8000-000000001321",
    run_id: "0198d500-1000-7000-8000-000000001322",
    output_id: "0198d500-1000-7000-8000-000000001323", output_sequence: 1,
    output_hash: "a".repeat(64), evidence_hash: "b".repeat(64),
    portfolio_snapshot_id: snapshotId,
    diagnostic_report_id: "0198d500-1000-7000-8000-000000001312",
    research_data_snapshot_id: "0198d500-1000-7000-8000-000000001325",
    instrument_id: instrumentIds.aapl,
    valuation_time: "2026-08-24T00:00:00Z", research_as_of: "2026-08-25T00:00:00Z",
    output_available_at: "2026-08-25T00:10:00Z", context_as_of: "2026-08-25T00:15:00Z",
    run_status: "completed", quality_status: "partial", intent: null,
    unavailable_reason: "STAGED_ACTION_SEMANTICS_UNVERIFIED",
  };
}

type DiagnosticReport = components["schemas"]["PortfolioDiagnosticResponse"];
type ProposalGroup = components["schemas"]["OptimizationProposalGroupResponse"];
type Overview = components["schemas"]["OverviewResponse"];
export type Shadow = components["schemas"]["ShadowGroupResponse"];
type ShadowCheckpoint = components["schemas"]["ShadowCheckpointResponse"];

const evidence = {
  source: "nexus_db",
  object_id: "diagnostic:fixture",
  available_at: "2026-08-24T00:00:00Z",
  location: "db://diagnostics/fixture",
  content_hash: "a".repeat(64),
};

export function diagnosticReport(
  optimizationNeed: DiagnosticReport["optimization_need"] = "OPTIMIZATION_CANDIDATE",
): DiagnosticReport {
  return {
    portfolio_snapshot_id: snapshotId,
    snapshot_as_of: "2026-08-24T00:00:00Z",
    trading_day: "2026-08-24",
    policy_version: "real_advisory_policy_v1",
    optimization_need: optimizationNeed,
    items: [
      {
        diagnostic_code: "CONCENTRATION_TOP1",
        metric_value: "0.20",
        unit: "ratio",
        reference_limit: "0.15",
        headroom: "-0.05",
        headroom_reason: null,
        policy_scope: "portfolio",
        severity: "warning",
        actionability: "reduce_only",
        unknown_reason: null,
        evidence_refs: [evidence],
      },
      {
        diagnostic_code: "LIQUIDITY_DAYS_TO_EXIT",
        metric_value: null,
        unit: "trading_days",
        reference_limit: "2",
        headroom: null,
        headroom_reason: "ADV20_MISSING",
        policy_scope: "portfolio",
        severity: "warning",
        actionability: "research",
        unknown_reason: "ADV20_MISSING",
        evidence_refs: [evidence],
      },
      {
        diagnostic_code: "DRAWDOWN_252",
        metric_value: null,
        unit: "ratio",
        reference_limit: "0.08",
        headroom: null,
        headroom_reason: "DRAWDOWN_252_COVERAGE_INSUFFICIENT",
        policy_scope: "portfolio",
        severity: "info",
        actionability: "research",
        unknown_reason: "DRAWDOWN_252_COVERAGE_INSUFFICIENT",
        evidence_refs: [evidence],
      },
      {
        diagnostic_code: "SOURCE_AUTHORITY",
        metric_value: null,
        unit: "state",
        reference_limit: null,
        headroom: null,
        headroom_reason: "SOURCE_CONFLICT",
        policy_scope: "portfolio",
        severity: "critical",
        actionability: "blocked",
        unknown_reason: "SOURCE_CONFLICT",
        evidence_refs: [evidence],
      },
    ],
  } as DiagnosticReport;
}

function known(value: string) {
  return { status: "known" as const, value, reason: null };
}

function unknown(reason: string) {
  return { status: "unknown" as const, value: null, reason };
}

function proposal(
  variant: keyof typeof proposalIds,
  options: { analysisOnly?: boolean; evidenceComplete?: boolean; unknownCost?: boolean } = {},
) {
  const targetWeight = variant === "hold_current" ? "0.20" : variant === "minimum_change" ? "0.15" : "0.125";
  const unknownCost = options.unknownCost ?? variant === "strategy_target";
  return {
    proposal_id: proposalIds[variant],
    variant,
    analysis_only: options.analysisOnly ?? true,
    strategy_evidence_complete: options.evidenceComplete ?? variant !== "strategy_target",
    portfolio_scope: "real_read_only",
    positions: [
      {
        instrument_id: instrumentIds.aapl,
        current_weight: "0.20",
        target_weight: targetWeight,
        action: targetWeight === "0.20" ? "maintain" : "reduce",
        reason_code: "CONCENTRATION_TOP1",
      },
      {
        instrument_id: instrumentIds.spy,
        current_weight: "0.10",
        target_weight: "0.10",
        action: "maintain",
        reason_code: "BASELINE_HOLDING",
      },
    ],
    comparison: {
      metrics: {
        authority: {
          diagnostic_report_id: "0198d500-1000-7000-8000-000000001312",
          data_snapshot_id: "0198d500-1000-7000-8000-000000001309",
          source_policy_id: "0198d500-1000-7000-8000-000000001310",
          cost_policy_id: "0198d500-1000-7000-8000-000000001311",
          optimizer_policy_id: "0198d500-1000-7000-8000-000000001313",
        },
        unknown_metrics: unknownCost ? ["cost"] : [],
        constraint_complete: true,
        eligibility_complete: true,
        evidence_complete: !unknownCost,
        stale: false,
        ranking_eligible: !unknownCost,
        dimensions: {
          hard_violations: variant === "hold_current" ? 1 : 0,
          top1: known(targetWeight),
          top3: known(variant === "hold_current" ? "0.30" : "0.25"),
          hhi: known("0.52"),
          sector_exposure: known("0.25"),
          currency_exposure: known("0.30"),
          volatility: unknown("VOLATILITY_POST_PROPOSAL_NOT_PROVEN"),
          drawdown: unknown("DRAWDOWN_POST_PROPOSAL_NOT_PROVEN"),
          correlation: unknown("CORRELATION_POST_PROPOSAL_NOT_PROVEN"),
          liquidity: known("0.90"),
          turnover: known(variant === "hold_current" ? "0" : "0.05"),
          cost: unknownCost ? unknown("COST_NOT_PROVEN") : known("0.001"),
          tax_impact: unknown("TAX_DATA_UNAVAILABLE"),
        },
      },
      tax_impact: "unknown",
      requires_fx: false,
      non_executable: unknownCost,
      cost_complete: !unknownCost,
    },
  };
}

export function proposalGroup(
  overrides: Partial<ProposalGroup> & { status?: ProposalGroup["status"] } = {},
): ProposalGroup {
  return {
    proposal_group_id: groupId,
    status: "compared",
    version: 4,
    baseline_portfolio_snapshot_id: snapshotId,
    portfolio_scope: "real_read_only",
    diagnostic_input_hash: "b".repeat(64),
    data_snapshot_id: "0198d500-1000-7000-8000-000000001309",
    source_policy_id: "0198d500-1000-7000-8000-000000001310",
    cost_policy_id: "0198d500-1000-7000-8000-000000001311",
    proposals: [
      proposal("hold_current", { analysisOnly: true, evidenceComplete: true }),
      proposal("minimum_change", { analysisOnly: true, evidenceComplete: true }),
      proposal("strategy_target", { analysisOnly: true, evidenceComplete: false, unknownCost: true }),
    ],
    shadow_eligibility: {
      eligible: false,
      reasons: ["STRATEGY_EVIDENCE_INCOMPLETE", "COST_NOT_PROVEN"],
    },
    ...overrides,
  } as ProposalGroup;
}

export function eligibleProposalGroup(
  overrides: Partial<ProposalGroup> = {},
): ProposalGroup {
  const base = proposalGroup();
  const makeEligible = (item: ProposalGroup["proposals"][number]) => ({
    ...item,
    strategy_evidence_complete: true,
    comparison: {
      ...item.comparison,
      non_executable: false,
      cost_complete: true,
      metrics: {
        ...item.comparison.metrics,
        unknown_metrics: [],
        evidence_complete: true,
        ranking_eligible: true,
        dimensions: {
          ...item.comparison.metrics.dimensions,
          cost: { status: "known" as const, value: "0.001", reason: null },
        },
      },
    },
  });
  return proposalGroup({
    proposals: [
      makeEligible(base.proposals[0]),
      makeEligible(base.proposals[1]),
      makeEligible(base.proposals[2]),
    ],
    shadow_eligibility: { eligible: true, reasons: [] },
    ...overrides,
  });
}

export function overviewData(overrides: Partial<Overview> = {}): Overview {
  return {
    overview_mode: "DIAGNOSTIC",
    portfolio_id: portfolioId,
    portfolio_name: null,
    provider: null,
    portfolio_snapshot_id: snapshotId,
    portfolio_snapshot: null,
    snapshot_as_of: "2026-08-24T00:00:00Z",
    trading_day: "2026-08-24",
    diagnostic_report: diagnosticReport(),
    diagnostic_unavailable_reason: null,
    proposal_group: { proposal_group_id: groupId, status: "compared", version: 4 },
    proposal_admission: {
      portfolio_snapshot_id: snapshotId,
      data_snapshot_id: "0198d500-1000-7000-8000-000000001309",
      cost_policy_id: "0198d500-1000-7000-8000-000000001311",
    },
    proposal_admission_reason: null,
    ...overrides,
  } as Overview;
}

const shadowGroupId = "0198d500-1000-7000-8000-000000001390";
const shadowPortfolioIds = [
  "0198d500-1000-7000-8000-000000001391",
  "0198d500-1000-7000-8000-000000001392",
  "0198d500-1000-7000-8000-000000001393",
] as const;
const shadowVariants = ["hold_current", "minimum_change", "strategy_target"] as const;

function shadowSessions(count: number): string[] {
  const sessions: string[] = [];
  const closed = new Set(["2026-09-07", "2026-11-26"]);
  const cursor = new Date(Date.UTC(2026, 7, 25));
  while (sessions.length < count) {
    const value = cursor.toISOString().slice(0, 10);
    if (cursor.getUTCDay() !== 0 && cursor.getUTCDay() !== 6 && !closed.has(value)) sessions.push(value);
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return sessions;
}

function shadowMetric(value: string) {
  return { status: "known" as const, value, reason: null };
}

function shadowUnknown(reason: string) {
  return { status: "unknown" as const, value: null, reason };
}

function checkpointMetrics(variant: typeof shadowVariants[number], unknownReason?: string) {
  const metric = unknownReason ? () => shadowUnknown(unknownReason) : shadowMetric;
  return {
    variant,
    metrics: {
      net_cumulative_return: metric(variant === "hold_current" ? "0.012000000000" : "0.018000000000"),
      return_delta_vs_hold: metric(variant === "hold_current" ? "0.000000000000" : "0.006000000000"),
      max_drawdown: metric("-0.021000000000"),
      drawdown_improvement: metric(variant === "hold_current" ? "0.000000000000" : "0.004000000000"),
      annualized_volatility: metric("0.143000000000"),
      volatility_improvement: metric(variant === "hold_current" ? "0.000000000000" : "0.011000000000"),
      worst_sessions: unknownReason
        ? []
        : [{ session: "2026-08-27", return_value: "-0.012000000000" }],
      cumulative_turnover: metric(variant === "hold_current" ? "0.000000000000" : "0.084000000000"),
      fee_drag_index_points: metric(variant === "hold_current" ? "0.000000000000" : "0.014000000000"),
    },
  };
}

function shadowCheckpoint(
  ordinal: 5 | 21 | 63,
  sessions: string[],
  status: "complete" | "incomplete" = "complete",
  coverageCount: number = ordinal,
): ShadowCheckpoint {
  const reason = status === "incomplete" ? "SHADOW_DATA_INCOMPLETE" : undefined;
  return {
    ordinal,
    session: sessions[ordinal - 1] ?? shadowSessions(63)[ordinal - 1],
    status,
    coverage_count: coverageCount,
    reason: reason ?? null,
    metrics: shadowVariants.map((variant) => checkpointMetrics(variant, reason)) as ShadowCheckpoint["metrics"],
  };
}

export function shadowActive(overrides: Partial<Shadow> = {}): Shadow {
  const sessions = shadowSessions(5);
  const portfolios = shadowVariants.map((variant, variantIndex) => ({
    shadow_portfolio_id: shadowPortfolioIds[variantIndex],
    variant,
    nav: sessions.map((session, index) => ({
      session,
      nav: (100 + index * (variantIndex + 1) * 0.125).toFixed(12),
    })),
    initial_outcomes: [{
      instrument_id: instrumentIds.aapl,
      side: "buy" as const,
      status: "filled" as const,
      reason: "filled at target-session open",
      fill: {
        side: "buy" as const,
        quantity: "0.100000000000",
        price: "100.000000000000",
        notional: "10.000000000000",
        cost: "0.010000000000",
      },
    }],
  })) as Shadow["portfolios"];
  return {
    proposal_group_id: groupId,
    shadow_group_id: shadowGroupId,
    proposal_status: "shadowing",
    version: 5,
    start_session: sessions[0],
    evaluation_end_session: shadowSessions(63)[62],
    grace_end_session: shadowSessions(68)[67],
    coverage: {
      completed: 5,
      due: 5,
      required: 63,
      next_expected_session: shadowSessions(6)[5],
      missing_sessions: [],
    },
    freshness: {
      status: "current",
      latest_session: sessions[4],
      available_at: "2026-08-31T20:30:00Z",
      last_attempt_at: "2026-08-31T20:31:00Z",
      database_time: "2026-08-31T20:31:01Z",
      reason: null,
    },
    sources: [
      { source: "fosun", kind: "price_bars", delivery_mode: "delayed", latest_session: sessions[4], available_at: "2026-08-31T20:30:00Z" },
      { source: "sec", kind: "corporate_actions", delivery_mode: "unknown", latest_session: sessions[4], available_at: "2026-08-31T20:29:00Z" },
    ],
    portfolios,
    checkpoints: [shadowCheckpoint(5, sessions)],
    last_failed_attempt: null,
    sealed: false,
    seal_reason: null,
    safety: { creates_orders: false, trading_approval: false },
    ...overrides,
  };
}

export function shadowNotStarted(): null {
  return null;
}

export function shadowGrace(): Shadow {
  const sessions = shadowSessions(61);
  return shadowActive({
    coverage: { completed: 61, due: 63, required: 63, next_expected_session: shadowSessions(62)[61], missing_sessions: shadowSessions(63).slice(61) },
    freshness: { status: "stale", latest_session: sessions[60], available_at: "2026-11-18T20:30:00Z", last_attempt_at: "2026-11-19T20:31:00Z", database_time: "2026-11-19T20:31:01Z", reason: "SHADOW_SESSION_DUE" },
    portfolios: shadowActive().portfolios.map((portfolio) => ({ ...portfolio, nav: sessions.map((session, index) => ({ session, nav: (100 + index * 0.05).toFixed(12) })) })) as Shadow["portfolios"],
    checkpoints: [shadowCheckpoint(5, sessions), shadowCheckpoint(21, sessions)],
  });
}

export function shadowPartial(): Shadow {
  const sessions = shadowSessions(3);
  return shadowActive({
    coverage: { completed: 3, due: 5, required: 63, next_expected_session: shadowSessions(4)[3], missing_sessions: shadowSessions(5).slice(3) },
    freshness: { status: "stale", latest_session: sessions[2], available_at: "2026-08-27T20:30:00Z", last_attempt_at: "2026-08-29T20:31:00Z", database_time: "2026-08-29T20:31:01Z", reason: "SHADOW_SESSION_DUE" },
    portfolios: shadowActive().portfolios.map((portfolio) => ({ ...portfolio, nav: portfolio.nav.slice(0, 3) })) as Shadow["portfolios"],
    checkpoints: [],
    last_failed_attempt: { requested_session: shadowSessions(4)[3], expected_session: shadowSessions(4)[3], result: "data_partial", reason: "SHADOW_DATA_INCOMPLETE", observed_at: "2026-08-29T20:31:00Z", created_at: "2026-08-29T20:31:01Z" },
  });
}

export function shadowStale(): Shadow {
  return shadowActive({
    freshness: { ...shadowActive().freshness, status: "stale", reason: "SHADOW_SESSION_DUE" },
  });
}

export function shadowValidated(): Shadow {
  const sessions = shadowSessions(63);
  return shadowActive({
    proposal_status: "validated",
    version: 6,
    coverage: { completed: 63, due: 63, required: 63, next_expected_session: null, missing_sessions: [] },
    freshness: { status: "current", latest_session: sessions[62], available_at: "2026-11-20T20:30:00Z", last_attempt_at: "2026-11-20T20:31:00Z", database_time: "2026-11-20T20:31:01Z", reason: null },
    portfolios: shadowActive().portfolios.map((portfolio, variantIndex) => ({ ...portfolio, nav: sessions.map((session, index) => ({ session, nav: (100 + index * (variantIndex + 1) * 0.05).toFixed(12) })) })) as Shadow["portfolios"],
    checkpoints: [shadowCheckpoint(5, sessions), shadowCheckpoint(21, sessions), shadowCheckpoint(63, sessions)],
    sealed: true,
    seal_reason: "validated",
  });
}

export function shadowRejected(): Shadow {
  return shadowActive({ proposal_status: "rejected", version: 6, sealed: true, seal_reason: "rejected" });
}

export function shadowExpired(): Shadow {
  const sessions = shadowSessions(3);
  return shadowActive({
    proposal_status: "expired",
    version: 6,
    coverage: { completed: 3, due: 63, required: 63, next_expected_session: shadowSessions(4)[3], missing_sessions: shadowSessions(63).slice(3) },
    freshness: { status: "stale", latest_session: sessions[2], available_at: "2026-08-27T20:30:00Z", last_attempt_at: "2026-11-01T20:31:00Z", database_time: "2026-11-01T20:31:01Z", reason: "SHADOW_SESSION_DUE" },
    portfolios: shadowActive().portfolios.map((portfolio) => ({ ...portfolio, nav: portfolio.nav.slice(0, 3) })) as Shadow["portfolios"],
    checkpoints: [shadowCheckpoint(5, sessions, "incomplete", 3), shadowCheckpoint(21, sessions, "incomplete", 3), shadowCheckpoint(63, sessions, "incomplete", 3)],
    sealed: true,
    seal_reason: "expired",
  });
}
